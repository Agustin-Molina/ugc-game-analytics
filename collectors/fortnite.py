"""Colector diario de Fortnite (Fortnite Data API, pública).

1. Pagina /islands (catálogo completo, solo en memoria).
2. Actualiza la dimensión `games` y registra en `events` islas nuevas / desaparecidas /
   re-listadas y cambios de título, tags o categoría.
3. Consulta métricas `day` (7 días, con retención) del grupo del día: hash(code) % 5.
   La API guarda 7 días y cada isla se visita cada 5 → 2 días de solapamiento.
4. Para islas con max(peakCCU) >= umbral, baja también la curva `hour` de 7 días.

Uso: python -m collectors.fortnite [--group N] [--limit N] [--data-dir PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from collectors.common import (
    TS,
    Http,
    HttpError,
    RateLimiter,
    RunSummary,
    floor_day,
    floor_hour,
    frame,
    group_for_date,
    group_of,
    iso_z,
    load_config,
    new_run_id,
    parse_ts,
    read_dimension,
    resolve_data_dir,
    setup_logging,
    utcnow,
    write_dimension,
    write_partition,
)

PLATFORM = "fortnite"
log = logging.getLogger("collectors.fortnite")

INTERVALS = {"day": timedelta(days=1), "hour": timedelta(hours=1)}

# nombre en la API → columna
METRIC_COLUMNS = {
    "plays": "plays",
    "uniquePlayers": "unique_players",
    "peakCCU": "peak_ccu",
    "averageMinutesPerPlayer": "avg_minutes_per_player",
    "minutesPlayed": "minutes_played",
    "favorites": "favorites",
    "recommendations": "recommendations",
}
VALUE_COLUMNS = [*METRIC_COLUMNS.values(), "retention_d1", "retention_d7"]

SNAPSHOT_SCHEMA: dict[str, pl.DataType] = {
    "island_code": pl.Utf8,
    "interval": pl.Utf8,
    "bucket_start": TS,
    "snapshot_at": TS,
    "is_partial": pl.Boolean,
    "plays": pl.Int64,
    "unique_players": pl.Int64,
    "peak_ccu": pl.Int64,
    "avg_minutes_per_player": pl.Float64,
    "minutes_played": pl.Int64,
    "favorites": pl.Int64,
    "recommendations": pl.Int64,
    "retention_d1": pl.Float64,
    "retention_d7": pl.Float64,
}

FETCH_LOG_SCHEMA: dict[str, pl.DataType] = {
    "island_code": pl.Utf8,
    "snapshot_at": TS,
    "interval": pl.Utf8,
    "status": pl.Utf8,  # ok | not_found | error
    "http_status": pl.Int32,
    "n_buckets": pl.Int32,  # buckets con algún dato
    "max_peak_ccu": pl.Int64,
}

GAMES_SCHEMA: dict[str, pl.DataType] = {
    "island_code": pl.Utf8,
    "title": pl.Utf8,
    "creator_code": pl.Utf8,
    "category": pl.Utf8,
    "created_in": pl.Utf8,
    "tags": pl.List(pl.Utf8),
    "genre_unified": pl.Utf8,  # Fase 2
    "first_seen_at": TS,
    "last_seen_at": TS,
    "is_listed": pl.Boolean,
    "first_traction_at": TS,
    "last_fetched_at": TS,
    "last_active_bucket": TS,
    "tier": pl.Utf8,
}
CATALOG_FIELDS = ["title", "creator_code", "category", "created_in", "tags"]

EVENTS_SCHEMA: dict[str, pl.DataType] = {
    "detected_at": TS,
    "island_code": pl.Utf8,
    "event_type": pl.Utf8,
    "old_value": pl.Utf8,
    "new_value": pl.Utf8,
}


# --------------------------------------------------------------------------- catálogo


def parse_island(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "island_code": item.get("code"),
        "title": item.get("title"),
        "creator_code": item.get("creatorCode"),
        "category": item.get("category"),
        "created_in": item.get("createdIn"),
        "tags": item.get("tags") or [],
    }


async def fetch_catalog(
    http: Http, cfg: dict, limiter: RateLimiter, summary: RunSummary, limit: int | None = None
) -> tuple[pl.DataFrame, bool]:
    """Devuelve (catálogo, completo). `completo=False` si se cortó por error o por --limit."""
    fcfg = cfg["fortnite"]
    url = f"{fcfg['base_url']}/islands"
    page_size = int(fcfg["page_size"])
    params: dict[str, Any] = {"size": page_size}
    items: list[dict] = []
    complete = False
    while True:
        summary.ops_total += 1
        try:
            payload = await http.get_json(url, params, limiter)
        except HttpError as exc:
            summary.ops_failed += 1
            summary.error(f"catalog_{exc.kind}")
            log.error("catálogo cortado en %d islas: %s", len(items), exc)
            break
        items.extend(parse_island(x) for x in payload.get("data") or [])
        cursor = ((payload.get("meta") or {}).get("page") or {}).get("nextCursor")
        if limit and len(items) >= limit:
            items = items[:limit]
            break
        if not cursor:
            complete = True
            break
        params = {"size": page_size, "after": cursor}
        if summary.ops_total % 50 == 0:
            log.info("catálogo: %d islas…", len(items))
    df = frame(items, {k: GAMES_SCHEMA[k] for k in ["island_code", *CATALOG_FIELDS]})
    df = df.filter(pl.col("island_code").is_not_null()).unique("island_code", keep="last")
    return df, complete


# --------------------------------------------------------------------------- dimensión


def _tags_str(col: str) -> pl.Expr:
    return pl.col(col).list.sort().list.join("|")


def update_dimension_with_catalog(
    dim: pl.DataFrame, catalog: pl.DataFrame, now: datetime, complete: bool
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Merge del catálogo de hoy con la dimensión. Devuelve (dim nueva, events).

    - Primera carga (dimensión vacía): no emite eventos.
    - Si el catálogo está incompleto, no marca islas como desaparecidas.
    """
    initial = dim.is_empty()
    cat = catalog.with_columns(pl.lit(True).alias("_in_cat"))
    old = dim.with_columns(pl.lit(True).alias("_in_old"))
    j = cat.join(old, on="island_code", how="full", coalesce=True, suffix="_old")
    in_cat = pl.col("_in_cat").fill_null(False)
    in_old = pl.col("_in_old").fill_null(False)
    now_lit = pl.lit(now, dtype=TS)

    new_dim = j.select(
        "island_code",
        *[
            pl.when(in_cat).then(pl.col(f)).otherwise(pl.col(f"{f}_old")).alias(f)
            for f in CATALOG_FIELDS
        ],
        pl.col("genre_unified"),
        pl.coalesce("first_seen_at", now_lit).alias("first_seen_at"),
        pl.when(in_cat).then(now_lit).otherwise(pl.col("last_seen_at")).alias("last_seen_at"),
        (
            pl.when(in_cat)
            .then(True)
            .when(pl.lit(complete))
            .then(False)
            .otherwise(pl.col("is_listed"))
            .alias("is_listed")
        ),
        "first_traction_at",
        "last_fetched_at",
        "last_active_bucket",
        pl.coalesce("tier", pl.lit("warm")).alias("tier"),
    ).sort("island_code")

    if initial:
        return new_dim.cast(GAMES_SCHEMA), pl.DataFrame(schema=EVENTS_SCHEMA)

    both = in_cat & in_old
    specs: list[tuple[pl.Expr, str, pl.Expr, pl.Expr]] = [
        (in_cat & ~in_old, "island_new", pl.lit(None, pl.Utf8), pl.col("title")),
        (both & ~pl.col("is_listed").fill_null(True), "island_relisted",
         pl.lit(None, pl.Utf8), pl.col("title")),
        (both & pl.col("title").ne_missing(pl.col("title_old")), "title_change",
         pl.col("title_old"), pl.col("title")),
        (both & _tags_str("tags").ne_missing(_tags_str("tags_old")), "tags_change",
         _tags_str("tags_old"), _tags_str("tags")),
        (both & pl.col("category").ne_missing(pl.col("category_old")), "category_change",
         pl.col("category_old"), pl.col("category")),
    ]
    if complete:
        specs.append(
            (~in_cat & in_old & pl.col("is_listed").fill_null(True), "island_removed",
             pl.col("title_old"), pl.lit(None, pl.Utf8))
        )
    events = pl.concat(
        [
            j.filter(cond).select(
                now_lit.alias("detected_at"),
                "island_code",
                pl.lit(etype).alias("event_type"),
                old_v.cast(pl.Utf8).alias("old_value"),
                new_v.cast(pl.Utf8).alias("new_value"),
            )
            for cond, etype, old_v, new_v in specs
        ]
    )
    return new_dim.cast(GAMES_SCHEMA), events.cast(EVENTS_SCHEMA)


def apply_fetch_results(
    dim: pl.DataFrame, fetch_log: pl.DataFrame, snaps: pl.DataFrame, traction_min_ccu: int
) -> pl.DataFrame:
    """Actualiza last_fetched_at, last_active_bucket y first_traction_at."""
    if fetch_log.is_empty():
        return dim
    fetched = (
        fetch_log.filter(pl.col("interval") == "day")
        .group_by("island_code")
        .agg(pl.col("snapshot_at").max().alias("_fetched"))
    )
    day = snaps.filter(pl.col("interval") == "day")
    active = day.group_by("island_code").agg(pl.col("bucket_start").max().alias("_active"))
    traction = (
        day.filter(pl.col("peak_ccu") >= traction_min_ccu)
        .group_by("island_code")
        .agg(pl.col("bucket_start").min().alias("_traction"))
    )
    out = (
        dim.join(fetched, on="island_code", how="left")
        .join(active, on="island_code", how="left")
        .join(traction, on="island_code", how="left")
        .with_columns(
            pl.coalesce("_fetched", "last_fetched_at").alias("last_fetched_at"),
            pl.max_horizontal("last_active_bucket", "_active").alias("last_active_bucket"),
            pl.min_horizontal("first_traction_at", "_traction").alias("first_traction_at"),
        )
        .drop("_fetched", "_active", "_traction")
    )
    return out.cast(GAMES_SCHEMA)


# --------------------------------------------------------------------------- métricas


def metrics_window(now: datetime, window_days: int) -> tuple[str, str]:
    """`from` = hoy 00:00 − N días; `to` = hora actual truncada (la API rechaza el futuro)."""
    return iso_z(floor_day(now) - timedelta(days=window_days)), iso_z(floor_hour(now))


def parse_metrics(
    code: str, payload: dict[str, Any], interval: str, snapshot_at: datetime
) -> list[dict[str, Any]]:
    """{metric: [{value, timestamp}]} → una fila por bucket; descarta buckets 100 % null."""
    buckets: dict[str, dict[str, Any]] = {}
    for key, series in payload.items():
        if not isinstance(series, list):
            continue
        for point in series:
            ts = point.get("timestamp")
            if ts is None:
                continue
            row = buckets.setdefault(ts, {})
            if key == "retention":
                row["retention_d1"] = point.get("d1")
                row["retention_d7"] = point.get("d7")
            elif key in METRIC_COLUMNS:
                row[METRIC_COLUMNS[key]] = point.get("value")
    step = INTERVALS[interval]
    rows = []
    for ts, values in sorted(buckets.items()):
        if all(values.get(c) is None for c in VALUE_COLUMNS):
            continue
        start = parse_ts(ts)
        rows.append(
            {
                "island_code": code,
                "interval": interval,
                "bucket_start": start,
                "snapshot_at": snapshot_at,
                "is_partial": start + step > snapshot_at,
                **{c: values.get(c) for c in VALUE_COLUMNS},
            }
        )
    return rows


class MetricsFetcher:
    """Pool de workers que consulta métricas respetando rate limit y deadline."""

    def __init__(self, http: Http, cfg: dict, limiter: RateLimiter, summary: RunSummary,
                 deadline: float):
        self.http = http
        self.fcfg = cfg["fortnite"]
        self.limiter = limiter
        self.summary = summary
        self.deadline = deadline
        self.snap_rows: list[dict] = []
        self.log_rows: list[dict] = []
        self.done = 0
        self.skipped_deadline = 0
        self.hourly_fetched = 0

    async def _fetch(self, code: str, interval: str) -> tuple[list[dict], dict]:
        frm, to = metrics_window(utcnow(), int(self.fcfg["window_days"]))
        url = f"{self.fcfg['base_url']}/islands/{code}/metrics/{interval}"
        entry: dict[str, Any] = {"island_code": code, "interval": interval}
        try:
            payload = await self.http.get_json(url, {"from": frm, "to": to}, self.limiter)
        except HttpError as exc:
            entry.update(snapshot_at=utcnow(), http_status=exc.status, n_buckets=0,
                         status="not_found" if exc.status == 404 else "error")
            if exc.status != 404:
                self.summary.error(f"{interval}_{exc.kind}")
            return [], entry
        snapshot_at = utcnow()
        rows = parse_metrics(code, payload, interval, snapshot_at)
        peaks = [r["peak_ccu"] for r in rows if r["peak_ccu"] is not None]
        entry.update(snapshot_at=snapshot_at, http_status=200, status="ok",
                     n_buckets=len(rows), max_peak_ccu=max(peaks) if peaks else None)
        return rows, entry

    async def process(self, code: str) -> None:
        self.summary.ops_total += 1
        rows, entry = await self._fetch(code, "day")
        self.log_rows.append(entry)
        if entry["status"] == "error":
            self.summary.ops_failed += 1
            return
        self.snap_rows.extend(rows)
        peak = entry.get("max_peak_ccu")
        if peak is not None and peak >= int(self.fcfg["hourly_min_peak_ccu"]):
            hrows, hentry = await self._fetch(code, "hour")
            self.log_rows.append(hentry)
            self.snap_rows.extend(hrows)
            self.hourly_fetched += 1

    async def run(self, codes: list[str], on_flush) -> None:
        queue: asyncio.Queue[str] = asyncio.Queue()
        for c in codes:
            queue.put_nowait(c)
        flush_every = int(self.fcfg["flush_every"])
        total = len(codes)
        flush_lock = asyncio.Lock()

        async def worker() -> None:
            while True:
                try:
                    code = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                if time.monotonic() > self.deadline:
                    self.skipped_deadline += 1
                    continue
                await self.process(code)
                self.done += 1
                if self.done % 1000 == 0:
                    log.info("métricas: %d/%d islas (tasa %.1f req/s, 429=%d)", self.done,
                             total, self.limiter.rate, self.limiter.throttles)
                if self.done % flush_every == 0:
                    async with flush_lock:
                        on_flush(self.take())

        await asyncio.gather(*(worker() for _ in range(int(self.fcfg["concurrency"]))))

    def take(self) -> tuple[list[dict], list[dict]]:
        snaps, logs = self.snap_rows, self.log_rows
        self.snap_rows, self.log_rows = [], []
        return snaps, logs


def select_today(dim: pl.DataFrame, group: int, n_groups: int) -> list[str]:
    """Islas listadas del grupo; primero las que tuvieron actividad (por si corta el deadline)."""
    listed = dim.filter(pl.col("is_listed"))
    codes = listed["island_code"].to_list()
    mask = [group_of(c, n_groups) == group for c in codes]
    sel = listed.filter(pl.Series(mask, dtype=pl.Boolean))
    sel = sel.sort(["last_active_bucket", "island_code"], descending=[True, False],
                   nulls_last=True)
    return sel["island_code"].to_list()


# --------------------------------------------------------------------------- main


async def run(
    data_dir: Path,
    cfg: dict,
    group: int | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    codes_override: list[str] | None = None,
) -> RunSummary:
    fcfg = cfg["fortnite"]
    run_id = new_run_id()
    summary = RunSummary(PLATFORM, run_id)
    started = summary.started_at
    deadline = time.monotonic() + float(fcfg["deadline_minutes"]) * 60
    n_groups = int(fcfg["n_groups"])
    group = group_for_date(started, n_groups) if group is None else group
    dim = read_dimension(data_dir, PLATFORM, GAMES_SCHEMA)
    part = [0]

    def write(table: str, df: pl.DataFrame, partitioned: bool = True) -> None:
        if dry_run:
            summary.rows[table] += df.height
            return
        if partitioned:
            path = write_partition(df, data_dir, PLATFORM, table, started, run_id, part[0])
            summary.add_file(table, path, df.height)
        else:  # la dimensión se reescribe: contar su tamaño final, no sumar
            path = write_dimension(df, data_dir, PLATFORM)
            summary.rows[table] = 0
            summary.add_file(table, path, df.height)

    all_snaps: list[pl.DataFrame] = []
    all_logs: list[pl.DataFrame] = []

    def flush(batch: tuple[list[dict], list[dict]]) -> None:
        snaps = frame(batch[0], SNAPSHOT_SCHEMA)
        logs = frame(batch[1], FETCH_LOG_SCHEMA)
        part[0] += 1
        write("snapshots", snaps)
        write("fetch_log", logs)
        all_snaps.append(snaps.select("island_code", "interval", "bucket_start", "peak_ccu"))
        all_logs.append(logs.select("island_code", "interval", "snapshot_at"))
        log.info("flush p%03d: %d filas de snapshots", part[0], snaps.height)

    limiter = RateLimiter(float(fcfg["rate_per_s"]), adaptive=True)
    async with Http(cfg) as http:
        summary.http = http.stats
        if codes_override is None:
            catalog, complete = await fetch_catalog(http, cfg, limiter, summary, limit)
            summary.notes.append(
                f"catálogo: {catalog.height} islas ({'completo' if complete else 'INCOMPLETO'})"
            )
            if catalog.is_empty() and dim.is_empty():
                summary.notes.append("sin catálogo ni dimensión previa: nada que hacer")
                return summary
            dim, events = update_dimension_with_catalog(dim, catalog, started, complete)
            write("events", events)
            write("games (dim)", dim, partitioned=False)  # persistir altas/bajas aunque luego falle
            codes = select_today(dim, group, n_groups)
            if limit:
                codes = codes[:limit]
        else:
            codes = codes_override
        summary.notes.append(f"grupo {group}/{n_groups}: {len(codes)} islas a consultar")

        fetcher = MetricsFetcher(http, cfg, limiter, summary, deadline)
        await fetcher.run(codes, flush)
        flush(fetcher.take())

    snaps_all = pl.concat(all_snaps) if all_snaps else pl.DataFrame(schema=SNAPSHOT_SCHEMA)
    logs_all = pl.concat(all_logs) if all_logs else pl.DataFrame(schema=FETCH_LOG_SCHEMA)
    if codes_override is None:
        dim = apply_fetch_results(dim, logs_all, snaps_all, int(fcfg["traction_min_ccu"]))
        write("games (dim)", dim, partitioned=False)

    n_active = snaps_all.filter(pl.col("interval") == "day")["island_code"].n_unique()
    summary.notes.append(
        f"islas con datos: {n_active}"
        f" · con curva horaria: {fetcher.hourly_fetched}"
        f" · 429 recibidos: {limiter.throttles} · tasa final: {limiter.rate:.1f} req/s"
    )
    if fetcher.skipped_deadline:
        summary.notes.append(f"DEADLINE: {fetcher.skipped_deadline} islas sin consultar")
    if dry_run:
        summary.notes.append("dry-run: no se escribió nada")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--group", type=int, default=None, help="grupo a consultar (0..n-1)")
    parser.add_argument("--limit", type=int, default=None,
                        help="máximo de islas (catálogo y métricas); para pruebas")
    parser.add_argument("--codes", default=None,
                        help="códigos separados por coma (salta catálogo y dimensión)")
    parser.add_argument("--data-dir", default=None, help="raíz de datos (default ./data)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)
    cfg = load_config()
    codes = [c.strip() for c in args.codes.split(",")] if args.codes else None
    summary = asyncio.run(
        run(resolve_data_dir(args.data_dir), cfg, args.group, args.limit, args.dry_run, codes)
    )
    summary.emit()
    return summary.exit_code(float(cfg["run"]["max_error_rate"]))


if __name__ == "__main__":
    sys.exit(main())
