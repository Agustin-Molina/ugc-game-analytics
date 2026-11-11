"""Colector diario de Fortnite (Fortnite Data API, pública).

1. Pagina /islands (catálogo completo, solo en memoria).
2. Actualiza la dimensión `games` y registra en `events` islas nuevas / desaparecidas /
   re-listadas y cambios de título, tags o categoría.
3. Elige qué islas consultar según su tier (config/tiers.yaml): hot todos los días, warm
   cada 5 (la API guarda 7 → 2 días de solapamiento), cold cada 7, más las atrasadas.
4. Consulta métricas `day` (7 días, con retención). Para islas con max(peakCCU) >= umbral
   baja también la curva `hour`, solo desde la última que ya tenemos.
5. Recalcula los tiers con lo recién bajado y registra los cambios en `tier_history`.

Uso: python -m collectors.fortnite [--group N] [--limit N] [--data-dir PATH] [--dry-run]
(`--group N` ignora los tiers y consulta todas las islas listadas de ese grupo de 5.)
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
    dimension_changes,
    floor_day,
    floor_hour,
    frame,
    group_of,
    iso_z,
    load_config,
    new_run_id,
    parse_ts,
    read_dimension,
    resolve_data_dir,
    setup_logging,
    utcnow,
    write_dimension_changes,
    write_partition,
)
from pipeline import tiers

PLATFORM = "fortnite"
KEY = "island_code"
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
    "genre_unified": pl.Utf8,  # vacía: se calcula al leer (pipeline/features.py)
    "first_seen_at": TS,
    "is_listed": pl.Boolean,
    "first_traction_at": TS,
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
        (
            pl.when(in_cat)
            .then(True)
            .when(pl.lit(complete))
            .then(False)
            .otherwise(pl.col("is_listed"))
            .alias("is_listed")
        ),
        "first_traction_at",
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


def apply_traction(dim: pl.DataFrame, snaps: pl.DataFrame, traction_min_ccu: int) -> pl.DataFrame:
    """first_traction_at = primer bucket diario con peakCCU >= umbral (nunca se mueve hacia
    adelante). last_seen / last_fetched / last_active NO se guardan: salen de fetch_log y
    snapshots, y guardarlos reescribiría toda la dimensión cada día."""
    traction = (
        snaps.filter((pl.col("interval") == "day") & (pl.col("peak_ccu") >= traction_min_ccu))
        .group_by("island_code")
        .agg(pl.col("bucket_start").min().alias("_traction"))
    )
    if traction.is_empty():
        return dim
    out = (
        dim.join(traction, on="island_code", how="left")
        .with_columns(
            pl.min_horizontal("first_traction_at", "_traction").alias("first_traction_at")
        )
        .drop("_traction")
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
                 deadline: float, last_hour: dict[str, datetime] | None = None):
        self.last_hour = last_hour or {}  # última curva horaria ya bajada, por isla
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
        if interval == "hour" and (prev := self.last_hour.get(code)) is not None:
            # solo las horas nuevas (+2 h por si la última estaba incompleta)
            frm = max(frm, iso_z(floor_hour(prev) - timedelta(hours=2)))
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
    """Islas listadas del grupo; primero las que tuvieron tracción (por si corta el deadline)."""
    listed = dim.filter(pl.col("is_listed"))
    codes = listed["island_code"].to_list()
    mask = [group_of(c, n_groups) == group for c in codes]
    sel = listed.filter(pl.Series(mask, dtype=pl.Boolean))
    sel = sel.sort(["first_traction_at", "island_code"], descending=[True, False],
                   nulls_last=True)
    return sel["island_code"].to_list()


HISTORY_SNAP_COLS = ["island_code", "interval", "bucket_start", "snapshot_at", "peak_ccu"]
HISTORY_SNAP_SCHEMA = {c: SNAPSHOT_SCHEMA[c] for c in HISTORY_SNAP_COLS}


def _fetched(log_df: pl.DataFrame) -> pl.DataFrame:
    """Islas con alguna consulta `day` (con o sin datos; 404 cuenta como consultada)."""
    day = log_df.filter((pl.col("interval") == "day") & (pl.col("status") != "error"))
    return day.select("island_code").unique()


def _last_fetch(log_df: pl.DataFrame, interval: str) -> pl.DataFrame:
    """(island_code, last_at) de la última consulta exitosa del intervalo."""
    return (
        log_df.filter((pl.col("interval") == interval) & (pl.col("status") == "ok"))
        .group_by("island_code").agg(pl.col("snapshot_at").max().alias("last_at"))
    )


def _retier(dim: pl.DataFrame, hist_day: pl.DataFrame, hist_log: pl.DataFrame,
            run_logs: list[pl.DataFrame], run_snaps: pl.DataFrame, now: datetime,
            rules: dict, summary: RunSummary, write) -> pl.DataFrame:
    """Recalcula tiers de las islas listadas con historia + esta corrida y escribe los
    cambios en tier_history. Las no listadas conservan el tier que tenían."""
    logs = pl.concat([hist_log, *run_logs], how="vertical_relaxed")
    day = pl.concat([hist_day, run_snaps.filter(pl.col("interval") == "day")],
                    how="vertical_relaxed")
    listed = dim.filter(pl.col("is_listed"))
    assigned = tiers.assign_fortnite_tiers(listed, day, _fetched(logs), now, rules)
    changes = tiers.tier_changes(listed, assigned, now, key=KEY)
    write("tier_history", changes.cast(tiers.FORTNITE_TIER_HISTORY_SCHEMA))
    counts = dict(assigned.group_by("tier").len().iter_rows())
    summary.notes.append(
        "tiers: " + " · ".join(f"{t} {counts.get(t, 0)}" for t in (tiers.HOT, tiers.WARM,
                                                                    tiers.COLD))
        + f" · cambios: {changes.height}")
    new_tier = assigned.select(KEY, pl.col("tier").alias("_tier"))
    return (
        dim.join(new_tier, on=KEY, how="left")
        .with_columns(pl.coalesce("_tier", "tier").alias("tier"))
        .drop("_tier")
        .cast(GAMES_SCHEMA)
    )


# --------------------------------------------------------------------------- main


async def run(
    data_dir: Path,
    cfg: dict,
    group: int | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    codes_override: list[str] | None = None,
    tier_rules: dict | None = None,
) -> RunSummary:
    fcfg = cfg["fortnite"]
    rules = tier_rules or tiers.load_rules(PLATFORM)
    run_id = new_run_id()
    summary = RunSummary(PLATFORM, run_id)
    started = summary.started_at
    deadline = time.monotonic() + float(fcfg["deadline_minutes"]) * 60
    n_groups = int(fcfg["n_groups"])
    dim = read_dimension(data_dir, PLATFORM, GAMES_SCHEMA, KEY)
    since = started - timedelta(days=int(rules["window_days"]))
    hist_log = tiers.read_recent(data_dir, PLATFORM, "fetch_log", list(FETCH_LOG_SCHEMA),
                                 since)
    hist_log = (pl.DataFrame(schema=FETCH_LOG_SCHEMA) if hist_log is None
                else hist_log.cast(FETCH_LOG_SCHEMA))
    hist_day = tiers.read_recent(data_dir, PLATFORM, "snapshots", HISTORY_SNAP_COLS, since)
    hist_day = (pl.DataFrame(schema=HISTORY_SNAP_SCHEMA) if hist_day is None
                else hist_day.cast(HISTORY_SNAP_SCHEMA).filter(pl.col("interval") == "day"))
    persisted = [dim]  # último estado escrito al log de la dimensión
    # Un catálogo recortado (--limit) o códigos sueltos (--codes) son pruebas: si se
    # persistieran, la próxima corrida completa vería ~287k islas "nuevas" en vez de una
    # carga inicial. Solo se guardan snapshots y fetch_log.
    persist_dim = codes_override is None and not limit
    part = [0]

    def write(table: str, df: pl.DataFrame) -> None:
        if dry_run:
            summary.rows[table] += df.height
            return
        path = write_partition(df, data_dir, PLATFORM, table, started, run_id, part[0])
        summary.add_file(table, path, df.height)

    def write_dim(new_dim: pl.DataFrame) -> None:
        """Agrega al log solo las filas nuevas o cambiadas desde la última escritura."""
        if dry_run:
            summary.rows["games (cambios dim)"] += dimension_changes(
                persisted[0], new_dim, KEY).height
        else:
            path, n = write_dimension_changes(
                persisted[0], new_dim, KEY, data_dir, PLATFORM, started, run_id, part[0])
            summary.add_file("games (cambios dim)", path, n)
        persisted[0] = new_dim

    all_snaps: list[pl.DataFrame] = []
    all_logs: list[pl.DataFrame] = []

    def flush(batch: tuple[list[dict], list[dict]]) -> None:
        snaps = frame(batch[0], SNAPSHOT_SCHEMA)
        logs = frame(batch[1], FETCH_LOG_SCHEMA)
        part[0] += 1
        write("snapshots", snaps)
        write("fetch_log", logs)
        all_snaps.append(snaps.select(HISTORY_SNAP_COLS))
        all_logs.append(logs)
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
            if persist_dim:
                write("events", events)
                write_dim(dim)  # persistir altas/bajas aunque después falle algo
            if group is not None:
                codes = select_today(dim, group, n_groups)
                summary.notes.append(f"grupo {group}/{n_groups} (sin tiers): "
                                     f"{len(codes)} islas a consultar")
            else:
                listed = dim.filter(pl.col("is_listed"))
                # tiers con la historia: las islas nuevas del catálogo de hoy ya entran hot
                planned = tiers.assign_fortnite_tiers(
                    listed, hist_day, _fetched(hist_log), started, rules)
                codes, counts = tiers.select_fortnite_due(
                    planned, _last_fetch(hist_log, "day"), started, rules["cadence"])
                summary.notes.append(
                    f"{len(codes)} islas a consultar: "
                    + " · ".join(f"{k} {v}" for k, v in counts.items()))
            if limit:
                codes = codes[:limit]
        else:
            codes = codes_override
            summary.notes.append(f"{len(codes)} islas a consultar (--codes)")

        last_hour = _last_fetch(hist_log, "hour")
        fetcher = MetricsFetcher(http, cfg, limiter, summary, deadline, dict(
            zip(last_hour["island_code"].to_list(), last_hour["last_at"].to_list(),
                strict=True)))
        await fetcher.run(codes, flush)
        flush(fetcher.take())

    snaps_all = (pl.concat(all_snaps) if all_snaps
                 else pl.DataFrame(schema=HISTORY_SNAP_SCHEMA))
    if persist_dim:
        new_dim = apply_traction(dim, snaps_all, int(fcfg["traction_min_ccu"]))
        if group is None:
            new_dim = _retier(new_dim, hist_day, hist_log, all_logs, snaps_all, started, rules,
                              summary, write)
        write_dim(new_dim)
    else:
        summary.notes.append("prueba (--limit/--codes): dimensión y events no se persisten")

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
