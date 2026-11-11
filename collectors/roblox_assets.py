"""Colector diario de "empaque" y monetización de Roblox: ícono, miniaturas y gamepasses.

La API solo muestra el estado actual, así que la historia hay que guardarla día a día:

1. Elige los juegos de la dimensión: hot y warm todos los días, cold una vez por semana
   (por hash del id).
2. Ícono (`thumbnails/v1/games/icons`, 100 por request): el hash de la URL identifica la
   imagen (es el mismo en todos los tamaños y estable entre llamadas).
3. Miniaturas del carrusel (`thumbnails/v1/games/multiget/thumbnails`, 100 por request,
   60 requests/min): ids de asset de cada imagen, en orden (la primera es la que se ve).
4. Gamepasses (`apis.roblox.com/game-passes/v1/universes/{id}/game-passes`, uno por juego,
   paginado): nombre, precio, a la venta, fechas.

Se guardan como logs de cambios (como la dimensión `games`), así no se repiten miles de filas
iguales por día:
- `media` (universe_id): icon_hash, thumbnail_ids. Cambios → `events` (icon_change,
  thumbnail_change).
- `gamepasses` (universe_id, pass_id): una fila por pase cuando aparece o cambia algo; si
  desaparece del listado, fila con `is_listed = false`.
- `asset_checks`: una fila por juego consultado (para saber entre qué días cambió algo).

Uso: python -m collectors.roblox_assets [--limit N] [--data-dir PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from collectors.common import (
    TS,
    Http,
    HttpError,
    RateLimiter,
    RunSummary,
    chunked,
    dimension_changes,
    frame,
    group_of,
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
from collectors.roblox import EVENTS_SCHEMA, GAMES_SCHEMA

PLATFORM = "roblox"
KEY = "universe_id"
PASS_KEY = ["universe_id", "pass_id"]
log = logging.getLogger("collectors.roblox_assets")

MEDIA_SCHEMA: dict[str, pl.DataType] = {
    "universe_id": pl.Int64,
    "icon_state": pl.Utf8,          # Completed | Blocked | Pending | Error …
    "icon_hash": pl.Utf8,           # identifica la imagen del ícono
    "thumbnail_ids": pl.List(pl.Int64),  # assets del carrusel, en orden
}
GAMEPASS_SCHEMA: dict[str, pl.DataType] = {
    "universe_id": pl.Int64,
    "pass_id": pl.Int64,
    "product_id": pl.Int64,
    "name": pl.Utf8,
    "description": pl.Utf8,
    "price": pl.Int64,              # null si no está a la venta
    "is_for_sale": pl.Boolean,
    "icon_asset_id": pl.Int64,
    "created_at": TS,
    "updated_at": TS,
    "is_listed": pl.Boolean,        # false: desapareció del listado del juego
}
CHECK_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_at": TS,
    "universe_id": pl.Int64,
    "media_ok": pl.Boolean,
    "gamepasses_status": pl.Utf8,   # ok | not_found | error
    "n_gamepasses": pl.Int32,
}

_HASH = re.compile(r"/\d+DAY-([0-9a-f]+)")


# --------------------------------------------------------------------------- selección


def select_ids(dim: pl.DataFrame, now: datetime, cold_days: int) -> list[int]:
    """hot y warm: todos; cold: los de su día de la semana (hash del id)."""
    day = now.date().toordinal() % cold_days
    out = []
    for uid, tier in zip(dim[KEY].to_list(), dim["tier"].to_list(), strict=True):
        if tier != "cold" or group_of(str(uid), cold_days) == day:
            out.append(uid)
    return out


# --------------------------------------------------------------------------- parseo


def icon_hash(url: str | None) -> str | None:
    m = _HASH.search(url or "")
    return m.group(1) if m else None


def parse_icons(items: list[dict]) -> dict[int, dict[str, Any]]:
    return {
        int(x["targetId"]): {"icon_state": x.get("state"),
                             "icon_hash": icon_hash(x.get("imageUrl"))}
        for x in items if x.get("targetId") is not None
    }


def parse_thumbnails(items: list[dict]) -> dict[int, list[int]]:
    return {
        int(x["universeId"]): [int(t["targetId"]) for t in x.get("thumbnails") or []
                               if t.get("targetId") is not None]
        for x in items if x.get("universeId") is not None and not x.get("error")
    }


def parse_gamepasses(universe_id: int, items: list[dict]) -> list[dict[str, Any]]:
    return [
        {
            "universe_id": universe_id,
            "pass_id": g.get("id"),
            "product_id": g.get("productId"),
            "name": g.get("displayName") or g.get("name"),
            "description": g.get("displayDescription"),
            "price": g.get("price"),
            "is_for_sale": g.get("isForSale"),
            "icon_asset_id": g.get("displayIconImageAssetId"),
            "created_at": parse_ts(g.get("created")),
            "updated_at": parse_ts(g.get("updated")),
            "is_listed": True,
        }
        for g in items if g.get("id") is not None
    ]


# --------------------------------------------------------------------------- requests


async def fetch_batched(http: Http, url: str, ids: list[int], params: dict[str, Any],
                        limiter: RateLimiter, summary: RunSummary, label: str
                        ) -> list[dict]:
    """GET en lotes de 100 `universeIds`; devuelve los `data` de los lotes exitosos."""
    async def one(chunk: list[int]) -> list[dict]:
        summary.ops_total += 1
        try:
            payload = await http.get_json(
                url, {"universeIds": ",".join(map(str, chunk)), **params}, limiter)
        except HttpError as exc:
            summary.ops_failed += 1
            summary.error(f"{label}_{exc.kind}")
            log.warning("%s falló para %d ids: %s", label, len(chunk), exc)
            return []
        return payload.get("data") or []

    results = await asyncio.gather(*(one(c) for c in chunked(ids, 100)))
    return [x for r in results for x in r]


async def fetch_gamepasses(http: Http, cfg: dict, ids: list[int], limiter: RateLimiter,
                           summary: RunSummary) -> tuple[list[dict], dict[int, tuple[str, int]]]:
    """(filas de pases, {universe_id: (status, n_pases)})."""
    rcfg = cfg["roblox"]
    url = rcfg["gamepasses_url"]
    sem = asyncio.Semaphore(int(rcfg["concurrency"]))
    max_pages = int(rcfg["gamepasses_max_pages"])
    rows: list[dict] = []
    status: dict[int, tuple[str, int]] = {}

    async def one(uid: int) -> None:
        async with sem:
            summary.ops_total += 1
            passes: list[dict] = []
            params: dict[str, Any] = {"passView": "Full", "pageSize": 100}
            try:
                for _ in range(max_pages):
                    payload = await http.get_json(url.format(universe_id=uid), params, limiter)
                    passes.extend(payload.get("gamePasses") or [])
                    token = payload.get("nextPageToken")
                    if not token:
                        break
                    params = {**params, "pageToken": token}
            except HttpError as exc:
                if exc.status == 404:  # universo borrado o inexistente
                    status[uid] = ("not_found", 0)
                    return
                summary.ops_failed += 1
                summary.error(f"gamepasses_{exc.kind}")
                status[uid] = ("error", 0)
                return
            parsed = parse_gamepasses(uid, passes)
            rows.extend(parsed)
            status[uid] = ("ok", len(parsed))

    await asyncio.gather(*(one(uid) for uid in ids))
    return rows, status


# --------------------------------------------------------------------------- cambios


def _ids_str(v: list[int] | None) -> str | None:
    return None if v is None else ",".join(map(str, v))


def media_events(old: pl.DataFrame, new: pl.DataFrame, now: datetime) -> pl.DataFrame:
    """icon_change / thumbnail_change para juegos que ya tenían estado (no en el primero)."""
    j = new.join(old, on=KEY, how="inner", suffix="_old")
    rows = []
    for r in j.iter_rows(named=True):
        if r["icon_hash"] is not None and r["icon_hash_old"] is not None \
                and r["icon_hash"] != r["icon_hash_old"]:
            rows.append({"detected_at": now, KEY: r[KEY], "event_type": "icon_change",
                         "old_value": r["icon_hash_old"], "new_value": r["icon_hash"]})
        if r["thumbnail_ids"] != r["thumbnail_ids_old"]:
            rows.append({"detected_at": now, KEY: r[KEY], "event_type": "thumbnail_change",
                         "old_value": _ids_str(r["thumbnail_ids_old"]),
                         "new_value": _ids_str(r["thumbnail_ids"])})
    return frame(rows, EVENTS_SCHEMA)


def gamepass_state(old: pl.DataFrame, fetched: pl.DataFrame, ok_ids: list[int]) -> pl.DataFrame:
    """Estado nuevo de los pases de los juegos consultados con éxito: los listados hoy, más
    los que estaban listados y desaparecieron (con is_listed = false)."""
    old_ok = old.filter(pl.col(KEY).is_in(ok_ids) & pl.col("is_listed"))
    gone = old_ok.join(fetched.select(PASS_KEY), on=PASS_KEY, how="anti").with_columns(
        pl.lit(False).alias("is_listed"))
    return pl.concat([fetched, gone.select(fetched.columns)]).cast(GAMEPASS_SCHEMA)


# --------------------------------------------------------------------------- main


async def run(data_dir: Path, cfg: dict, limit: int | None = None,
              dry_run: bool = False) -> RunSummary:
    rcfg = cfg["roblox"]
    run_id = new_run_id()
    summary = RunSummary(PLATFORM, run_id)
    started = summary.started_at

    dim = read_dimension(data_dir, PLATFORM, GAMES_SCHEMA, KEY)
    ids = select_ids(dim, started, int(rcfg["assets_cold_days"]))
    if limit:
        ids = ids[:limit]
    summary.notes.append(f"juegos a consultar: {len(ids)} de {dim.height}")

    async with Http(cfg) as http:
        summary.http = http.stats
        icons_lim = RateLimiter(float(rcfg["rate_icons_per_s"]), adaptive=True)
        thumbs_lim = RateLimiter(float(rcfg["rate_thumbnails_per_s"]), adaptive=True)
        passes_lim = RateLimiter(float(rcfg["rate_gamepasses_per_s"]), adaptive=True)
        icons_raw, thumbs_raw, (pass_rows, pass_status) = await asyncio.gather(
            fetch_batched(http, rcfg["icons_url"], ids,
                          {"size": "512x512", "format": "Png", "isCircular": "false"},
                          icons_lim, summary, "icons"),
            fetch_batched(http, rcfg["thumbnails_url"], ids,
                          {"countPerUniverse": 10, "size": "768x432", "format": "Png"},
                          thumbs_lim, summary, "thumbnails"),
            fetch_gamepasses(http, cfg, ids, passes_lim, summary),
        )
    checked_at = utcnow()

    icons = parse_icons(icons_raw)
    thumbs = parse_thumbnails(thumbs_raw)
    media_ids = [u for u in ids if u in icons and u in thumbs]  # solo con ambas partes
    media_new = frame([{KEY: u, **icons[u], "thumbnail_ids": thumbs[u]} for u in media_ids],
                      MEDIA_SCHEMA)
    media_old = read_dimension(data_dir, PLATFORM, MEDIA_SCHEMA, KEY, table="media")
    events = media_events(media_old, media_new, checked_at)

    ok_ids = [u for u, (s, _) in pass_status.items() if s == "ok"]
    pass_old = read_dimension(data_dir, PLATFORM, GAMEPASS_SCHEMA, PASS_KEY,
                              table="gamepasses")
    pass_new = gamepass_state(pass_old, frame(pass_rows, GAMEPASS_SCHEMA), ok_ids)
    pass_changes = dimension_changes(pass_old, pass_new, PASS_KEY)

    media_set = set(media_ids)
    checks = frame([{"snapshot_at": checked_at, KEY: u, "media_ok": u in media_set,
                     "gamepasses_status": pass_status.get(u, ("error", 0))[0],
                     "n_gamepasses": pass_status.get(u, ("error", 0))[1]} for u in ids],
                   CHECK_SCHEMA)

    if dry_run:
        summary.rows["media (cambios)"] += dimension_changes(media_old, media_new, KEY).height
        summary.rows["gamepasses (cambios)"] += pass_changes.height
        summary.rows["events"] += events.height
        summary.rows["asset_checks"] += checks.height
        summary.notes.append("dry-run: no se escribió nada")
    else:
        path, n = write_dimension_changes(media_old, media_new, KEY, data_dir, PLATFORM,
                                          started, run_id, table="media")
        summary.add_file("media (cambios)", path, n)
        path, n = write_dimension_changes(pass_old, pass_new, PASS_KEY, data_dir, PLATFORM,
                                          started, run_id, table="gamepasses")
        summary.add_file("gamepasses (cambios)", path, n)
        for table, df in (("events", events), ("asset_checks", checks)):
            summary.add_file(table, write_partition(df, data_dir, PLATFORM, table, started,
                                                    run_id), df.height)

    first = media_old.is_empty()
    n_icon = events.filter(pl.col("event_type") == "icon_change").height
    n_thumb = events.filter(pl.col("event_type") == "thumbnail_change").height
    statuses: dict[str, int] = {}
    for s, _ in pass_status.values():
        statuses[s] = statuses.get(s, 0) + 1
    summary.notes += [
        f"ícono + miniaturas: {len(media_ids)} juegos"
        + (" (primera carga: sin eventos)" if first else
           f" · cambios de ícono: {n_icon} · de miniaturas: {n_thumb}"),
        "gamepasses: " + " · ".join(f"{k} {v}" for k, v in sorted(statuses.items()))
        + f" · pases listados: {pass_new.filter(pl.col('is_listed')).height}"
        + f" · nuevos o cambiados: {pass_changes.height}",
    ]
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--limit", type=int, default=None, help="máximo de juegos (pruebas)")
    parser.add_argument("--data-dir", default=None, help="raíz de datos (default ./data)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)
    cfg = load_config()
    summary = asyncio.run(run(resolve_data_dir(args.data_dir), cfg, args.limit, args.dry_run))
    summary.emit()
    return summary.exit_code(float(cfg["run"]["max_error_rate"]))


if __name__ == "__main__":
    sys.exit(main())
