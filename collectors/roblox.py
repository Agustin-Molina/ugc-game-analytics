"""Colector horario de Roblox.

1. Charts de explore (`get-sorts`, device=all, country=all) → `chart_positions`.
2. Universo a consultar = IDs de charts ∪ IDs ya trackeados (dimensión `games`).
3. Detalles en lotes de 50 y votos en lotes de 100 → `snapshots`.
4. Actualiza la dimensión `games` y registra cambios en `events`.

Uso: python -m collectors.roblox [--limit N] [--data-dir PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import uuid
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
    frame,
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

PLATFORM = "roblox"
log = logging.getLogger("collectors.roblox")

CHART_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_at": TS,
    "sort_id": pl.Utf8,
    "sort_name": pl.Utf8,
    "rank": pl.Int32,
    "universe_id": pl.Int64,
    "root_place_id": pl.Int64,
    "player_count": pl.Int64,
    "up_votes": pl.Int64,
    "down_votes": pl.Int64,
    "is_sponsored": pl.Boolean,
}

SNAPSHOT_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_at": TS,
    "universe_id": pl.Int64,
    "playing": pl.Int64,
    "visits": pl.Int64,
    "favorites": pl.Int64,
    "up_votes": pl.Int64,
    "down_votes": pl.Int64,
    "updated_at": TS,
    "max_players": pl.Int32,
    "price": pl.Int64,
}

GAMES_SCHEMA: dict[str, pl.DataType] = {
    "universe_id": pl.Int64,
    "root_place_id": pl.Int64,
    "name": pl.Utf8,
    "description": pl.Utf8,
    "creator_id": pl.Int64,
    "creator_name": pl.Utf8,
    "creator_type": pl.Utf8,
    "creator_verified": pl.Boolean,
    "genre_raw": pl.Utf8,
    "genre_l1": pl.Utf8,
    "genre_l2": pl.Utf8,
    "genre_unified": pl.Utf8,  # Fase 2 (config/genres.yaml)
    "age_rating": pl.Utf8,
    "is_officially_licensed": pl.Boolean,
    "max_players": pl.Int32,
    "created_at": TS,
    "updated_at": TS,
    "first_seen_at": TS,
    "last_seen_at": TS,
    "first_traction_at": TS,
    "discovered_via": pl.Utf8,
    "tier": pl.Utf8,
}

EVENTS_SCHEMA: dict[str, pl.DataType] = {
    "detected_at": TS,
    "universe_id": pl.Int64,
    "event_type": pl.Utf8,
    "old_value": pl.Utf8,
    "new_value": pl.Utf8,
}


# --------------------------------------------------------------------------- charts


def parse_sorts_page(payload: dict[str, Any], snapshot_at: datetime) -> tuple[list[dict], dict]:
    """Filas de `chart_positions` de una página de get-sorts + metadata por juego."""
    rows: list[dict] = []
    meta: dict[int, dict] = {}
    for sort in payload.get("sorts") or []:
        if sort.get("contentType") != "Games":
            continue
        sort_id = sort.get("sortId") or sort.get("id")
        for rank, g in enumerate(sort.get("games") or [], start=1):
            uid = g.get("universeId")
            if uid is None:
                continue
            rows.append(
                {
                    "snapshot_at": snapshot_at,
                    "sort_id": sort_id,
                    "sort_name": sort.get("sortDisplayName"),
                    "rank": rank,
                    "universe_id": uid,
                    "root_place_id": g.get("rootPlaceId"),
                    "player_count": g.get("playerCount"),
                    "up_votes": g.get("totalUpVotes"),
                    "down_votes": g.get("totalDownVotes"),
                    "is_sponsored": g.get("isSponsored"),
                }
            )
            meta.setdefault(uid, {"sort_id": sort_id, "age_rating": g.get("contentMaturity")})
    return rows, meta


async def fetch_charts(
    http: Http, cfg: dict, limiter: RateLimiter, summary: RunSummary
) -> tuple[list[dict], dict[int, dict]]:
    rcfg = cfg["roblox"]
    session_id = str(uuid.uuid4())
    params: dict[str, Any] = {"sessionId": session_id, "device": "all", "country": "all"}
    rows: list[dict] = []
    meta: dict[int, dict] = {}
    for _ in range(int(rcfg["max_sort_pages"])):
        summary.ops_total += 1
        try:
            payload = await http.get_json(rcfg["sorts_url"], params, limiter)
        except HttpError as exc:
            summary.ops_failed += 1
            summary.error(f"sorts_{exc.kind}")
            log.warning("get-sorts falló: %s", exc)
            break
        page_rows, page_meta = parse_sorts_page(payload, utcnow())
        rows.extend(page_rows)
        for uid, m in page_meta.items():
            meta.setdefault(uid, m)
        token = payload.get("nextSortsPageToken")
        if not token:
            break
        params = {**params, "sortsPageToken": token}
    return rows, meta


# --------------------------------------------------------------------------- detalles


def parse_game(g: dict[str, Any]) -> dict[str, Any]:
    creator = g.get("creator") or {}
    return {
        "universe_id": g.get("id"),
        "root_place_id": g.get("rootPlaceId"),
        "name": g.get("name"),
        "description": g.get("description"),
        "creator_id": creator.get("id"),
        "creator_name": creator.get("name"),
        "creator_type": creator.get("type"),
        "creator_verified": creator.get("hasVerifiedBadge"),
        "genre_raw": g.get("genre"),
        "genre_l1": g.get("genre_l1"),
        "genre_l2": g.get("genre_l2"),
        "is_officially_licensed": g.get("isOfficiallyLicensed"),
        "max_players": g.get("maxPlayers"),
        "created_at": parse_ts(g.get("created")),
        "updated_at": parse_ts(g.get("updated")),
        "playing": g.get("playing"),
        "visits": g.get("visits"),
        "favorites": g.get("favoritedCount"),
        "price": g.get("price"),
    }


async def _fetch_batches(
    http: Http,
    url: str,
    ids: list[int],
    batch: int,
    limiter: RateLimiter,
    sem: asyncio.Semaphore,
    summary: RunSummary,
    label: str,
) -> list[tuple[datetime, list[dict]]]:
    async def one(chunk: list[int]) -> tuple[datetime, list[dict]] | None:
        async with sem:
            summary.ops_total += 1
            try:
                payload = await http.get_json(
                    url, {"universeIds": ",".join(map(str, chunk))}, limiter
                )
            except HttpError as exc:
                summary.ops_failed += 1
                summary.error(f"{label}_{exc.kind}")
                log.warning("%s falló para %d ids: %s", label, len(chunk), exc)
                return None
            return utcnow(), payload.get("data") or []

    results = await asyncio.gather(*(one(c) for c in chunked(ids, batch)))
    return [r for r in results if r is not None]


async def fetch_details_and_votes(
    http: Http, cfg: dict, ids: list[int], summary: RunSummary
) -> tuple[dict[int, dict], dict[int, dict]]:
    rcfg = cfg["roblox"]
    sem = asyncio.Semaphore(int(rcfg["concurrency"]))
    # Roblox limita por IDs pedidos, no por requests → tasa de requests = IDs/s ÷ lote
    games_lim = RateLimiter(rcfg["games_ids_per_s"] / rcfg["games_batch"], adaptive=True)
    votes_lim = RateLimiter(rcfg["votes_ids_per_s"] / rcfg["votes_batch"], adaptive=True)
    det_batches, vote_batches = await asyncio.gather(
        _fetch_batches(http, rcfg["games_url"], ids, int(rcfg["games_batch"]), games_lim, sem,
                       summary, "games"),
        _fetch_batches(http, rcfg["votes_url"], ids, int(rcfg["votes_batch"]), votes_lim, sem,
                       summary, "votes"),
    )
    details: dict[int, dict] = {}
    for fetched_at, items in det_batches:
        for g in items:
            row = parse_game(g)
            row["snapshot_at"] = fetched_at
            details[row["universe_id"]] = row
    votes: dict[int, dict] = {}
    for _, items in vote_batches:
        for v in items:
            votes[v["id"]] = {"up_votes": v.get("upVotes"), "down_votes": v.get("downVotes")}
    return details, votes


def build_snapshots(details: dict[int, dict], votes: dict[int, dict]) -> pl.DataFrame:
    rows = []
    for uid, d in details.items():
        v = votes.get(uid, {})
        rows.append(
            {
                "snapshot_at": d["snapshot_at"],
                "universe_id": uid,
                "playing": d["playing"],
                "visits": d["visits"],
                "favorites": d["favorites"],
                "up_votes": v.get("up_votes"),
                "down_votes": v.get("down_votes"),
                "updated_at": d["updated_at"],
                "max_players": d["max_players"],
                "price": d["price"],
            }
        )
    return frame(rows, SNAPSHOT_SCHEMA).sort("universe_id")


# --------------------------------------------------------------------------- dimensión


DIM_FIELDS = [
    "root_place_id", "name", "description", "creator_id", "creator_name", "creator_type",
    "creator_verified", "genre_raw", "genre_l1", "genre_l2", "is_officially_licensed",
    "max_players", "created_at", "updated_at",
]
TRACKED_CHANGES = {"name": "name_change", "description": "description_change",
                   "updated_at": "update"}


def _as_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


def update_dimension(
    dim: pl.DataFrame,
    details: dict[int, dict],
    chart_meta: dict[int, dict],
    now: datetime,
    traction_min_ccu: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Upsert de la dimensión con los detalles de esta corrida. Devuelve (dim, events)."""
    current = {r["universe_id"]: r for r in dim.iter_rows(named=True)}
    events: list[dict] = []
    for uid, d in details.items():
        old = current.get(uid)
        meta = chart_meta.get(uid, {})
        if old is None:
            row = {k: None for k in GAMES_SCHEMA}
            row.update(universe_id=uid, first_seen_at=now, tier="warm",
                       discovered_via=f"chart:{meta['sort_id']}" if meta else "tracked")
        else:
            row = dict(old)
            for field, event_type in TRACKED_CHANGES.items():
                if d[field] is not None and old.get(field) != d[field]:
                    events.append(
                        {
                            "detected_at": now,
                            "universe_id": uid,
                            "event_type": event_type,
                            "old_value": _as_str(old.get(field)),
                            "new_value": _as_str(d[field]),
                        }
                    )
        for field in DIM_FIELDS:
            if d.get(field) is not None:
                row[field] = d[field]
        if meta.get("age_rating"):
            row["age_rating"] = meta["age_rating"]
        row["last_seen_at"] = d["snapshot_at"]
        if row.get("first_traction_at") is None and (d.get("playing") or 0) >= traction_min_ccu:
            row["first_traction_at"] = d["snapshot_at"]
        current[uid] = row
    new_dim = frame(list(current.values()), GAMES_SCHEMA).sort("universe_id")
    return new_dim, frame(events, EVENTS_SCHEMA)


# --------------------------------------------------------------------------- main


async def run(data_dir: Path, limit: int | None, dry_run: bool, cfg: dict) -> RunSummary:
    run_id = new_run_id()
    summary = RunSummary(PLATFORM, run_id)
    started = summary.started_at
    rcfg = cfg["roblox"]
    dim = read_dimension(data_dir, PLATFORM, GAMES_SCHEMA)

    async with Http(cfg) as http:
        summary.http = http.stats
        explore_lim = RateLimiter(rcfg["rate_explore_per_s"], adaptive=True)
        chart_rows, chart_meta = await fetch_charts(http, cfg, explore_lim, summary)
        chart_ids = list(dict.fromkeys(r["universe_id"] for r in chart_rows))
        ids = list(dict.fromkeys(chart_ids + dim["universe_id"].to_list()))
        if limit:
            ids = ids[:limit]
        summary.notes.append(
            f"charts: {len(chart_ids)} juegos únicos · trackeados: {dim.height} · "
            f"consultados: {len(ids)}"
        )
        details, votes = await fetch_details_and_votes(http, cfg, ids, summary)

    missing = len(set(ids) - set(details))
    if missing:
        summary.notes.append(f"{missing} ids sin detalle (privados/borrados o lote fallido)")

    charts_df = frame(chart_rows, CHART_SCHEMA)
    snaps_df = build_snapshots(details, votes)
    new_dim, events_df = update_dimension(
        dim, details, chart_meta, started, int(rcfg["traction_min_ccu"])
    )
    summary.notes.append(f"juegos nuevos en la dimensión: {new_dim.height - dim.height}")

    if dry_run:
        for name, df in (("chart_positions", charts_df), ("snapshots", snaps_df),
                         ("events", events_df), ("games", new_dim)):
            summary.rows[name] += df.height
        summary.notes.append("dry-run: no se escribió nada")
        return summary

    for table, df in (("chart_positions", charts_df), ("snapshots", snaps_df),
                      ("events", events_df)):
        path = write_partition(df, data_dir, PLATFORM, table, started, run_id)
        summary.add_file(table, path, df.height)
    path = write_dimension(new_dim, data_dir, PLATFORM)
    summary.add_file("games (dim)", path, new_dim.height)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--limit", type=int, default=None, help="máximo de juegos a consultar")
    parser.add_argument("--data-dir", default=None, help="raíz de datos (default ./data)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)
    cfg = load_config()
    data_dir = resolve_data_dir(args.data_dir)
    summary = asyncio.run(run(data_dir, args.limit, args.dry_run, cfg))
    summary.emit()
    return summary.exit_code(float(cfg["run"]["max_error_rate"]))


if __name__ == "__main__":
    sys.exit(main())
