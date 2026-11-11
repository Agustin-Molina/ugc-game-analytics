"""Colector diario del historial de los creadores de Roblox: todos sus juegos públicos.

Para cada creador (usuario o grupo) de los juegos trackeados baja la lista de sus juegos
públicos con fecha de creación y visitas acumuladas. Eso responde "¿este creador ya tuvo
éxitos antes?" sin necesitar historia de CCU de sus juegos viejos.

Endpoints (verificados 2026-11-11):
- grupos:   games.roblox.com/v2/groups/{id}/games    (100 por página, 30 requests/min)
- usuarios: games.roblox.com/v2/users/{id}/games     (50 por página, 200 requests/min)
Ordenan del más nuevo al más viejo; una página alcanza para casi todos. Grupo inexistente →
400; usuario inexistente → lista vacía.

Selección por corrida, con presupuesto de tiempo (el límite de grupos es el cuello de botella):
1. creadores nunca consultados, primero los de juegos hot, después warm y cold;
2. después, los que no se consultan hace `creators_refresh_days`, del más viejo al más nuevo.

Tablas:
- `creator_games` (snapshot, append): una fila por juego de cada creador consultado.
- `creator_checks`: una fila por creador consultado (status ok | not_found | error, cantidad
  de juegos, si se cortó por máximo de páginas).

Uso: python -m collectors.roblox_creators [--limit N] [--budget-minutes M] [--dry-run]
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
    frame,
    load_config,
    new_run_id,
    parse_ts,
    read_dimension,
    resolve_data_dir,
    setup_logging,
    utcnow,
    write_partition,
)
from collectors.roblox import GAMES_SCHEMA

PLATFORM = "roblox"
log = logging.getLogger("collectors.roblox_creators")

CREATOR_GAMES_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_at": TS,
    "creator_type": pl.Utf8,        # User | Group
    "creator_id": pl.Int64,
    "universe_id": pl.Int64,
    "root_place_id": pl.Int64,
    "name": pl.Utf8,
    "created_at": TS,
    "updated_at": TS,
    "place_visits": pl.Int64,       # visitas acumuladas hasta snapshot_at
}
CREATOR_CHECKS_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_at": TS,
    "creator_type": pl.Utf8,
    "creator_id": pl.Int64,
    "status": pl.Utf8,              # ok | not_found | error
    "n_games": pl.Int32,
    "truncated": pl.Boolean,        # true: había más páginas que creators_max_pages
}
TIER_ORDER = {"hot": 0, "warm": 1, "cold": 2}

Creator = tuple[str, int]  # (creator_type, creator_id)


# --------------------------------------------------------------------------- selección


def last_checks(data_dir: Path) -> pl.DataFrame:
    """Último check exitoso (ok o not_found) por creador."""
    base = data_dir / PLATFORM / "creator_checks"
    files = sorted(base.glob("date=*/*.parquet"))
    if not files:
        return frame([], {"creator_type": pl.Utf8, "creator_id": pl.Int64, "last_at": TS})
    return (
        pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
        .filter(pl.col("status") != "error")
        .group_by("creator_type", "creator_id")
        .agg(pl.col("snapshot_at").max().alias("last_at"))
    )


def select_creators(dim: pl.DataFrame, checks: pl.DataFrame, now: datetime,
                    refresh_days: int) -> tuple[list[Creator], dict[str, int]]:
    """Creadores en orden de prioridad + conteos (nuevos, vencidos, al día)."""
    creators = (
        dim.filter(pl.col("creator_id").is_not_null()
                   & pl.col("creator_type").is_in(["User", "Group"]))
        .with_columns(pl.col("tier").replace_strict(TIER_ORDER, default=3).alias("rank"))
        .group_by("creator_type", "creator_id")
        .agg(pl.col("rank").min())
        .join(checks, on=["creator_type", "creator_id"], how="left")
    )
    new = creators.filter(pl.col("last_at").is_null()).sort("rank", "creator_id")
    due = creators.filter(pl.col("last_at") < now - timedelta(days=refresh_days)).sort(
        "last_at", "creator_id")
    order = [(t, int(i)) for t, i in new.select("creator_type", "creator_id").iter_rows()]
    order += [(t, int(i)) for t, i in due.select("creator_type", "creator_id").iter_rows()]
    counts = {"nuevos": new.height, "vencidos": due.height,
              "al día": creators.height - new.height - due.height}
    return order, counts


# --------------------------------------------------------------------------- parseo


def parse_games(creator: Creator, items: list[dict], now: datetime) -> list[dict[str, Any]]:
    ctype, cid = creator
    return [
        {
            "snapshot_at": now,
            "creator_type": ctype,
            "creator_id": cid,
            "universe_id": g.get("id"),
            "root_place_id": (g.get("rootPlace") or {}).get("id"),
            "name": g.get("name"),
            "created_at": parse_ts(g.get("created")),
            "updated_at": parse_ts(g.get("updated")),
            "place_visits": g.get("placeVisits"),
        }
        for g in items if g.get("id") is not None
    ]


# --------------------------------------------------------------------------- requests


async def fetch_creator(http: Http, rcfg: dict, creator: Creator, limiter: RateLimiter
                        ) -> tuple[str, list[dict], bool]:
    """(status, juegos crudos, truncated). Lanza HttpError en errores no esperados."""
    ctype, cid = creator
    if ctype == "Group":
        url, limit = rcfg["creator_group_games_url"], 100
    else:
        url, limit = rcfg["creator_user_games_url"], 50
    params: dict[str, Any] = {"accessFilter": "Public", "limit": limit, "sortOrder": "Desc"}
    games: list[dict] = []
    try:
        for _ in range(int(rcfg["creators_max_pages"])):
            payload = await http.get_json(url.format(id=cid), params, limiter)
            games.extend(payload.get("data") or [])
            cursor = payload.get("nextPageCursor")
            if not cursor:
                return "ok", games, False
            params = {**params, "cursor": cursor}
    except HttpError as exc:
        if exc.status in (400, 404):  # grupo inexistente o borrado
            return "not_found", [], False
        raise
    return "ok", games, True


async def sweep(http: Http, rcfg: dict, creators: list[Creator], limiter: RateLimiter,
                summary: RunSummary, deadline: float
                ) -> tuple[list[dict], list[dict], int]:
    """Consulta los creadores en orden hasta el deadline. (juegos, checks, sin consultar)."""
    games: list[dict] = []
    checks: list[dict] = []
    for n, creator in enumerate(creators):
        if time.monotonic() >= deadline:
            return games, checks, len(creators) - n
        summary.ops_total += 1
        try:
            status, raw, truncated = await fetch_creator(http, rcfg, creator, limiter)
        except HttpError as exc:
            summary.ops_failed += 1
            summary.error(f"creator_{exc.kind}")
            log.warning("creador %s/%s falló: %s", *creator, exc)
            status, raw, truncated = "error", [], False
        now = utcnow()
        parsed = parse_games(creator, raw, now)
        games.extend(parsed)
        checks.append({"snapshot_at": now, "creator_type": creator[0],
                       "creator_id": creator[1], "status": status,
                       "n_games": len(parsed), "truncated": truncated})
    return games, checks, 0


# --------------------------------------------------------------------------- main


async def run(data_dir: Path, cfg: dict, limit: int | None = None,
              budget_minutes: float | None = None, dry_run: bool = False) -> RunSummary:
    rcfg = cfg["roblox"]
    run_id = new_run_id()
    summary = RunSummary(PLATFORM, run_id)
    started = summary.started_at
    budget = float(budget_minutes if budget_minutes is not None
                   else rcfg["creators_budget_minutes"])
    deadline = time.monotonic() + budget * 60

    dim = read_dimension(data_dir, PLATFORM, GAMES_SCHEMA, "universe_id")
    order, counts = select_creators(dim, last_checks(data_dir), started,
                                    int(rcfg["creators_refresh_days"]))
    if limit:
        order = order[:limit]
    groups = [c for c in order if c[0] == "Group"]
    users = [c for c in order if c[0] == "User"]
    summary.notes.append(
        "creadores: " + " · ".join(f"{k} {v}" for k, v in counts.items())
        + f" · a consultar: {len(groups)} grupos, {len(users)} usuarios")

    async with Http(cfg) as http:
        summary.http = http.stats
        # límites distintos por endpoint: los grupos van por su lado, más lentos
        (g_games, g_checks, g_left), (u_games, u_checks, u_left) = await asyncio.gather(
            sweep(http, rcfg, groups,
                  RateLimiter(float(rcfg["rate_creator_groups_per_s"]), adaptive=True),
                  summary, deadline),
            sweep(http, rcfg, users,
                  RateLimiter(float(rcfg["rate_creator_users_per_s"]), adaptive=True),
                  summary, deadline),
        )

    games = frame(g_games + u_games, CREATOR_GAMES_SCHEMA)
    checks = frame(g_checks + u_checks, CREATOR_CHECKS_SCHEMA)
    if dry_run:
        summary.rows["creator_games"] += games.height
        summary.rows["creator_checks"] += checks.height
        summary.notes.append("dry-run: no se escribió nada")
    else:
        for table, df in (("creator_games", games), ("creator_checks", checks)):
            summary.add_file(table, write_partition(df, data_dir, PLATFORM, table, started,
                                                    run_id), df.height)

    statuses = checks.group_by("status").len().sort("status").iter_rows()
    summary.notes.append(
        "consultados: " + " · ".join(f"{s} {n}" for s, n in statuses)
        + f" · juegos: {games.height}"
        + f" · truncados: {checks.filter(pl.col('truncated')).height}")
    if g_left or u_left:
        summary.notes.append(f"presupuesto agotado: quedan {g_left} grupos y {u_left} "
                             "usuarios para la próxima corrida")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--limit", type=int, default=None, help="máximo de creadores (pruebas)")
    parser.add_argument("--budget-minutes", type=float, default=None,
                        help="presupuesto de tiempo (default: config)")
    parser.add_argument("--data-dir", default=None, help="raíz de datos (default ./data)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)
    cfg = load_config()
    summary = asyncio.run(run(resolve_data_dir(args.data_dir), cfg, args.limit,
                              args.budget_minutes, args.dry_run))
    summary.emit()
    return summary.exit_code(float(cfg["run"]["max_error_rate"]))


if __name__ == "__main__":
    sys.exit(main())
