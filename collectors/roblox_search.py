"""Barrido diario de búsqueda de Roblox (omni-search) → `search_positions`.

Por qué es un colector aparte: desde IPs de datacenter (GitHub Actions) omni-search permite
**1 request por minuto** (`x-ratelimit-limit: 1, 1;w=60`, ventana fija; medido 2026-11-10),
mientras que desde una IP residencial son 4/s. Con ese límite la búsqueda no entra en la
corrida horaria, así que corre una vez por día con un presupuesto de tiempo y 1 request cada
61 s. No reparte requests entre varios runners para sumar IPs: respeta el límite publicado.

Recorre las keywords de `config/keywords.yaml` en anchura (página 1 de todas, después la 2,
…) hasta `search_max_pages` o hasta agotar el presupuesto, así todas las keywords quedan
cubiertas aunque el presupuesto no alcance para todas las páginas. Los tokens de página son
stateless (`{"start","count","endOfPage"}` en base64), así que se arman directamente.

El colector horario (`collectors.roblox`) lee los `search_positions` recientes y empieza a
trackear los resultados con jugadores que todavía no seguía.

Uso: python -m collectors.roblox_search [--budget-minutes N] [--max-pages N]
                                        [--keywords a,b] [--data-dir PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from collectors.common import (
    ROOT,
    TS,
    Http,
    HttpError,
    RateLimiter,
    RunSummary,
    frame,
    load_config,
    new_run_id,
    resolve_data_dir,
    setup_logging,
    utcnow,
    write_partition,
)

PLATFORM = "roblox"
TABLE = "search_positions"
KEYWORDS_PATH = ROOT / "config" / "keywords.yaml"
PAGE_SIZE = 40  # fijo del lado de Roblox (no hay parámetro que lo cambie)
log = logging.getLogger("collectors.roblox_search")

SEARCH_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_at": TS,
    "keyword": pl.Utf8,
    "rank": pl.Int32,
    "universe_id": pl.Int64,
    "root_place_id": pl.Int64,
    "player_count": pl.Int64,
    "up_votes": pl.Int64,
    "down_votes": pl.Int64,
    "is_sponsored": pl.Boolean,
    "age_rating": pl.Utf8,
}


def load_keywords(path: Path = KEYWORDS_PATH) -> list[str]:
    return [str(k) for k in load_config(path)[PLATFORM]]


def page_token(start: int) -> str:
    raw = json.dumps({"start": start, "count": PAGE_SIZE, "endOfPage": False},
                     separators=(",", ":"))
    return base64.b64encode(raw.encode()).decode()


def parse_search_page(
    payload: dict[str, Any], keyword: str, snapshot_at: datetime, rank_offset: int
) -> list[dict]:
    games = [
        c
        for group in payload.get("searchResults") or []
        for c in group.get("contents") or []
        if c.get("contentType") == "Game" and c.get("universeId")
    ]
    return [
        {
            "snapshot_at": snapshot_at,
            "keyword": keyword,
            "rank": rank_offset + i,
            "universe_id": g["universeId"],
            "root_place_id": g.get("rootPlaceId"),
            "player_count": g.get("playerCount"),
            "up_votes": g.get("totalUpVotes"),
            "down_votes": g.get("totalDownVotes"),
            "is_sponsored": g.get("isSponsored"),
            "age_rating": g.get("contentMaturity"),
        }
        for i, g in enumerate(games, start=1)
    ]


async def sweep(
    http: Http,
    cfg: dict,
    keywords: list[str],
    summary: RunSummary,
    deadline: float,
    max_pages: int,
    on_pass,
) -> int:
    """Recorre las keywords en anchura. Llama `on_pass(rows)` al terminar cada pasada.

    Devuelve la cantidad de páginas pedidas.
    """
    rcfg = cfg["roblox"]
    limiter = RateLimiter(float(rcfg["rate_search_per_s"]))
    active = list(keywords)
    requested = 0
    for page in range(max_pages):
        rows: list[dict] = []
        still_active: list[str] = []
        for kw in active:
            if time.monotonic() >= deadline:
                summary.notes.append(f"presupuesto agotado en la página {page + 1} ({kw!r})")
                on_pass(rows)
                return requested
            params: dict[str, Any] = {"searchQuery": kw, "sessionId": str(uuid.uuid4()),
                                      "pageType": "all"}
            if page:
                params["pageToken"] = page_token(page * PAGE_SIZE)
            summary.ops_total += 1
            requested += 1
            try:
                payload = await http.get_json(rcfg["search_url"], params, limiter)
            except HttpError as exc:
                summary.ops_failed += 1
                summary.error(f"search_{exc.kind}")
                log.warning("búsqueda %r página %d falló: %s", kw, page + 1, exc)
                still_active.append(kw)  # se reintenta en la próxima pasada
                continue
            found = parse_search_page(payload, kw, utcnow(), page * PAGE_SIZE)
            rows.extend(found)
            if payload.get("nextPageToken") and len(found) >= PAGE_SIZE // 2:
                still_active.append(kw)
        log.info("pasada %d: %d keywords, %d resultados", page + 1, len(active), len(rows))
        on_pass(rows)
        active = still_active
        if not active:
            break
    return requested


async def run(
    data_dir: Path,
    cfg: dict,
    keywords: list[str] | None = None,
    budget_minutes: float | None = None,
    max_pages: int | None = None,
    dry_run: bool = False,
) -> RunSummary:
    rcfg = cfg["roblox"]
    run_id = new_run_id()
    summary = RunSummary(PLATFORM, run_id)
    started = summary.started_at
    keywords = keywords if keywords is not None else load_keywords()
    budget = float(budget_minutes if budget_minutes is not None
                   else rcfg["search_budget_minutes"])
    max_pages = int(max_pages if max_pages is not None else rcfg["search_max_pages"])
    deadline = time.monotonic() + budget * 60
    part = [0]

    def on_pass(rows: list[dict]) -> None:
        df = frame(rows, SEARCH_SCHEMA)
        part[0] += 1
        if dry_run:
            summary.rows[TABLE] += df.height
            return
        path = write_partition(df, data_dir, PLATFORM, TABLE, started, run_id, part[0])
        summary.add_file(TABLE, path, df.height)

    async with Http(cfg) as http:
        summary.http = http.stats
        pages = await sweep(http, cfg, keywords, summary, deadline, max_pages, on_pass)
    summary.notes.append(
        f"{len(keywords)} keywords · {pages} páginas pedidas · presupuesto {budget:.0f} min · "
        f"máx {max_pages} páginas por keyword"
    )
    if dry_run:
        summary.notes.append("dry-run: no se escribió nada")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--budget-minutes", type=float, default=None)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--keywords", default=None, help="lista separada por comas")
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)
    cfg = load_config()
    kws = [k.strip() for k in args.keywords.split(",")] if args.keywords else None
    summary = asyncio.run(run(resolve_data_dir(args.data_dir), cfg, kws, args.budget_minutes,
                              args.max_pages, args.dry_run))
    summary.emit()
    return summary.exit_code(float(cfg["run"]["max_error_rate"]))


if __name__ == "__main__":
    sys.exit(main())
