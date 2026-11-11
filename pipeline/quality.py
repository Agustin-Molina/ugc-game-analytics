"""Checks de calidad de los datos recolectados.

    python -m pipeline.quality [--platform roblox|fortnite|all] [--data-dir PATH] [--no-write]

Cada check da ok / warn / error (o skip si todavía no hay historia suficiente). Los umbrales
están en config/quality.yaml. El resultado se imprime como tabla markdown (también en el Step
Summary de Actions) y se guarda en `<plataforma>/quality/date=YYYY-MM-DD/*.parquet`, así queda
la serie histórica. Con algún error el exit code es 1: el workflow falla y GitHub avisa.

Qué mira:
- continuidad: huecos entre corridas, última corrida, juegos e islas atrasados respecto de
  la cadencia de su tier (en Fortnite, pasado el atraso la API ya borró días que no tenemos);
- valores imposibles: negativos, retención fuera de [0, 1], peak CCU > jugadores únicos,
  minutos por jugador mayores que el bucket, visitas que bajan;
- duplicados en hechos y en el log de dimensiones; nulls; snapshots sin juego en la dimensión.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import yaml

from collectors.common import (
    ROOT,
    TS,
    frame,
    iso_z,
    new_run_id,
    resolve_data_dir,
    setup_logging,
    utcnow,
    write_partition,
)
from pipeline.db import connect

log = logging.getLogger(__name__)

QUALITY_PATH = ROOT / "config" / "quality.yaml"
PLATFORMS = ("roblox", "fortnite")
OK, WARN, ERROR, SKIP = "ok", "warn", "error", "skip"
ICONS = {OK: "✅", WARN: "⚠️", ERROR: "❌", SKIP: "⏭️"}

QUALITY_SCHEMA: dict[str, pl.DataType] = {
    "checked_at": TS,
    "check": pl.Utf8,
    "status": pl.Utf8,
    "value": pl.Float64,
    "unit": pl.Utf8,
    "warn_above": pl.Float64,
    "error_above": pl.Float64,
    "detail": pl.Utf8,
}


@dataclass
class Check:
    name: str
    status: str
    value: float | None = None
    unit: str = ""
    warn: float | None = None
    error: float | None = None
    detail: str = ""

    def fmt(self, v: float | None) -> str:
        if v is None:
            return "—"
        if self.unit == "%":
            return f"{v:.2%}"
        if self.unit == "h":
            return f"{v:.1f} h"
        return f"{v:g}"


def grade(name: str, value: float, limits: dict[str, float], unit: str = "",
          detail: str = "") -> Check:
    """ok / warn / error según si `value` supera los umbrales."""
    warn, error = float(limits["warn"]), float(limits["error"])
    status = ERROR if value > error else WARN if value > warn else OK
    return Check(name, status, float(value), unit, warn, error, detail)


def must_be_zero(name: str, value: int, detail: str = "") -> Check:
    return grade(name, value, {"warn": 0, "error": 0}, "", detail)


def load_rules(path: Path = QUALITY_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


# --------------------------------------------------------------------------- helpers SQL


def _has(con: duckdb.DuckDBPyConnection, view: str) -> bool:
    return bool(con.execute(
        "select count(*) from duckdb_views() where view_name = ?", [view]).fetchall()[0][0])


def _one(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> Any:
    return con.execute(sql, params or []).fetchall()[0][0]


def _since(ts: datetime) -> list[Any]:
    """Parámetros (fecha de partición, epoch) para `date >= ?::date and snapshot_at >= ...`."""
    return [ts.date().isoformat(), ts.timestamp()]


WINDOW = "date >= ?::date and snapshot_at >= to_timestamp(?)"


def _pl(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None
        ) -> pl.DataFrame:
    df = con.sql(sql, params=params or None).pl()
    # DuckDB entrega la zona de la sesión (p. ej. "Etc/UTC"); normalizar a "UTC"
    return df.with_columns(pl.col(pl.Datetime("us", "*")).dt.convert_time_zone("UTC"))


def _ts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[datetime]:
    """Ejecuta una consulta de una columna timestamp y la devuelve como datetimes UTC."""
    df = _pl(con, sql, params)
    return df.to_series(0).to_list() if df.height else []


def run_starts(con: duckdb.DuckDBPyConnection, view: str, since: datetime,
               gap_minutes: int, where: str = "true") -> list[datetime]:
    """Inicio de cada corrida: los snapshot_at se agrupan si distan menos de `gap_minutes`
    (los colectores sellan cada lote con su hora real, no una por corrida)."""
    return _ts(con, f"""
        with t as (select distinct snapshot_at from {view} where {WINDOW} and {where}),
        g as (select snapshot_at,
                     snapshot_at - lag(snapshot_at) over (order by snapshot_at) as d from t)
        select snapshot_at from g where d is null or d > to_minutes(?) order by 1
    """, [*_since(since), gap_minutes])


def _hours(td: timedelta) -> float:
    return td.total_seconds() / 3600


def max_gap_hours(starts: list[datetime], now: datetime, hours: int = 24) -> tuple[float, int]:
    """Hueco máximo entre inicios de corrida en las últimas `hours` (incluye ahora − última).
    Devuelve (hueco, corridas en la ventana)."""
    window = now - timedelta(hours=hours)
    recent = [s for s in starts if s >= window]
    before = [s for s in starts if s < window]
    if not recent:
        return (_hours(now - before[-1]) if before else float(hours)), 0
    points = ([before[-1]] if before else []) + recent + [now]
    gaps = [_hours(b - a) for a, b in pairwise(points)]
    return max(gaps), len(recent)


# --------------------------------------------------------------------------- Roblox


def check_roblox(con: duckdb.DuckDBPyConnection, now: datetime, rules: dict[str, Any]
                 ) -> list[Check]:
    r = rules["roblox"]
    if not _has(con, "roblox_snapshots"):
        return [Check("datos", ERROR, detail="no hay snapshots de Roblox")]
    since = now - timedelta(days=int(rules["lookback_days"]))
    checks: list[Check] = []

    # continuidad de las corridas horarias
    starts = run_starts(con, "roblox_snapshots", now - timedelta(hours=48),
                        int(r["run_gap_minutes"]))
    gap, n_runs = max_gap_hours(starts, now)
    last = iso_z(starts[-1]) if starts else "—"
    checks.append(grade("max_gap_hours", gap, r["max_gap_hours"], "h",
                        f"{n_runs} corridas en 24 h · última {last}"))

    # juegos atrasados respecto de la cadencia de su tier
    factor = float(r["overdue_factor"])
    thr = {t: h * factor for t, h in r["cadence_hours"].items()}
    lookback = timedelta(hours=max(thr.values()) + 24)
    last_obs = _pl(con, f"""
        with last as (select universe_id, max(snapshot_at) as last_at from roblox_snapshots
                      where {WINDOW} group by 1)
        select coalesce(g.tier, 'warm') as tier,
               coalesce(l.last_at, g.first_seen_at) as last_at
        from roblox_games g left join last l using (universe_id)
    """, _since(now - lookback))
    for tier, hours in thr.items():
        sub = last_obs.filter(pl.col("tier") == tier)
        n = sub.height
        late = sub.filter(pl.col("last_at").is_null()
                          | (pl.col("last_at") < now - timedelta(hours=hours))).height
        if n == 0:
            checks.append(Check(f"overdue_{tier}", SKIP, detail="sin juegos en este tier"))
        else:
            checks.append(grade(f"overdue_{tier}", late / n, r["overdue_share"], "%",
                                f"{late}/{n} sin snapshot en {hours:g} h"))

    # valores
    row = con.execute(f"""
        select count(*),
               count(*) - count(distinct (universe_id, snapshot_at)),
               count(*) filter (where playing < 0 or visits < 0
                                or up_votes < 0 or down_votes < 0),
               count(*) filter (where playing is null or visits is null),
               count(distinct universe_id),
               count(distinct universe_id) filter (where favorites < 0)
        from roblox_snapshots where {WINDOW}
    """, _since(since)).fetchall()[0]
    n, dups, negative, nulls, n_games, neg_fav = row
    checks.append(must_be_zero("snapshot_duplicates", dups, f"de {n} filas"))
    checks.append(must_be_zero("negative_values", negative, "playing/visits/votos"))
    # La API devuelve favoritedCount negativo en algunos juegos (visto 2026-11-10: -1586 en
    # un juego con 387k visitas). Se guarda tal cual; se avisa si crece.
    checks.append(grade("negative_favorites_share", neg_fav / n_games if n_games else 0.0,
                        r["negative_favorites_share"], "%",
                        f"{neg_fav} de {n_games} juegos (valor de la API, se guarda tal cual)"))
    checks.append(grade("null_share", nulls / n if n else 0.0, r["null_share"], "%",
                        "playing o visits nulos"))

    dec, pairs = con.execute(f"""
        with s as (select visits, lag(visits) over (partition by universe_id
                                                    order by snapshot_at) as prev
                   from roblox_snapshots where {WINDOW})
        select count(*) filter (where visits < prev), count(prev) from s
    """, _since(since)).fetchall()[0]
    checks.append(grade("visits_decrease_share", dec / pairs if pairs else 0.0,
                        r["visits_decrease_share"], "%", f"{dec} de {pairs} pares consecutivos"))

    orphans = _one(con, f"""
        select count(distinct universe_id) from roblox_snapshots s
        where {WINDOW} and not exists (select 1 from roblox_games g
                                       where g.universe_id = s.universe_id)
    """, _since(since))
    checks.append(must_be_zero("orphan_snapshots", orphans, "juegos sin fila en la dimensión"))
    checks.append(_dim_duplicates(con, "roblox", "universe_id"))

    # barrido diario de búsqueda
    if _has(con, "roblox_search_positions"):
        last_search = _ts(con, f"select max(snapshot_at) from roblox_search_positions "
                               f"where {WINDOW}", _since(now - timedelta(days=7)))
        age = _hours(now - last_search[0]) if last_search and last_search[0] else 24 * 7.0
        checks.append(grade("search_age_hours", age, r["search_age_hours"], "h",
                            "desde el último barrido de keywords"))
    else:
        checks.append(Check("search_age_hours", SKIP, detail="sin barridos todavía"))

    # barrido diario de ícono, miniaturas y gamepasses
    if _has(con, "roblox_asset_checks"):
        last_assets = _ts(con, f"select max(snapshot_at) from roblox_asset_checks "
                               f"where {WINDOW}", _since(now - timedelta(days=7)))
        age = _hours(now - last_assets[0]) if last_assets and last_assets[0] else 24 * 7.0
        checks.append(grade("assets_age_hours", age, r["assets_age_hours"], "h",
                            "desde el último barrido de ícono/miniaturas/gamepasses"))
    else:
        checks.append(Check("assets_age_hours", SKIP, detail="sin barridos todavía"))
    return checks


# --------------------------------------------------------------------------- Fortnite


def check_fortnite(con: duckdb.DuckDBPyConnection, now: datetime, rules: dict[str, Any]
                   ) -> list[Check]:
    r = rules["fortnite"]
    if not _has(con, "fortnite_fetch_log") or not _has(con, "fortnite_games"):
        return [Check("datos", ERROR, detail="no hay fetch_log o dimensión de Fortnite")]
    since = now - timedelta(days=int(rules["lookback_days"]))
    checks: list[Check] = []

    # última corrida
    starts = run_starts(con, "fortnite_fetch_log", now - timedelta(days=3),
                        int(r["run_gap_minutes"]), "interval = 'day'")
    if not starts:
        checks.append(Check("last_run_age_hours", ERROR, 72.0, "h",
                            r["last_run_age_hours"]["warn"], r["last_run_age_hours"]["error"],
                            "sin corridas en 3 días"))
        return checks
    last_start = starts[-1]
    checks.append(grade("last_run_age_hours", _hours(now - last_start), r["last_run_age_hours"],
                        "h", f"inicio {iso_z(last_start)}"))

    fetched = _pl(con, f"""
        select island_code, status from fortnite_fetch_log
        where {WINDOW} and interval = 'day'
    """, _since(last_start))
    status = Counter(fetched["status"].to_list())
    errors = status.get("error", 0)
    checks.append(grade("fetch_error_share", errors / fetched.height, r["fetch_error_share"],
                        "%", " · ".join(f"{k} {v}" for k, v in sorted(status.items()))))

    # islas atrasadas respecto de la cadencia de su tier (hot 1 día, warm 5, cold 7): pasado
    # `overdue_days` sin consulta, la API ya borró días que no tenemos
    first = _ts(con, "select min(snapshot_at) from fortnite_fetch_log", [])
    span = _hours(now - first[0]) / 24 if first and first[0] else 0.0
    for tier, days in r["overdue_days"].items():
        days = float(days)
        if span < days:
            checks.append(Check(f"overdue_{tier}", SKIP,
                                detail=f"historia de {span:.1f} días; necesita {days:g}"))
            continue
        cutoff = now - timedelta(days=days)
        n, late = con.execute(f"""
            with seen as (select distinct island_code from fortnite_fetch_log
                          where {WINDOW} and interval = 'day' and status != 'error')
            select count(*), count(*) filter (where s.island_code is null)
            from fortnite_games g left join seen s using (island_code)
            where g.is_listed and coalesce(g.tier, 'warm') = ?
              and g.first_seen_at < to_timestamp(?)
        """, [*_since(cutoff), tier, cutoff.timestamp()]).fetchall()[0]
        if n == 0:
            checks.append(Check(f"overdue_{tier}", SKIP, detail="sin islas en este tier"))
        else:
            checks.append(grade(f"overdue_{tier}", late / n, r["overdue_share"], "%",
                                f"{late}/{n} sin consulta en {days:g} días"))

    # valores
    if _has(con, "fortnite_snapshots"):
        n, dups, negative, retention, peak, minutes = con.execute(f"""
            select count(*),
                   count(*) - count(distinct (island_code, interval, bucket_start, snapshot_at)),
                   count(*) filter (where plays < 0 or unique_players < 0 or peak_ccu < 0
                                    or minutes_played < 0 or favorites < 0
                                    or recommendations < 0 or avg_minutes_per_player < 0),
                   count(*) filter (where retention_d1 not between 0 and 1
                                    or retention_d7 not between 0 and 1),
                   count(*) filter (where peak_ccu > unique_players),
                   count(*) filter (where avg_minutes_per_player >
                                    case interval when 'hour' then 60 else 1440 end)
            from fortnite_snapshots where {WINDOW}
        """, _since(since)).fetchall()[0]
        checks.append(must_be_zero("snapshot_duplicates", dups, f"de {n} filas"))
        checks.append(must_be_zero("negative_values", negative, "métricas negativas"))
        checks.append(must_be_zero("retention_out_of_range", retention, "fuera de [0, 1]"))
        checks.append(must_be_zero("peak_above_unique", peak, "peak_ccu > unique_players"))
        checks.append(must_be_zero("minutes_above_bucket", minutes,
                                   "minutos por jugador > duración del bucket"))
    checks.append(_dim_duplicates(con, "fortnite", "island_code"))
    return checks


def _dim_duplicates(con: duckdb.DuckDBPyConnection, platform: str, key: str) -> Check:
    dups = _one(con, f"select count(*) - count(distinct ({key}, valid_from)) "
                     f"from {platform}_games_history")
    return must_be_zero("dim_duplicates", dups, f"({key}, valid_from) repetidos")


# --------------------------------------------------------------------------- corrida


def to_frame(checks: list[Check], now: datetime) -> pl.DataFrame:
    return frame([{"checked_at": now, "check": c.name, "status": c.status, "value": c.value,
                   "unit": c.unit, "warn_above": c.warn, "error_above": c.error,
                   "detail": c.detail} for c in checks], QUALITY_SCHEMA)


def render(results: dict[str, list[Check]], now: datetime) -> str:
    lines = [f"## Calidad de datos — {iso_z(now)}", ""]
    for platform, checks in results.items():
        counts = Counter(c.status for c in checks)
        resumen = " · ".join(f"{ICONS[s]} {counts[s]}" for s in (OK, WARN, ERROR, SKIP)
                             if counts[s])
        lines += [f"### {platform} — {resumen}", "",
                  "| check | estado | valor | warn > | error > | detalle |",
                  "|---|:---:|---:|---:|---:|---|"]
        lines += [f"| {c.name} | {ICONS[c.status]} | {c.fmt(c.value)} | {c.fmt(c.warn)} "
                  f"| {c.fmt(c.error)} | {c.detail} |" for c in checks]
        lines.append("")
    return "\n".join(lines)


def run(data_dir: Path, platforms: tuple[str, ...] = PLATFORMS, now: datetime | None = None,
        write: bool = True, rules: dict[str, Any] | None = None
        ) -> dict[str, list[Check]]:
    now = now or utcnow()
    rules = rules or load_rules()
    con = connect(data_dir)
    results: dict[str, list[Check]] = {}
    for platform in platforms:
        if platform == "roblox":
            results[platform] = check_roblox(con, now, rules)
        else:
            results[platform] = check_fortnite(con, now, rules)
    if write:
        run_id = new_run_id()
        for platform, checks in results.items():
            write_partition(to_frame(checks, now), data_dir, platform, "quality", now, run_id)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Checks de calidad de datos")
    parser.add_argument("--platform", choices=[*PLATFORMS, "all"], default="all")
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--no-write", action="store_true", help="no guardar el resultado")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    setup_logging(args.verbose)

    platforms = PLATFORMS if args.platform == "all" else (args.platform,)
    data_dir = resolve_data_dir(args.data_dir)
    now = utcnow()
    results = run(data_dir, platforms, now, write=not args.no_write)
    text = render(results, now)
    print(text)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    failed = [f"{p}.{c.name}" for p, cs in results.items() for c in cs if c.status == ERROR]
    if failed:
        log.error("checks con error: %s", ", ".join(failed))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
