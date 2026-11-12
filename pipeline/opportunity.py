"""Score de oportunidad por nicho (género unificado): demanda vs. competencia.

Por ahora Fortnite, que es la plataforma con historia diaria completa (la API trae 7 días por
consulta). Roblox se suma cuando haya semanas de snapshots propios.

Demanda por isla (`fortnite_island_demand`): cada isla tiene su propia ventana de cobertura,
la de sus consultas (`fetch_log`): desde 7 días antes de la primera hasta el día anterior a
la última. Dentro de la ventana, un día sin datos es un día con < 5 jugadores (la API no
publica menos), así que cuenta como 0.
- `demand_w1`: jugadores únicos sumados en los últimos 7 días cubiertos (semana completa,
  sin sesgo de día de la semana: los fines de semana tienen el doble de jugadores).
- `demand_w0`: lo mismo en los 7 días anteriores; null si la isla todavía no tiene 14 días
  de cobertura.

Por nicho (`fortnite_niche_opportunity`, sin las islas de Epic, que no compiten):
- `islands_listed`, `islands_active` (con demanda en la última semana)
- `demand_w1`, `demand_growth` (semana contra semana, solo islas con las dos semanas)
- `demand_per_active_island`, `median_island_demand` (la isla típica, sin el arrastre de
  los hits), `top10_share` (concentración: cuánto se llevan las 10 islas más jugadas)
- `opportunity_score` = crecimiento de la demanda (jugadores-día ganados en la semana) /
  islas activas compitiendo. Null hasta que haya dos semanas de datos.

`python -m pipeline.opportunity` imprime la tabla.
"""

from __future__ import annotations

import argparse

import duckdb

# creadores que no son competencia UGC (las islas propias de Epic)
EXCLUDED_CREATORS = ("epic",)

_ISLAND_DEMAND = """
create or replace view fortnite_island_demand as
with cov as (
    select island_code,
           min((snapshot_at at time zone 'UTC')::date) - 7 as cov_start,
           max((snapshot_at at time zone 'UTC')::date) - 1 as cov_end
    from fortnite_fetch_log
    where status = 'ok' and interval = 'day'
    group by island_code
)
select c.island_code, c.cov_start, c.cov_end,
       coalesce(sum(f.unique_players)
                filter (f.day between c.cov_end - 6 and c.cov_end), 0) as demand_w1,
       case when c.cov_start <= c.cov_end - 13 then
           coalesce(sum(f.unique_players)
                    filter (f.day between c.cov_end - 13 and c.cov_end - 7), 0)
       end as demand_w0,
       max(f.peak_ccu) filter (f.day between c.cov_end - 6 and c.cov_end) as peak_w1
from cov c left join fortnite_daily f using (island_code)
group by c.island_code, c.cov_start, c.cov_end"""

_NICHE = """
create or replace view fortnite_niche_opportunity as
with i as (
    select f.genre_unified, coalesce(g.is_listed, true) as is_listed,
           d.demand_w1, d.demand_w0
    from fortnite_games g
    join fortnite_game_features f using (island_code)
    left join fortnite_island_demand d using (island_code)
    where coalesce(g.creator_code, '') not in (@EXCLUDED@)
), r as (
    select *, row_number() over (partition by genre_unified
                                 order by demand_w1 desc nulls last) as rk
    from i
), agg as (
    select genre_unified,
           count(*) filter (is_listed) as islands_listed,
           count(*) filter (demand_w1 > 0) as islands_active,
           coalesce(sum(demand_w1), 0) as demand_w1,
           sum(demand_w1) filter (demand_w0 is not null) as demand_w1_comparable,
           sum(demand_w0) as demand_w0,
           median(demand_w1) filter (demand_w1 > 0) as median_island_demand,
           sum(demand_w1) filter (rk <= 10) as top10_demand
    from r group by genre_unified
)
select genre_unified, islands_listed, islands_active, demand_w1,
       demand_w1_comparable / nullif(demand_w0, 0) - 1 as demand_growth,
       demand_w1 / nullif(islands_active, 0) as demand_per_active_island,
       median_island_demand,
       top10_demand / nullif(demand_w1, 0) as top10_share,
       (demand_w1_comparable - demand_w0) / nullif(islands_active, 0) as opportunity_score
from agg"""


def _has_view(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return bool(con.execute(
        "select count(*) from duckdb_views() where view_name = ?", [name]).fetchall()[0][0])


def register(con: duckdb.DuckDBPyConnection) -> list[str]:
    needed = ("fortnite_fetch_log", "fortnite_daily", "fortnite_games",
              "fortnite_game_features")
    if not all(_has_view(con, v) for v in needed):
        return []
    excluded = ", ".join("'" + c.replace("'", "''") + "'" for c in EXCLUDED_CREATORS)
    con.execute(_ISLAND_DEMAND)
    con.execute(_NICHE.replace("@EXCLUDED@", excluded))
    return ["fortnite_island_demand", "fortnite_niche_opportunity"]


def render(con: duckdb.DuckDBPyConnection) -> str:
    rows = con.sql("""
        select * from fortnite_niche_opportunity
        order by opportunity_score desc nulls last, median_island_demand desc nulls last
    """).fetchall()
    growth_ready = any(r[8] is not None for r in rows)
    lines = [
        "## Fortnite — oportunidad por nicho",
        "",
        "Demanda = jugadores únicos sumados en la última semana cubierta de cada isla "
        "(sin islas de Epic)."
        + ("" if growth_ready else
           " **Todavía no hay dos semanas de datos: sin crecimiento ni score, ordenado por la "
           "demanda de la isla típica.**"),
        "",
        "| nicho | islas | activas | demanda 7d | crec. sem. | demanda/activa | mediana | "
        "top 10 | score |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    def num(v, fmt="{:,.0f}"):
        return "—" if v is None else fmt.format(v)

    for g, listed, active, dem, growth, per, med, top10, score in rows:
        lines.append(
            f"| {g} | {num(listed)} | {num(active)} | {num(dem)} | {num(growth, '{:+.0%}')} "
            f"| {num(per)} | {num(med)} | {num(top10, '{:.0%}')} | {num(score, '{:+,.1f}')} |")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    from pipeline.db import connect

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", default=None)
    args = parser.parse_args(argv)
    con = connect(args.data_dir)
    if not _has_view(con, "fortnite_niche_opportunity"):
        print("faltan datos de Fortnite (fetch_log, snapshots o dimensión)")
        return 1
    print(render(con))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
