"""Score de oportunidad por nicho (género unificado): demanda vs. competencia.

Las dos plataformas miden la demanda distinto, así que los scores no se comparan entre sí:
Fortnite con jugadores únicos (la API trae 7 días por consulta), Roblox con visitas ganadas
(contador acumulado que se interpola entre snapshots propios).

## Fortnite

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

## Roblox

Roblox no da historia ni jugadores únicos, y el CCU depende de a qué hora se muestrea (los
juegos cold tienen una foto por semana). La demanda son las **visitas ganadas**: `visits` es
acumulado, así que entre dos snapshots se reparte la diferencia en forma lineal y se suma la
parte que cae en cada ventana, sin importar la frecuencia de muestreo. Un juego creado hace
menos de 14 días tiene además el tramo creación (0 visitas) → primer snapshot.

Por juego (`roblox_game_demand`), con ventanas relativas a su último snapshot (`cov_end`):
- `visits_w1`: visitas en los últimos 7 días. Con menos de 7 días cubiertos (al menos 18 h)
  se extrapola a la semana (`w1_coverage_days` dice cuánto hubo).
- `visits_w0`: visitas en los 7 días anteriores; solo si las dos semanas están completas.
- `peak_ccu_w1`: CCU máximo visto en la última semana.

Por nicho (`roblox_niche_opportunity`): `games_tracked`, `games_active`, `visits_w1`,
`visits_growth`, `visits_per_active_game`, `median_game_visits`, `top10_share`,
`opportunity_score` (visitas ganadas en la semana / juegos activos) y dos señales de entrada
que Fortnite no tiene (no expone fecha de creación): `new_games_30d` (juegos creados en los
últimos 30 días) y `new_games_visits_share` (qué parte de la demanda se llevan).

Sesgo: solo están los juegos que aparecieron en charts o búsquedas, así que la competencia
chica está subcontada. Sirve para comparar nichos entre sí, no como censo.

`python -m pipeline.opportunity [--platform fortnite|roblox]` imprime las tablas.
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

# Cobertura mínima de la última semana para extrapolar (días) y para dar una semana por
# completa al comparar semana contra semana.
MIN_W1_DAYS = 0.75
FULL_WEEK_DAYS = 6.5
NEW_GAME_DAYS = 30

_GAME_DEMAND = """
create or replace view roblox_game_demand as
with s as (
    select universe_id, snapshot_at as t, max(visits)::double as v, false as origin
    from roblox_snapshots where visits > 0
    group by all
    union all
    select universe_id, created_at, 0, true from roblox_games where created_at is not null
), p as (
    select universe_id, lag(t) over w as t0, t as t1,
           greatest(v - lag(v) over w, 0) as dv,
           lag(origin) over w as from_origin
    from s window w as (partition by universe_id order by t)
), p2 as (
    -- tramos de hasta 14 días: el de creación → primer snapshot solo para juegos nuevos
    select * from p
    where t0 is not null and t1 > t0 and t1 - t0 <= interval 14 day
), b as (
    select universe_id, max(t1) as cov_end from p2 group by 1
), w as (
    select p2.universe_id, b.cov_end, k.wk, p2.dv, p2.from_origin, p2.t0,
           epoch(p2.t1) - epoch(p2.t0) as dur_s,
           epoch(least(p2.t1, b.cov_end - to_days(7 * k.wk)))
             - epoch(greatest(p2.t0, b.cov_end - to_days(7 * (k.wk + 1)))) as ov_s
    from p2 join b using (universe_id)
    cross join (values (0), (1)) as k(wk)   -- 0 = última semana, 1 = la anterior
), agg as (
    select universe_id, cov_end,
           -- antes de la creación las visitas son 0 seguro: ese tramo cuenta como cubierto
           (coalesce(sum(ov_s) filter (wk = 0), 0)
            + coalesce(greatest(epoch(min(t0) filter (from_origin))
                                - epoch(cov_end - interval 7 day), 0), 0)) / 86400 as w1_days,
           -- semanas completas para comparar: solo tramos entre snapshots reales (el de
           -- creación → primer snapshot es una recta supuesta, no sirve para crecimiento)
           coalesce(sum(ov_s) filter (wk = 0 and not from_origin), 0) / 86400 as w1_real,
           coalesce(sum(ov_s) filter (wk = 1 and not from_origin), 0) / 86400 as w0_real,
           coalesce(sum(dv * ov_s / dur_s) filter (wk = 0), 0) as v1,
           coalesce(sum(dv * ov_s / dur_s) filter (wk = 1), 0) as v0
    from w where ov_s > 0
    group by universe_id, cov_end
)
select a.universe_id, a.cov_end, round(a.w1_days, 2) as w1_coverage_days,
       case when a.w1_days >= @MIN_W1@ then a.v1 / least(a.w1_days, 7) * 7 end as visits_w1,
       case when a.w1_real >= @FULL@ and a.w0_real >= @FULL@ then a.v0 end as visits_w0,
       (select max(playing) from roblox_snapshots r
        where r.universe_id = a.universe_id
          and r.snapshot_at > a.cov_end - interval 7 day) as peak_ccu_w1
from agg a"""

_ROBLOX_NICHE = """
create or replace view roblox_niche_opportunity as
with ref as (select max(snapshot_at) as t from roblox_snapshots),
g as (
    select f.genre_unified, gm.created_at, d.visits_w1, d.visits_w0,
           gm.created_at >= (select t from ref) - to_days(@NEW_DAYS@) as is_new
    from roblox_games gm
    join roblox_game_features f using (universe_id)
    left join roblox_game_demand d using (universe_id)
), r as (
    select *, row_number() over (partition by genre_unified
                                 order by visits_w1 desc nulls last) as rk
    from g
), agg as (
    select genre_unified,
           count(*) as games_tracked,
           count(*) filter (visits_w1 > 0) as games_active,
           coalesce(sum(visits_w1), 0) as visits_w1,
           sum(visits_w1) filter (visits_w0 is not null) as visits_w1_comparable,
           sum(visits_w0) as visits_w0,
           median(visits_w1) filter (visits_w1 > 0) as median_game_visits,
           sum(visits_w1) filter (rk <= 10) as top10_visits,
           count(*) filter (is_new) as new_games_30d,
           coalesce(sum(visits_w1) filter (is_new), 0) as new_visits
    from r group by genre_unified
)
select genre_unified, games_tracked, games_active, visits_w1,
       visits_w1_comparable / nullif(visits_w0, 0) - 1 as visits_growth,
       visits_w1 / nullif(games_active, 0) as visits_per_active_game,
       median_game_visits,
       top10_visits / nullif(visits_w1, 0) as top10_share,
       new_games_30d,
       new_visits / nullif(visits_w1, 0) as new_games_visits_share,
       (visits_w1_comparable - visits_w0) / nullif(games_active, 0) as opportunity_score
from agg"""


def _has_view(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return bool(con.execute(
        "select count(*) from duckdb_views() where view_name = ?", [name]).fetchall()[0][0])


def register(con: duckdb.DuckDBPyConnection) -> list[str]:
    created = []
    if all(_has_view(con, v) for v in ("fortnite_fetch_log", "fortnite_daily",
                                        "fortnite_games", "fortnite_game_features")):
        excluded = ", ".join("'" + c.replace("'", "''") + "'" for c in EXCLUDED_CREATORS)
        con.execute(_ISLAND_DEMAND)
        con.execute(_NICHE.replace("@EXCLUDED@", excluded))
        created += ["fortnite_island_demand", "fortnite_niche_opportunity"]
    if all(_has_view(con, v) for v in ("roblox_snapshots", "roblox_games",
                                        "roblox_game_features")):
        con.execute(_GAME_DEMAND.replace("@MIN_W1@", str(MIN_W1_DAYS))
                    .replace("@FULL@", str(FULL_WEEK_DAYS)))
        con.execute(_ROBLOX_NICHE.replace("@NEW_DAYS@", str(NEW_GAME_DAYS)))
        created += ["roblox_game_demand", "roblox_niche_opportunity"]
    return created


def _num(v, fmt="{:,.0f}"):
    return "—" if v is None else fmt.format(v)


def render(con: duckdb.DuckDBPyConnection, platform: str = "fortnite") -> str:
    if platform == "roblox":
        return _render_roblox(con)
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
    for g, listed, active, dem, growth, per, med, top10, score in rows:
        lines.append(
            f"| {g} | {_num(listed)} | {_num(active)} | {_num(dem)} "
            f"| {_num(growth, '{:+.0%}')} | {_num(per)} | {_num(med)} "
            f"| {_num(top10, '{:.0%}')} | {_num(score, '{:+,.1f}')} |")
    return "\n".join(lines)


def _render_roblox(con: duckdb.DuckDBPyConnection) -> str:
    rows = con.sql("""
        select * from roblox_niche_opportunity
        order by opportunity_score desc nulls last, median_game_visits desc nulls last
    """).fetchall()
    growth_ready = any(r[10] is not None for r in rows)
    lines = [
        "## Roblox — oportunidad por nicho",
        "",
        "Demanda = visitas ganadas en la última semana de cada juego (interpoladas entre "
        "snapshots). Solo juegos vistos en charts o búsquedas."
        + ("" if growth_ready else
           " **Todavía no hay dos semanas de datos: sin crecimiento ni score, ordenado por las "
           "visitas del juego típico.**"),
        "",
        "| nicho | juegos | activos | visitas 7d | crec. sem. | visitas/activo | mediana | "
        "top 10 | nuevos 30d | % visitas nuevos | score |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for g, tracked, active, vis, growth, per, med, top10, new, new_share, score in rows:
        lines.append(
            f"| {g} | {_num(tracked)} | {_num(active)} | {_num(vis)} "
            f"| {_num(growth, '{:+.0%}')} | {_num(per)} | {_num(med)} "
            f"| {_num(top10, '{:.0%}')} | {_num(new)} | {_num(new_share, '{:.0%}')} "
            f"| {_num(score, '{:+,.0f}')} |")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    from pipeline.db import connect

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--platform", choices=["fortnite", "roblox"], action="append",
                        help="por defecto, las dos")
    args = parser.parse_args(argv)
    con = connect(args.data_dir)
    shown = 0
    for platform in args.platform or ["fortnite", "roblox"]:
        if not _has_view(con, f"{platform}_niche_opportunity"):
            print(f"faltan datos de {platform} (snapshots, dimensión o fetch_log)\n")
            continue
        print(render(con, platform) + "\n")
        shown += 1
    return 0 if shown else 1


if __name__ == "__main__":
    raise SystemExit(main())
