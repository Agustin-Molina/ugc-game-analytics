"""Métricas derivadas de la serie temporal de cada juego (vistas DuckDB, calculadas al leer).

Series diarias:
- `roblox_daily`    universe_id, day, peak_ccu (máx. de `playing` en el día), avg_ccu,
                    n_snapshots, visits, favorites, up_votes, down_votes (máx. del día),
                    is_partial (el día UTC de hoy, todavía abierto)
- `fortnite_daily`  island_code, day y las métricas diarias de la API (peak_ccu,
                    unique_players, plays, favorites, recommendations, retention_d1/d7, …),
                    solo buckets cerrados

Métricas por juego (`<plataforma>_game_metrics`), sobre los días cerrados:
- `first_day`, `last_day`, `obs_days`, `peak_ccu`, `peak_day`, `median_daily_peak`,
  `peak_over_median` (picos aislados vs. tracción pareja)
- `anchor_day`: día de la primera tracción (`first_traction_at`, CCU ≥ umbral);
  `traction_censored`: ya tenía tracción cuando empezamos a mirarlo (no sabemos cuándo
  despegó). Para estudiar lanzamientos, filtrar `not traction_censored`.
- `days_to_peak` (desde anchor_day), `days_since_peak`
- `slope_d1_7`, `slope_d1_14`: pendiente de ln(1 + peak diario) por día en los días 0–6 y
  0–13 desde anchor_day (≥ 3 y ≥ 5 días con datos; 0,1 ≈ +10 % por día)
- `half_life_days`: días desde el peak hasta el primer día con peak ≤ 50 % del máximo
  (null si todavía no cayó a la mitad)
- `sustained_ratio`: peak diario promedio de los días 30–60 desde anchor_day / peak_ccu
  (la definición de "sostenido" de la Fase 3)
- Roblox: `favorites_per_visit`, `like_ratio` (up / (up + down)), `visits_last`,
  `visits_per_day_7d` (pendiente de visitas en los últimos 7 días observados)
- Fortnite: `favorites_per_player`, `recommendations_per_player` (sumas / jugadores únicos),
  `retention_d1_mean`, `retention_d7_mean`, `minutes_per_player_mean`

Ojo con la cadencia: los juegos cold de Roblox se miran una vez por semana y las islas warm o
cold de Fortnite cada 5–7 días (con 7 días de historia por consulta), así que su serie diaria
puede tener huecos y su peak horario es una cota inferior.
"""

from __future__ import annotations

import duckdb

UTC_DAY = "({col} at time zone 'UTC')::date"

_ROBLOX_DAILY = f"""
create or replace view roblox_daily as
select *, day = (now() at time zone 'UTC')::date as is_partial
from (
    select universe_id, {UTC_DAY.format(col="snapshot_at")} as day,
           max(playing) as peak_ccu, avg(playing) as avg_ccu, count(*) as n_snapshots,
           max(visits) as visits, max(favorites) as favorites,
           max(up_votes) as up_votes, max(down_votes) as down_votes
    from roblox_snapshots group by all
)"""

_FORTNITE_DAILY = f"""
create or replace view fortnite_daily as
select island_code, {UTC_DAY.format(col="bucket_start")} as day, peak_ccu, unique_players,
       plays, favorites, recommendations, retention_d1, retention_d7,
       avg_minutes_per_player, minutes_played
from fortnite_snapshots_latest
where interval = 'day' and not is_partial"""

# anchor: K, anchor_day, traction_censored
_ROBLOX_ANCHOR = f"""
select g.universe_id, {UTC_DAY.format(col="g.first_traction_at")} as anchor_day,
       {UTC_DAY.format(col="g.first_traction_at")} <= f.first_obs
       and coalesce({UTC_DAY.format(col="g.created_at")} < f.first_obs, true)
           as traction_censored
from roblox_games g
join (select universe_id, min(day) as first_obs from roblox_daily group by 1) f
    using (universe_id)
where g.first_traction_at is not null"""

_FORTNITE_ANCHOR = f"""
select g.island_code, {UTC_DAY.format(col="g.first_traction_at")} as anchor_day,
       {UTC_DAY.format(col="g.first_traction_at")} <= f.first_day
       and {UTC_DAY.format(col="g.first_seen_at")} <= c.catalog_start as traction_censored
from fortnite_games g
join (select island_code, min(day) as first_day from fortnite_daily group by 1) f
    using (island_code)
cross join (select min({UTC_DAY.format(col="first_seen_at")}) as catalog_start
            from fortnite_games) c
where g.first_traction_at is not null"""

_ROBLOX_EXTRA = """
select universe_id,
       arg_max(favorites, day) / nullif(arg_max(visits, day), 0) as favorites_per_visit,
       arg_max(up_votes, day)
           / nullif(arg_max(up_votes, day) + arg_max(down_votes, day), 0) as like_ratio,
       arg_max(visits, day) as visits_last
from roblox_daily group by universe_id"""

_ROBLOX_EXTRA_7D = """
select d.universe_id,
       case when count(*) >= 2 then regr_slope(d.visits, epoch(d.day) / 86400) end
           as visits_per_day_7d
from roblox_daily d
join (select universe_id, max(day) as last_day from roblox_daily group by 1) l
    using (universe_id)
where d.day > l.last_day - 7 and d.visits > 0   -- la API a veces devuelve 0 visitas
group by d.universe_id"""

_FORTNITE_EXTRA = """
select island_code,
       sum(favorites) / nullif(sum(unique_players), 0) as favorites_per_player,
       sum(recommendations) / nullif(sum(unique_players), 0) as recommendations_per_player,
       avg(retention_d1) as retention_d1_mean,
       avg(retention_d7) as retention_d7_mean,
       avg(avg_minutes_per_player) as minutes_per_player_mean
from fortnite_daily group by island_code"""


def metrics_sql(platform: str, key: str, daily_filter: str, anchor: str,
                extras: list[str]) -> str:
    extra_ctes = "".join(f",\nx{i} as ({sql})" for i, sql in enumerate(extras))
    extra_cols = "".join(f", x{i}.* exclude ({key})" for i in range(len(extras)))
    extra_joins = "".join(f"\nleft join x{i} using ({key})" for i in range(len(extras)))
    return f"""
create or replace view {platform}_game_metrics as
with d as (
    select {key}, day, peak_ccu from {platform}_daily
    where peak_ccu is not null {daily_filter}
), s as (
    select {key}, min(day) as first_day, max(day) as last_day, count(*) as obs_days,
           max(peak_ccu) as peak_ccu, median(peak_ccu) as median_daily_peak
    from d group by {key}
), pd as (   -- primer día con el máximo
    select d.{key}, min(d.day) as peak_day
    from d join s using ({key}) where d.peak_ccu = s.peak_ccu group by d.{key}
), a as ({anchor}
), rel as (
    select d.{key}, d.peak_ccu, d.day - a.anchor_day as t
    from d join a using ({key})
), sl as (
    select {key},
           case when count(*) filter (t between 0 and 6) >= 3 then
               regr_slope(ln(1 + peak_ccu), t) filter (t between 0 and 6) end as slope_d1_7,
           case when count(*) filter (t between 0 and 13) >= 5 then
               regr_slope(ln(1 + peak_ccu), t) filter (t between 0 and 13) end as slope_d1_14,
           avg(peak_ccu) filter (t between 30 and 60) as avg_peak_d30_60
    from rel group by {key}
), hl as (
    select d.{key}, min(d.day) - any_value(pd.peak_day) as half_life_days
    from d join pd using ({key}) join s using ({key})
    where d.day > pd.peak_day and d.peak_ccu <= s.peak_ccu / 2.0
    group by d.{key}
){extra_ctes}
select s.{key}, s.first_day, s.last_day, s.obs_days, s.peak_ccu, pd.peak_day,
       s.median_daily_peak, s.peak_ccu / nullif(s.median_daily_peak, 0) as peak_over_median,
       a.anchor_day, a.traction_censored,
       pd.peak_day - a.anchor_day as days_to_peak,
       s.last_day - pd.peak_day as days_since_peak,
       sl.slope_d1_7, sl.slope_d1_14, hl.half_life_days,
       sl.avg_peak_d30_60 / nullif(s.peak_ccu, 0) as sustained_ratio{extra_cols}
from s join pd using ({key})
left join a using ({key})
left join sl using ({key})
left join hl using ({key}){extra_joins}"""


def _has_view(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return bool(con.execute(
        "select count(*) from duckdb_views() where view_name = ?", [name]).fetchall()[0][0])


def _columns(con: duckdb.DuckDBPyConnection, view: str) -> set[str]:
    return {r[0] for r in con.sql(f"describe {view}").fetchall()}


def register(con: duckdb.DuckDBPyConnection) -> list[str]:
    created: list[str] = []
    if (_has_view(con, "roblox_snapshots") and _has_view(con, "roblox_games")
            and {"first_traction_at", "created_at"} <= _columns(con, "roblox_games")):
        con.execute(_ROBLOX_DAILY)
        con.execute(metrics_sql("roblox", "universe_id", "and not is_partial", _ROBLOX_ANCHOR,
                                [_ROBLOX_EXTRA, _ROBLOX_EXTRA_7D]))
        created += ["roblox_daily", "roblox_game_metrics"]
    if (_has_view(con, "fortnite_snapshots_latest") and _has_view(con, "fortnite_games")
            and {"first_traction_at", "first_seen_at"} <= _columns(con, "fortnite_games")):
        con.execute(_FORTNITE_DAILY)
        con.execute(metrics_sql("fortnite", "island_code", "", _FORTNITE_ANCHOR,
                                [_FORTNITE_EXTRA]))
        created += ["fortnite_daily", "fortnite_game_metrics"]
    return created
