"""Datos del dashboard: lee el export de Kaggle (un Parquet por tabla, pipeline/export.py).

Usa el export y no el repo de datos crudo para que el dashboard corra igual localmente
(`python -m pipeline.export --out export`) y desplegado, con las features ya calculadas.
Funciones puras (sin Streamlit): reciben una conexión DuckDB y devuelven DataFrames.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PLATFORMS = ("roblox", "fortnite")


def find_data_dir() -> Path | None:
    """$UGC_EXPORT_DIR, o export/ en la raíz del repo."""
    for c in (os.environ.get("UGC_EXPORT_DIR"), ROOT / "export"):
        if c and Path(c, "roblox_games.parquet").exists():
            return Path(c)
    return None


def open_db(data_dir: Path) -> duckdb.DuckDBPyConnection:
    """Una vista por tabla; los archivos mensuales (<tabla>_YYYY-MM.parquet) se unen."""
    con = duckdb.connect()
    tables: dict[str, list[str]] = defaultdict(list)
    for f in sorted(Path(data_dir).glob("*.parquet")):
        tables[re.sub(r"_\d{4}-\d{2}$", "", f.stem)].append(f.as_posix())
    for name, files in tables.items():
        con.execute(f"create view {name} as select * from read_parquet({files})")
    return con


def has_table(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return bool(con.execute("select count(*) from duckdb_views() where view_name = ?",
                            [name]).fetchone()[0])


def _df(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> pd.DataFrame:
    return con.execute(sql, params or []).df()


# ------------------------------------------------------------------ overview

def freshness(con) -> dict:
    # vía pandas: fetchone() de timestamps con zona horaria necesita pytz
    return _df(con, """
        select (select max(snapshot_at) from roblox_snapshots) as roblox_last,
               (select max(day) from fortnite_daily) as fortnite_last_day,
               (select count(*) from roblox_games) as roblox_games,
               (select count(*) from fortnite_islands where is_listed) as fortnite_islands,
               (select min(snapshot_at) from roblox_snapshots) as tracking_since
    """).iloc[0].to_dict()


def top_now(con, platform: str, n: int = 15) -> pd.DataFrame:
    """Juegos más jugados: Roblox en su último snapshot, Fortnite en el último día cerrado."""
    if platform == "roblox":
        return _df(con, """
            with last as (
                select universe_id, arg_max(playing, snapshot_at) as ccu
                from roblox_snapshots
                where snapshot_at >= (select max(snapshot_at) from roblox_snapshots)
                                     - interval 2 hour
                group by 1)
            select g.name, g.genre_unified as genre, l.ccu
            from last l join roblox_games g using (universe_id)
            order by l.ccu desc limit ?""", [n])
    return _df(con, """
        select i.title as name, i.genre_unified as genre, d.peak_ccu as ccu
        from fortnite_daily d join fortnite_islands i using (island_code)
        where d.day = (select max(day) from fortnite_daily)
        order by d.peak_ccu desc limit ?""", [n])


def daily_cycle(con) -> pd.DataFrame:
    """CCU total por hora del día (UTC), indexado a la media diaria de cada plataforma."""
    df = _df(con, """
        with r as (
            select date_trunc('hour', snapshot_at) as t, universe_id, max(playing) as ccu
            from roblox_snapshots
            where universe_id in (select universe_id from roblox_games where tier = 'hot')
            group by all
        ), rh as (select t, sum(ccu) as ccu from r group by 1),
        fh as (
            select bucket_start as t, sum(peak_ccu) as ccu from fortnite_snapshots
            where interval = 'hour' group by 1
        )
        select 'Roblox' as platform, hour(t) as hour, avg(ccu) as ccu from rh group by all
        union all
        select 'Fortnite', hour(t), avg(ccu) from fh group by all
        order by platform, hour""")
    df["index"] = df["ccu"] / df.groupby("platform")["ccu"].transform("mean") * 100
    return df


# ------------------------------------------------------------------ niches

def niches(con, platform: str) -> pd.DataFrame:
    return _df(con, f"select * from {platform}_niche_opportunity")


# ------------------------------------------------------------------ launches

def launches(con, platform: str) -> pd.DataFrame:
    """Lanzamientos observados desde su primera tracción (sin censura a izquierda)."""
    if platform == "roblox":
        return _df(con, """
            select g.universe_id::varchar as id, g.name, g.genre_unified as genre,
                   g.creator_name as creator, g.first_traction_at, m.obs_days, m.peak_ccu,
                   m.days_to_peak, m.slope_d1_7, d.last_peak as last_day_peak,
                   g.created_at
            from roblox_game_metrics m
            join roblox_games g using (universe_id)
            left join (select universe_id, arg_max(peak_ccu, day) as last_peak
                       from roblox_daily group by 1) d using (universe_id)
            where not m.traction_censored and m.anchor_day is not null
            order by m.peak_ccu desc""")
    return _df(con, """
        select i.island_code as id, i.title as name, i.genre_unified as genre,
               i.creator_code as creator, i.first_traction_at, m.obs_days, m.peak_ccu,
               m.days_to_peak, m.slope_d1_7, d.last_peak as last_day_peak,
               m.retention_d1_mean
        from fortnite_island_metrics m
        join fortnite_islands i using (island_code)
        left join (select island_code, arg_max(peak_ccu, day) as last_peak
                   from fortnite_daily group by 1) d using (island_code)
        where not m.traction_censored and m.anchor_day is not null
        order by m.peak_ccu desc""")


def launch_curves(con, platform: str, ids: list[str]) -> pd.DataFrame:
    """CCU desde la primera tracción, en días: Roblox horario, Fortnite pico diario."""
    if not ids:
        return pd.DataFrame(columns=["id", "name", "days", "ccu"])
    if platform == "roblox":
        return _df(con, """
            select g.universe_id::varchar as id, g.name,
                   date_diff('minute', g.first_traction_at, s.snapshot_at) / 1440.0 as days,
                   s.playing as ccu
            from roblox_snapshots s join roblox_games g using (universe_id)
            where g.universe_id::varchar in (select unnest(?))
              and s.snapshot_at >= g.first_traction_at
            order by 1, 3""", [list(ids)])
    return _df(con, """
        select i.island_code as id, i.title as name,
               date_diff('day', (i.first_traction_at at time zone 'UTC')::date, d.day) as days,
               d.peak_ccu as ccu
        from fortnite_daily d join fortnite_islands i using (island_code)
        where i.island_code in (select unnest(?))
          and d.day >= (i.first_traction_at at time zone 'UTC')::date
        order by 1, 3""", [list(ids)])


# ------------------------------------------------------------------ game lookup

def search_games(con, text: str, limit: int = 50) -> pd.DataFrame:
    """Busca por nombre (o código de isla) en las dos plataformas, los más grandes primero."""
    pat = f"%{text.strip()}%"
    return _df(con, """
        select * from (
            select 'roblox' as platform, g.universe_id::varchar as id, g.name, g.tier,
                   g.genre_unified as genre, m.peak_ccu
            from roblox_games g left join roblox_game_metrics m using (universe_id)
            where g.name ilike ? or g.universe_id::varchar = ?
            union all
            select 'fortnite', i.island_code, i.title, i.tier, i.genre_unified, m.peak_ccu
            from fortnite_islands i left join fortnite_island_metrics m using (island_code)
            where i.title ilike ? or i.island_code = ?
        ) order by peak_ccu desc nulls last limit ?""",
        [pat, text.strip(), pat, text.strip(), limit])


_ROBLOX_INFO = """
    select g.name, g.creator_name as creator, g.creator_type, g.genre_unified as genre,
           g.genre_l1, g.genre_l2, g.created_at, g.updated_at, g.first_seen_at,
           g.first_traction_at, g.tier, g.max_players, g.age_rating, g.discovered_via,
           g.title_clean, g.bracket_tags, g.has_update_tag, g.n_emojis,
           g.creator_total_games, g.creator_prev_hits, g.creator_prev_max_visits,
           g.clone_key, g.clone_group_size, g.clone_rank,
           m.peak_ccu, m.peak_day, m.days_to_peak, m.slope_d1_7, m.half_life_days,
           m.traction_censored, m.like_ratio, m.favorites_per_visit, m.visits_last,
           g.description
    from roblox_games g left join roblox_game_metrics m using (universe_id)
    where g.universe_id::varchar = ?"""

_FORTNITE_INFO = """
    select i.title as name, i.creator_code as creator, i.genre_unified as genre, i.tags,
           i.category, i.created_in, i.first_seen_at, i.first_traction_at, i.tier, i.is_listed,
           i.title_clean, i.bracket_tags, i.has_update_tag, i.n_emojis,
           i.creator_other_islands, i.creator_best_peak_other,
           i.clone_key, i.clone_group_size, i.clone_rank,
           m.peak_ccu, m.peak_day, m.days_to_peak, m.slope_d1_7, m.half_life_days,
           m.traction_censored, m.retention_d1_mean, m.retention_d7_mean,
           m.minutes_per_player_mean
    from fortnite_islands i left join fortnite_island_metrics m using (island_code)
    where i.island_code = ?"""


def game_info(con, platform: str, game_id: str) -> dict | None:
    df = _df(con, _ROBLOX_INFO if platform == "roblox" else _FORTNITE_INFO, [game_id])
    return None if df.empty else df.iloc[0].to_dict()


def game_series(con, platform: str, game_id: str) -> pd.DataFrame:
    """Roblox: cada snapshot (CCU, visitas, favoritos). Fortnite: métricas diarias."""
    if platform == "roblox":
        return _df(con, """
            select snapshot_at as t, playing as ccu, visits, favorites
            from roblox_snapshots where universe_id::varchar = ? order by 1""", [game_id])
    return _df(con, """
        select day::timestamp as t, peak_ccu as ccu, unique_players, plays, retention_d1,
               avg_minutes_per_player
        from fortnite_daily where island_code = ? order by 1""", [game_id])


def game_charts(con, game_id: str) -> pd.DataFrame:
    """Charts de explore de Roblox en los que apareció el juego."""
    return _df(con, """
        select sort_name as chart, min(rank) as best_rank, count(*) as hours_listed,
               min(snapshot_at) as first_seen, max(snapshot_at) as last_seen
        from roblox_chart_positions where universe_id::varchar = ?
        group by 1 order by best_rank""", [game_id])


def game_events(con, platform: str, game_id: str) -> pd.DataFrame:
    key = "universe_id::varchar" if platform == "roblox" else "island_code"
    return _df(con, f"""
        select detected_at, event_type, old_value, new_value from {platform}_events
        where {key} = ? order by detected_at desc limit 100""", [game_id])


# ------------------------------------------------------------------ clones

def clone_families(con, min_size: int = 2, cross_platform: bool = False) -> pd.DataFrame:
    return _df(con, """
        select clone_key as family, any_value(clone_group_size) as titles,
               any_value(clone_group_size_roblox) as roblox,
               any_value(clone_group_size_fortnite) as fortnite
        from title_clones
        where clone_key is not null and clone_group_size >= ?
        group by 1
        having not ? or (any_value(clone_group_size_roblox) > 0
                         and any_value(clone_group_size_fortnite) > 0)
        order by titles desc""", [min_size, cross_platform])


def clone_members(con, family: str) -> pd.DataFrame:
    return _df(con, """
        select c.platform, c.id, coalesce(g.name, i.title) as name, c.launched_at,
               c.clone_rank, coalesce(rm.peak_ccu, fm.peak_ccu) as peak_ccu
        from title_clones c
        left join roblox_games g on c.platform = 'roblox' and c.id = g.universe_id::varchar
        left join roblox_game_metrics rm
               on c.platform = 'roblox' and c.id = rm.universe_id::varchar
        left join fortnite_islands i on c.platform = 'fortnite' and c.id = i.island_code
        left join fortnite_island_metrics fm
               on c.platform = 'fortnite' and c.id = fm.island_code
        where c.clone_key = ?
        order by peak_ccu desc nulls last""", [family])
