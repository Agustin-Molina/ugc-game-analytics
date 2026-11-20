"""Tendencias entre plataformas: formatos que crecen en una y llegan (o no) a la otra.

Medido 2026-11-20: "Throw Food For Animals" salió en Roblox el 18/09 y explotó; en Fortnite
ya había 17+ copias y seguían apareciendo nuevas cada pocos días. Otros formatos cruzaron
con 19–51 días de demora ("Seed Throw", "Do Not Stop", "Bomb Volley", "Glow Up"). Esta vista
busca esas olas de forma sistemática.

Un **tema** se arma de dos maneras (`level`):
- `family`: la familia de clones de `title_clones` (`clone_key`): el formato exacto.
- `keyword`: cada palabra significativa del título (`tokens`): agrupa una ola que las
  familias parten en pedazos ("ragdoll realistic", "physic ragdoll", "ragdoll sandbox").
  Solo palabras con >= 5 juegos y alguno nuevo en 30 días; para tener `signal` tienen que
  ser una ola: >= 20 % de sus juegos nuevos (`new_share`). Así las genéricas ("1v1",
  "build", "vs": miles de títulos que siempre suman algunos nuevos) no tapan a las tendencias.

Por tema y plataforma (`rb_*` Roblox, `fn_*` Fortnite): juegos, juegos nuevos en 7 y 30 días,
primera aparición, demanda de la última semana y su crecimiento (visitas en Roblox, jugadores
únicos en Fortnite; de `pipeline/opportunity.py`) y el juego más jugado.

Fechas: Roblox publica la fecha de creación; Fortnite no, así que la de una isla es la de su
primera aparición en el catálogo, y todo lo que existía antes de la primera carga figura
con esa fecha (`fn_first_known = false`). Por eso `first_platform` / `lag_days` solo son
exactos para formatos que llegaron a Fortnite después de la carga inicial. `lag_days` se
informa hasta 180 días: más que eso es un formato viejo que reaparece, no la misma ola.

`signal`:
- `port_to_fortnite`: crece en Roblox (>= 3 juegos nuevos en 30 días y >= 1 M
  visitas en la semana) y en Fortnite hay <= 2 islas: la ventana para llegar primero.
- `port_to_roblox`: lo mismo al revés (>= 5.000 jugadores en la semana), salvo formatos
  atados a construir y combatir en Fortnite (género pvp_practice / battle_royale, o palabras
  como 1v1, buildfight, zone wars, pillars, rvb, edit).
En las dos, la plataforma de destino tampoco tiene que tener ya demanda grande: si la tiene,
el formato nació allá ("Throw Food For Animals": 2 juegos en Roblox, pero 155 M visitas).
- `crossed`: está en las dos plataformas con copias nuevas en alguna.

Sesgo: en Roblox solo están los juegos trackeados (charts, búsquedas, creadores conocidos),
y el emparejamiento es por título: el mismo formato con otro nombre no se detecta.
"""

from __future__ import annotations

import duckdb

KW_MIN_GAMES = 5
# Una palabra es "ola" si al menos esta parte de sus juegos es nueva (30 días); si no, es
# genérica ("1v1", "build", "vs": miles de títulos, siempre suman algunos nuevos).
KW_MIN_NEW_SHARE = 0.2
PORT_MIN_NEW_30D = 3
PORT_MAX_OTHER = 2
RB_MIN_VISITS_W1 = 1_000_000
FN_MIN_PLAYERS_W1 = 5_000
# Formatos atados a la mecánica de construir/combatir de Fortnite: no se portan a Roblox. El
# género no alcanza (medido 2026-11-20: pillars, edit wars y buildfights caen en shooter o
# party_casual), así que también por palabras del tema.
FORTNITE_NATIVE_GENRES = ("pvp_practice", "battle_royale")
FORTNITE_NATIVE_TOKENS = ("1v1", "1v1v1", "2v2", "3v3", "4v4", "buildfight", "boxfight",
                          "zone", "zonewar", "pillar", "pilar", "piler", "piller", "rvb",
                          "tilted", "reload", "ffa", "edit", "realistic", "royale")
MAX_LAG_DAYS = 180  # más que esto no es "la misma ola", es un formato viejo que reaparece
EXCLUDED_CREATORS = ("epic",)


def _has_view(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    # fetchall: con fetchone el resultado queda abierto y DuckDB 2.0 bloquea la conexión
    return bool(con.execute(
        "select count(*) from duckdb_views() where view_name = ?", [name]).fetchall()[0][0])


_GAMES = """
create or replace view trend_games as
with ref as (
    select min(launched_at) filter (where platform = 'fortnite') as fn_start,
           max(launched_at) as ref_at
    from title_clones
)
select c.platform, c.id, c.title_clean, c.clone_key, c.tokens, c.launched_at,
       c.platform = 'roblox' or c.launched_at > ref.fn_start + interval 1 day as launch_known,
       c.launched_at >= ref.ref_at - interval 7 day as new_7d,
       c.launched_at >= ref.ref_at - interval 30 day as new_30d,
       d.w1, d.w0, gf.genre_unified
from title_clones c
cross join ref
left join (@DEMAND@) d using (platform, id)
left join (@GENRES@) gf using (platform, id)
@EXCLUDE@"""

_GENRE_PARTS = {
    "roblox_game_features": "select 'roblox' as platform, universe_id::varchar as id, "
                            "genre_unified from roblox_game_features",
    "fortnite_game_features": "select 'fortnite' as platform, island_code as id, "
                              "genre_unified from fortnite_game_features",
}

_DEMAND_PARTS = {
    "roblox_game_demand": "select 'roblox' as platform, universe_id::varchar as id, "
                          "visits_w1 as w1, visits_w0 as w0 from roblox_game_demand",
    "fortnite_island_demand": "select 'fortnite' as platform, island_code as id, "
                              "demand_w1 as w1, demand_w0 as w0 "
                              "from fortnite_island_demand",
}

_TRENDS = """
create or replace view cross_platform_trends as
with themed as (
    select 'family' as level, clone_key as theme, g.* from trend_games g
    where clone_key is not null
    union all
    select 'keyword', t.tok, g.* from trend_games g, unnest(g.tokens) as t(tok)
), agg as (
    select level, theme,
        count(*) filter (where platform = 'roblox') as rb_games,
        count(*) filter (where platform = 'fortnite') as fn_games,
        count(*) filter (where platform = 'roblox' and new_7d) as rb_new_7d,
        count(*) filter (where platform = 'roblox' and new_30d) as rb_new_30d,
        count(*) filter (where platform = 'fortnite' and new_7d and launch_known) as fn_new_7d,
        count(*) filter (where platform = 'fortnite' and new_30d and launch_known)
            as fn_new_30d,
        min(launched_at) filter (where platform = 'roblox') as rb_first_at,
        min(launched_at) filter (where platform = 'fortnite') as fn_first_at,
        bool_and(launch_known) filter (where platform = 'fortnite') as fn_first_known,
        sum(w1) filter (where platform = 'roblox') as rb_visits_w1,
        sum(w1) filter (where platform = 'roblox' and w0 is not null)
            / nullif(sum(w0) filter (where platform = 'roblox'), 0) - 1 as rb_visits_growth,
        sum(w1) filter (where platform = 'fortnite') as fn_players_w1,
        sum(w1) filter (where platform = 'fortnite' and w0 is not null)
            / nullif(sum(w0) filter (where platform = 'fortnite'), 0) - 1
            as fn_players_growth,
        arg_max(title_clean, coalesce(w1, -1)) filter (where platform = 'roblox')
            as rb_top_game,
        arg_max(title_clean, coalesce(w1, -1)) filter (where platform = 'fortnite')
            as fn_top_island,
        mode(genre_unified) as genre
    from themed group by level, theme
), labeled as (
    select *,
        case
            when fn_games = 0 then 'roblox_only'
            when rb_games = 0 then 'fortnite_only'
            when fn_first_known and rb_first_at < fn_first_at then 'roblox'
            when fn_first_known and fn_first_at < rb_first_at then 'fortnite'
            -- la isla ya existía en la carga inicial y el juego de Roblox es posterior
            when not fn_first_known and rb_first_at > fn_first_at then 'fortnite'
            else 'unknown'
        end as first_platform,
        (rb_new_30d + fn_new_30d) / (rb_games + fn_games) as new_share,
        -- una palabra cuenta como ola solo si buena parte de sus juegos es nueva
        level = 'family' or (rb_new_30d + fn_new_30d) / (rb_games + fn_games) >= @KW_SHARE@
            as is_wave
    from agg
    where (level = 'family' and rb_games + fn_games >= 2)
       or (level = 'keyword' and rb_games + fn_games >= @KW_MIN@
           and rb_new_30d + fn_new_30d > 0)
)
select level, theme, genre, rb_games, fn_games, rb_new_7d, rb_new_30d, fn_new_7d,
       fn_new_30d, new_share, rb_first_at, fn_first_at, fn_first_known, first_platform,
       case when first_platform in ('roblox', 'fortnite') and fn_first_known
                 and abs(date_diff('day', rb_first_at, fn_first_at)) <= @MAX_LAG@
            then abs(date_diff('day', rb_first_at, fn_first_at)) end as lag_days,
       rb_visits_w1, rb_visits_growth, fn_players_w1, fn_players_growth,
       rb_top_game, fn_top_island,
       case
           when not is_wave then null
           -- "para portar": crece en una plataforma y en la otra casi no existe (pocos
           -- juegos y sin demanda grande: si allá ya es enorme, nació allá)
           when rb_new_30d >= @NEW@ and coalesce(rb_visits_w1, 0) >= @RB_DEMAND@
                and fn_games <= @MAX_OTHER@ and coalesce(fn_players_w1, 0) < @FN_DEMAND@
               then 'port_to_fortnite'
           when fn_new_30d >= @NEW@ and coalesce(fn_players_w1, 0) >= @FN_DEMAND@
                and rb_games <= @MAX_OTHER@ and coalesce(rb_visits_w1, 0) < @RB_DEMAND@
                and coalesce(genre, '') not in (@NATIVE@)
                and not list_has_any(string_split(theme, ' '), [@NATIVE_TOKENS@])
               then 'port_to_roblox'
           when rb_games > 0 and fn_games > 0 and rb_new_30d + fn_new_30d > 0
               then 'crossed'
       end as signal
from labeled"""


def _fill(sql: str) -> str:
    native = ", ".join(f"'{g}'" for g in FORTNITE_NATIVE_GENRES)
    native_tokens = ", ".join(f"'{t}'" for t in FORTNITE_NATIVE_TOKENS)
    for key, val in {"@KW_MIN@": KW_MIN_GAMES, "@KW_SHARE@": KW_MIN_NEW_SHARE,
                     "@NEW@": PORT_MIN_NEW_30D, "@MAX_OTHER@": PORT_MAX_OTHER,
                     "@RB_DEMAND@": RB_MIN_VISITS_W1, "@FN_DEMAND@": FN_MIN_PLAYERS_W1,
                     "@NATIVE_TOKENS@": native_tokens, "@NATIVE@": native,
                     "@MAX_LAG@": MAX_LAG_DAYS}.items():
        sql = sql.replace(key, str(val))
    return sql


def register(con: duckdb.DuckDBPyConnection) -> list[str]:
    """`trend_games` y `cross_platform_trends`, si existe `title_clones`. La demanda sale de
    las vistas de `pipeline/opportunity.py` que estén (si no hay, queda null)."""
    if not _has_view(con, "title_clones"):
        return []
    parts = [sql for view, sql in _DEMAND_PARTS.items() if _has_view(con, view)]
    demand = " union all ".join(parts) if parts else (
        "select null::varchar as platform, null::varchar as id, null::double as w1, "
        "null::double as w0 where false")
    exclude = ""
    if _has_view(con, "fortnite_games"):
        names = ", ".join("'" + c.replace("'", "''") + "'" for c in EXCLUDED_CREATORS)
        exclude = (
            "where not (c.platform = 'fortnite' and c.id in (select island_code from "
            f"fortnite_games where coalesce(creator_code, '') in ({names})))")
    gparts = [sql for view, sql in _GENRE_PARTS.items() if _has_view(con, view)]
    genres = " union all ".join(gparts) if gparts else (
        "select null::varchar as platform, null::varchar as id, null::varchar as "
        "genre_unified where false")
    con.execute(_GAMES.replace("@DEMAND@", demand).replace("@GENRES@", genres)
                .replace("@EXCLUDE@", exclude))
    con.execute(_fill(_TRENDS))
    return ["trend_games", "cross_platform_trends"]
