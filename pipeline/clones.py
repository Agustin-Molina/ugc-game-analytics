"""Clones por título, entre juegos de Roblox e islas de Fortnite (vistas DuckDB).

Dos títulos son de la misma familia si, normalizados, tienen las mismas palabras:
minúsculas, sin emojis ni corchetes (`title_clean`), sin números sueltos ("Grow a Garden 2"
= "Grow a Garden"), sin palabras vacías (`clone_stopwords` en config/genres.yaml), con un
plural simple recortado ("Brainrots" = "Brainrot") y sin importar el orden.

Familia exacta (`title_key`, `title_family_size`): mismas palabras. Familia ampliada
(`clone_key`, `clone_group_size`): además, un título ÚNICO (su familia exacta tiene 1 juego)
de 3+ palabras que es una familia existente de 2+ palabras más UNA palabra se suma a esa
familia: "Steal a Brainrot Duels" → "brainrot steal". Si hay varias candidatas gana la más
grande. Es un solo nivel y no mueve familias establecidas ("1v1 build fight" no se mete en
"build fight"). Puede sumar falsos positivos con familias genéricas ("Build to Fight
Zombies" → "build fight"): para medir copias de un hit puntual, `title_key` es más estricto.

Vistas:
- `game_titles`   platform, id, title_clean, tokens, title_key, launched_at
- `title_clones`  lo anterior + title_family_size, clone_key (familia ampliada),
                  clone_group_size (total y por plataforma), clone_rank (orden de aparición
                  dentro de la familia ampliada; 1 = el primero) y clone_group_first_at.

`launched_at`: en Roblox, la fecha de creación real del juego; en Fortnite la API no la da y
se usa `first_seen_at` (todo el catálogo inicial es del 2026-11-09), así que el orden solo es
confiable entre islas aparecidas después y contra juegos de Roblox.

Las familias grandes de nombres genéricos ("1v1 build fight": 2.000+ islas) no son copias de
un hit sino saturación de un nombre; las dos cosas sirven para medir oferta.
"""

from __future__ import annotations

import duckdb

# palabra → sin plural simple ("fights" → "fight"; "boss", "glass" quedan igual)
_TOKENS_SQL = r"""
create or replace macro ugc_title_tokens(t) as list_sort(list_distinct(list_filter(
    list_transform(
        regexp_extract_all(lower(coalesce(t, '')), '[\p{L}\p{N}]+'),
        lambda w: case when length(w) > 3 and regexp_matches(w, '[^s]s$')
                       then w[1:-2] else w end),
    lambda w: length(w) > 1 and not regexp_matches(w, '^[0-9]+$')
              and not list_contains(@STOPWORDS@, w))))
"""

_TITLES_SQL = r"""
create or replace view game_titles as
@PARTS@
"""

_CLONES_SQL = r"""
create or replace view title_clones as
with t as (
    select *, array_to_string(tokens, ' ') as title_key
    from game_titles where len(tokens) > 0
), fam as (
    select title_key, count(*) as n, any_value(len(tokens)) as n_tokens
    from t group by title_key
), drop_one as (   -- cada título de 3+ palabras sin una de sus palabras
    select platform, id, array_to_string(list_filter(tokens, lambda x: x <> w), ' ') as key2
    from (select platform, id, tokens, unnest(tokens) as w from t where len(tokens) >= 3)
), parent as (
    select d.platform, d.id, arg_max(f.title_key, f.n) as parent_key
    from drop_one d join fam f on f.title_key = d.key2 and f.n_tokens >= 2
    group by d.platform, d.id
), keyed as (
    select t.platform, t.id, t.title_clean, t.tokens, t.title_key, t.launched_at,
           own.n as title_family_size,
           -- solo los títulos únicos se suman como variante de otra familia
           case when own.n = 1 then coalesce(p.parent_key, t.title_key)
                else t.title_key end as clone_key
    from t join fam own using (title_key)
    left join parent p using (platform, id)
)
select *,
       count(*) over g as clone_group_size,
       sum((platform = 'roblox')::int) over g as clone_group_size_roblox,
       sum((platform = 'fortnite')::int) over g as clone_group_size_fortnite,
       row_number() over (partition by clone_key
                          order by launched_at nulls last, platform, id) as clone_rank,
       min(launched_at) over g as clone_group_first_at
from keyed
window g as (partition by clone_key)
"""


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def register(con: duckdb.DuckDBPyConnection, stopwords: list[str],
             platforms: list[str]) -> list[str]:
    """Crea `game_titles` y `title_clones` sobre las plataformas con `<p>_game_features`."""
    if not platforms:
        return []
    stop = "[" + ", ".join(_lit(w.lower()) for w in sorted(set(stopwords))) + "]::varchar[]"
    con.execute(_TOKENS_SQL.replace("@STOPWORDS@", stop))
    parts = []
    if "roblox" in platforms:
        parts.append(
            "select 'roblox' as platform, g.universe_id::varchar as id, f.title_clean, "
            "ugc_title_tokens(f.title_clean) as tokens, "
            "coalesce(g.created_at, g.first_seen_at) as launched_at "
            "from roblox_games g join roblox_game_features f using (universe_id)")
    if "fortnite" in platforms:
        parts.append(
            "select 'fortnite' as platform, g.island_code as id, f.title_clean, "
            "ugc_title_tokens(f.title_clean) as tokens, g.first_seen_at as launched_at "
            "from fortnite_games g join fortnite_game_features f using (island_code)")
    con.execute(_TITLES_SQL.replace("@PARTS@", "\nunion all\n".join(parts)))
    con.execute(_CLONES_SQL)
    return ["game_titles", "title_clones"]


CLONE_COLUMNS = ("title_key", "title_family_size", "clone_key", "clone_group_size",
                 "clone_group_size_roblox", "clone_group_size_fortnite", "clone_rank",
                 "clone_group_first_at")


def enriched_join(platform: str, key: str) -> tuple[str, str]:
    """(columnas, join) para sumar las columnas de clones a `<platform>_games_enriched`."""
    cols = ", " + ", ".join(f"c.{c}" for c in CLONE_COLUMNS)
    join = (f" left join (select * from title_clones where platform = '{platform}') c"
            f" on c.id = g.{key}::varchar")
    return cols, join
