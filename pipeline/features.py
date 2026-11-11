"""Features derivadas de la dimensión de juegos, como vistas DuckDB.

Se calculan al leer, no se guardan: cambiar `config/genres.yaml` reclasifica todo sin tocar
los datos. `pipeline.db.connect()` registra estas vistas si la dimensión existe:

- `<plataforma>_game_features`  clave + género unificado + features de título
- `<plataforma>_games_enriched` estado actual de la dimensión + esas features

Género unificado (`genre_unified`): taxonomía común Roblox ↔ Fortnite.
- Roblox: reglas sobre `genre_l1` / `genre_l2` / `genre_raw`; gana la primera que matchea.
- Fortnite: reglas sobre las tags; gana la regla de mayor prioridad entre las tags de la
  isla. `genres` lista todos los géneros que matchean, por prioridad. `licensed_ip` es la IP
  licenciada (`category`) normalizada a mayúsculas.

Features de título (ambas plataformas; en Roblox el título es `name`):
- `title_len`, `title_words`, `n_emojis`, `has_emoji`, `has_number`, `has_exclamation`,
  `caps_ratio` (mayúsculas / letras)
- `has_brackets`, `starts_with_bracket`, `bracket_tags` (texto de cada [..] / (..) / 【..】)
- `has_update_tag`, `has_beta_tag`, `has_event_tag`: etiquetas dentro de los corchetes
  (expresiones en `title_tags` de genres.yaml)
- `has_multiplier` ("2X", "x10"), `title_clean` (sin corchetes ni emojis, para comparar
  nombres), `title_keywords` (keywords de config/keywords.yaml presentes como palabras)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[1]
GENRES_PATH = ROOT / "config" / "genres.yaml"
KEYWORDS_PATH = ROOT / "config" / "keywords.yaml"

# Bloques Unicode de emojis y símbolos pictográficos (RE2). Cuenta code points: un emoji
# compuesto (ZWJ, banderas) suma más de uno.
EMOJI_RE = r"[\x{1F000}-\x{1FAFF}\x{2600}-\x{27BF}\x{2B00}-\x{2BFF}\x{1F1E6}-\x{1F1FF}]"
VARIATION_RE = r"[\x{FE0F}\x{200D}]"
BRACKET_RE = r"[\[\(【]([^\]\)】]*)[\]\)】]"

# columnas que necesita cada vista de features
REQUIRED = {
    "roblox": {"universe_id", "name", "genre_l1", "genre_l2", "genre_raw"},
    "fortnite": {"island_code", "title", "tags", "category"},
}


def load_genres(path: Path = GENRES_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    known = set(cfg["genres"])
    for platform in ("roblox", "fortnite"):
        for rule in cfg[platform]:
            if rule["genre"] not in known:
                raise ValueError(f"{platform}: género desconocido {rule['genre']!r}")
    tags = [t for rule in cfg["fortnite"] for t in rule["tags"]]
    dup = {t for t in tags if tags.count(t) > 1}
    if dup:
        raise ValueError(f"fortnite: tags en más de una regla: {sorted(dup)}")
    return cfg


def load_keywords(path: Path = KEYWORDS_PATH) -> list[str]:
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    words = {str(k).strip().lower() for k in cfg.get("roblox", [])}
    return sorted(w for w in words if w)


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def title_features_sql(cfg: dict[str, Any], keywords: list[str]) -> str:
    """Macro `ugc_title_features(t)`: struct con las features de un título."""
    tags = cfg["title_tags"]
    brackets = f"upper(array_to_string(regexp_extract_all(t, {_lit(BRACKET_RE)}, 1), ' | '))"
    letters = r"'\p{L}'"
    kw_list = "[" + ", ".join(_lit(k) for k in keywords) + "]::varchar[]"
    clean = (
        f"trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
        f"t, {_lit(BRACKET_RE)}, ' ', 'g'), {_lit(EMOJI_RE)}, ' ', 'g'), "
        f"{_lit(VARIATION_RE)}, '', 'g'), '\\s+', ' ', 'g'))"
    )
    return f"""
create or replace macro ugc_title_features(t) as struct_pack(
    title_len := length(t),
    title_words := len(regexp_extract_all(t, '[\\p{{L}}\\p{{N}}]+')),
    n_emojis := len(regexp_extract_all(t, {_lit(EMOJI_RE)})),
    has_emoji := regexp_matches(t, {_lit(EMOJI_RE)}),
    has_number := regexp_matches(t, '[0-9]'),
    has_exclamation := contains(t, '!'),
    caps_ratio := len(regexp_extract_all(t, '\\p{{Lu}}'))
                  / nullif(len(regexp_extract_all(t, {letters})), 0),
    has_brackets := regexp_matches(t, {_lit(BRACKET_RE)}),
    starts_with_bracket := regexp_matches(t, '^\\s*[\\[\\(【]'),
    bracket_tags := list_filter(list_transform(
        regexp_extract_all(t, {_lit(BRACKET_RE)}, 1),
        lambda b: trim(regexp_replace(regexp_replace(b, {_lit(EMOJI_RE)}, '', 'g'),
                                      {_lit(VARIATION_RE)}, '', 'g'))),
        lambda b: b <> ''),
    -- "[UPD] X" o "🎨UPD🎨 X" (UPD/UPDATE al inicio del título limpio, sin corchetes)
    has_update_tag := regexp_matches({brackets}, {_lit(tags["update"])})
                      or regexp_matches(upper({clean}), '^(UPD|UPDATE)\\b'),
    has_beta_tag := regexp_matches({brackets}, {_lit(tags["beta"])}),
    has_event_tag := regexp_matches({brackets}, {_lit(tags["event"])}),
    has_multiplier := regexp_matches(upper(t), '\\b(\\d+X|X\\d+)\\b'),
    title_clean := {clean},
    -- sobre el título limpio: "[BETA]" no cuenta como keyword "beta"
    title_keywords := list_filter({kw_list}, lambda k: contains(
        ' ' || lower(regexp_replace({clean}, '[^\\p{{L}}\\p{{N}}]+', ' ', 'g')) || ' ',
        ' ' || k || ' '))
)"""


def _columns(con: duckdb.DuckDBPyConnection, view: str) -> set[str]:
    return {r[0] for r in con.sql(f"describe {view}").fetchall()}


def _has_view(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return bool(con.execute(
        "select count(*) from duckdb_views() where view_name = ?", [name]).fetchall()[0][0])


def register(
    con: duckdb.DuckDBPyConnection,
    cfg: dict[str, Any] | None = None,
    keywords: list[str] | None = None,
) -> list[str]:
    """Crea las vistas de features para las plataformas con dimensión. Devuelve sus nombres."""
    cfg = cfg or load_genres()
    keywords = load_keywords() if keywords is None else keywords
    con.execute(title_features_sql(cfg, keywords))
    created: list[str] = []

    if _has_view(con, "roblox_games") and REQUIRED["roblox"] <= _columns(con, "roblox_games"):
        con.execute("create or replace table _genre_rules_roblox "
                    "(priority int, l1 varchar, l2 varchar, raw varchar, genre varchar)")
        con.executemany(
            "insert into _genre_rules_roblox values (?, ?, ?, ?, ?)",
            [[i, r.get("l1"), r.get("l2"), r.get("raw"), r["genre"]]
             for i, r in enumerate(cfg["roblox"])],
        )
        cols = _columns(con, "roblox_games")
        desc = (",\n       length(coalesce(g.description, '')) as description_len"
                if "description" in cols else "")
        con.execute(f"""
create or replace view roblox_game_features as
select g.universe_id,
       coalesce((select r.genre from _genre_rules_roblox r
                 where (r.l1 is null or r.l1 = coalesce(g.genre_l1, ''))
                   and (r.l2 is null or r.l2 = coalesce(g.genre_l2, ''))
                   and (r.raw is null or r.raw = coalesce(g.genre_raw, ''))
                 order by r.priority limit 1), 'other') as genre_unified,
       unnest(ugc_title_features(g.name)){desc}
from roblox_games g""")
        created.append("roblox_game_features")

    if (_has_view(con, "fortnite_games")
            and REQUIRED["fortnite"] <= _columns(con, "fortnite_games")):
        con.execute("create or replace table _genre_rules_fortnite "
                    "(priority int, tag varchar, genre varchar)")
        con.executemany(
            "insert into _genre_rules_fortnite values (?, ?, ?)",
            [[i, t.lower(), r["genre"]] for i, r in enumerate(cfg["fortnite"])
             for t in r["tags"]],
        )
        con.execute("""
create or replace view fortnite_game_features as
with tags as (
    select island_code, lower(trim(unnest(tags))) as tag from fortnite_games
), matched as (
    select t.island_code, r.genre, min(r.priority) as priority
    from tags t join _genre_rules_fortnite r using (tag)
    group by all
), agg as (
    select island_code, arg_min(genre, priority) as genre_unified,
           list(genre order by priority) as genres
    from matched group by island_code
)
select g.island_code,
       coalesce(a.genre_unified, 'other') as genre_unified,
       coalesce(a.genres, []::varchar[]) as genres,
       nullif(upper(trim(g.category)), '') as licensed_ip,
       unnest(ugc_title_features(g.title))
from fortnite_games g left join agg a using (island_code)""")
        created.append("fortnite_game_features")

    for platform, key in (("roblox", "universe_id"), ("fortnite", "island_code")):
        if f"{platform}_game_features" in created:
            # la columna genre_unified de la dimensión (vacía) la reemplaza la de features
            excl = (" exclude (genre_unified)"
                    if "genre_unified" in _columns(con, f"{platform}_games") else "")
            con.execute(f"""
create or replace view {platform}_games_enriched as
select g.*{excl}, f.* exclude ({key})
from {platform}_games g join {platform}_game_features f using ({key})""")
            created.append(f"{platform}_games_enriched")
    return created
