# UGC Game Analytics — Roblox & Fortnite

Dos datasets vivos, recolectados automáticamente con GitHub Actions:

- **Roblox experiences**: snapshots de CCU, visitas, favoritos y votos de todo lo que aparece
  en los charts de explore y en un barrido diario de búsqueda por keywords (long tail), con
  cadencia por tier (hot cada hora, warm diario, cold semanal).
- **Fortnite islands**: métricas diarias (plays, jugadores únicos, peak CCU, minutos,
  favoritos, recomendaciones, retención D1/D7) y curva horaria de las islas con tracción.

Preguntas que busca responder: qué tiene un juego para **explotar** o para **retener**, qué
**nichos** tienen demanda creciente y poca oferta, y si con los primeros 3–7 días se puede
**predecir** qué va a explotar.

Los datos viven en un repo aparte: **[ugc-game-analytics-data](https://github.com/Agustin-Molina/ugc-game-analytics-data)**.

## Estado

| Fase | Estado |
|---|---|
| 0 — Colector mínimo | ✅ todos los crons activos (2026-11-10) |
| 1 — Robustez (tiers, calidad, keywords, compactación) | en curso: tiers y búsqueda por keywords de Roblox y checks de calidad listos |
| 2 — Enriquecimiento | pendiente |
| 3 — Análisis | pendiente |

## Fuentes

| Plataforma | Endpoint | Uso |
|---|---|---|
| Fortnite | `api.fortnite.com/ecosystem/v1/islands` (cursor, `size` ≤ 1000) | catálogo completo (~287k islas) |
| Fortnite | `/islands/{code}/metrics/{day\|hour}?from&to` | métricas; la API guarda **solo 7 días** |
| Roblox | `apis.roblox.com/explore-api/v1/get-sorts` (`device=all&country=all`) | ~26 charts (Top Trending, Up-and-Coming, …) |
| Roblox | `games.roblox.com/v1/games?universeIds=` (lotes de 50) | detalles: playing, visits, favoritos, género |
| Roblox | `games.roblox.com/v1/games/votes?universeIds=` (lotes de 100) | likes / dislikes |
| Roblox | `apis.roblox.com/search-api/omni-search?searchQuery=&pageType=all` (40/página, cursor) | descubrimiento por keywords |

Rate limits medidos (no todos documentados): Fortnite ~6 req/s sostenidos; Roblox
`/v1/games` ~500 IDs cada 30 s (el header dice 300 req/min, pero el límite real es por IDs);
omni-search 4 req/s desde una IP residencial pero **1 req/min desde IPs de datacenter**
(GitHub Actions).
Todo con reintentos, backoff exponencial con jitter y User-Agent identificable.

**Cobertura de Fortnite sin huecos:** las islas se reparten en 5 grupos por `sha1(code) % 5`
y cada día se consulta un grupo con ventana de 7 días → cada isla se ve cada 5 días con 2 días
de solapamiento.

## Tablas

Parquet (zstd) particionado: `<plataforma>/<tabla>/date=YYYY-MM-DD/*.parquet`. Todos los
timestamps en UTC; `snapshot_at` es el momento real de cada respuesta.

Las dimensiones (`games`) son un **log append-only**: cada corrida agrega solo las filas
nuevas o con algún atributo cambiado, con `valid_from`. El estado actual es la última fila
por id (vista `<plataforma>_games` en [`pipeline/db.py`](pipeline/db.py)). Así el historial
de git no guarda una copia completa de la dimensión por corrida. Los campos que cambian
siempre (último visto, última consulta, última actividad) no se guardan: se derivan de
`snapshots` / `fetch_log`.

**Roblox**
- `snapshots` — snapshot_at, universe_id, playing, visits, favorites, up_votes, down_votes,
  updated_at, max_players, price.
- `chart_positions` — snapshot_at, sort_id, sort_name, rank, universe_id, player_count,
  up_votes, down_votes, is_sponsored.
- `games` — dimensión (log): nombre, descripción, creador, géneros (`genre_raw`,
  `genre_l1`, `genre_l2`), created/updated, `first_seen_at`, `first_traction_at`, tier,
  `valid_from`.
- `events` — name_change, description_change, update (old/new value).
- `search_positions` — snapshot_at, keyword, rank, universe_id, root_place_id, player_count,
  up_votes, down_votes, is_sponsored, age_rating (ranking de búsqueda por keyword, 1 vez por día).
- `tier_history` — changed_at, universe_id, from_tier, to_tier, reason (cada cambio de tier).

**Fortnite**
- `snapshots` — island_code, interval (`day`|`hour`), bucket_start, snapshot_at, is_partial,
  plays, unique_players, peak_ccu, avg_minutes_per_player, minutes_played, favorites,
  recommendations, retention_d1, retention_d7 (**fracción 0–1**). Por el solapamiento puede
  haber varias filas por (isla, bucket): la vista `fortnite_snapshots_latest` se queda con el
  `snapshot_at` más reciente.
- `fetch_log` — una fila por consulta (también las islas sin datos) → permite medir
  supervivencia sin sesgo.
- `games` — dimensión (log): título, creador, categoría (IP), tags, `first_seen_at`,
  `is_listed`, `first_traction_at`, tier, `valid_from`.
- `events` — island_new, island_removed, island_relisted, title_change, tags_change,
  category_change.

**Ambas:** `quality` — checked_at, check, status (ok/warn/error/skip), value, unit,
warn_above, error_above, detail (resultado diario de los checks de calidad).

### Tiers de tracking (Roblox)

Para que el set trackeado pueda crecer sin romper el presupuesto horario de la API, cada
juego tiene un tier ([`config/tiers.yaml`](config/tiers.yaml), lógica en
[`pipeline/tiers.py`](pipeline/tiers.py)):

| tier | se consulta | criterio (por defecto) |
|---|---|---|
| hot | cada hora | en algún chart, playing ≥ 1000, crecimiento ≥ 1,5× en ~24 h (con ≥ 100), o creado hace ≤ 14 días con ≥ 20 — cualquier señal en las últimas 48 h |
| warm | diario | pico ≥ 20 en 11 días, o en un chart en los últimos 7 días |
| cold | semanal | el resto (incluye privados/borrados) |

Warm y cold se reparten por hash del id en slots horarios (24 y 168) → carga pareja; los que
quedaron atrasados por corridas salteadas entran igual. Los juegos de los charts se consultan
siempre. Cada corrida recalcula los tiers con los snapshots/charts recientes (sin estado
extra) y registra los cambios en `tier_history`. Los juegos que no despegan bajan a warm/cold
pero se siguen observando (sin sesgo de supervivencia).

### Descubrimiento por keywords (Roblox)

Los charts solo muestran lo que ya está arriba. Para tener historia **antes** de que un juego
explote, [`collectors/roblox_search.py`](collectors/roblox_search.py) barre una vez por día
las keywords de [`config/keywords.yaml`](config/keywords.yaml) y guarda el ranking en
`search_positions`. La corrida horaria toma de ahí los resultados con ≥ 1 jugador que no
trackeábamos (hasta 1500 por corrida, priorizando por jugadores; `discovered_via =
search:<keyword>`) y los tiers deciden su cadencia.

Desde GitHub Actions omni-search permite 1 request por minuto, así que el barrido es un
workflow aparte de ~5,5 h (≈ 325 páginas de 40): recorre las keywords en anchura (página 1
de todas, después la 2, …) para que todas queden cubiertas. Medido: la búsqueda ordena por
relevancia (no por fecha) y casi no muestra juegos de pocos días; aporta long tail, no
lanzamientos del día.

### Checks de calidad

[`pipeline/quality.py`](pipeline/quality.py) corre una vez por día y revisa, con umbrales en
[`config/quality.yaml`](config/quality.yaml):

- **continuidad**: hueco máximo entre corridas horarias de Roblox, juegos atrasados respecto
  de la cadencia de su tier, antigüedad del último barrido de búsqueda y de la última corrida
  de Fortnite, islas del grupo del día sin consultar, e islas sin consulta en 6 días (la API
  ya borró parte de su historia);
- **valores imposibles**: negativos, retención fuera de [0, 1], peak CCU > jugadores únicos,
  minutos por jugador mayores que el bucket, visitas que bajan;
- **duplicados** en snapshots y en el log de dimensiones, nulls y snapshots sin juego en la
  dimensión.

Cada check da ✅ ok / ⚠️ warn / ❌ error (o ⏭️ skip si falta historia). El resultado queda en
el Step Summary y en la tabla `quality`. Con algún error el workflow falla, así GitHub avisa
por mail. Hallazgo: la API de Roblox devuelve `favoritedCount` negativo en algunos juegos.
Se guarda tal cual y queda como aviso.

### Leer con DuckDB

```python
from pipeline.db import connect

con = connect()  # ./data o $UGC_DATA_DIR; crea vistas para todas las tablas
con.sql("""
    select g.name, max(s.playing) as peak
    from roblox_snapshots s join roblox_games g using (universe_id)
    group by 1 order by peak desc limit 20
""")
```

`uv run python -m pipeline.db` lista las vistas disponibles y sus filas.

## Correr localmente

Requiere [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
uv run python -m collectors.roblox --limit 100          # escribe en ./data
uv run python -m collectors.roblox_search --keywords tycoon,obby --budget-minutes 3
uv run python -m collectors.fortnite --limit 500
uv run python -m collectors.fortnite --codes 9782-7008-5547 --dry-run
uv run python -m pipeline.quality --no-write            # checks de calidad sobre ./data
```

`--data-dir` (o `UGC_DATA_DIR`) cambia la carpeta de salida. Configuración (tasas, lotes,
umbrales como `hourly_min_peak_ccu`) en [`config/collectors.yaml`](config/collectors.yaml).

> En Windows con *Smart App Control* activo, los binarios de polars/pyarrow pueden estar
> bloqueados; usar WSL (es además el mismo entorno que GitHub Actions).

## Automatización

| Workflow | Frecuencia | Qué hace |
|---|---|---|
| `roblox_hourly.yml` | cada hora (:17) — **activo** | charts + descubiertos por búsqueda + detalles y votos de los juegos que tocan por tier |
| `roblox_search.yml` | diario (06:23 UTC) — **activo** | barrido de keywords (~5,5 h, 1 req/min) → `search_positions` |
| `fortnite_daily.yml` | diario (03:37 UTC) — **activo** | catálogo + métricas del grupo del día (~3 h) |
| `quality_daily.yml` | diario (12:53 UTC) | checks de calidad → `quality`; falla si hay errores |
| `keepalive.yml` | mensual (día 3) — **activo** | re-habilita los workflows vía API (regla de 60 días) |

Los workflows hacen checkout del repo de datos en `./data` con una deploy key
(`DATA_REPO_DEPLOY_KEY`) y commitean ahí. Todos se pueden correr a mano con
`workflow_dispatch`.

## Estructura

```
collectors/   common.py (HTTP, reintentos, rate limit, Parquet), roblox.py, roblox_search.py,
              fortnite.py
config/       collectors.yaml, tiers.yaml, keywords.yaml
pipeline/     db.py (vistas DuckDB), tiers.py, quality.py; próximas: compact, features
tests/        pytest + respx, sin red
scripts/      commit_data.sh
.github/workflows/
```

## Licencia

Código: MIT. Los datos provienen de APIs públicas de Roblox y Epic Games y están sujetos a
sus términos.
