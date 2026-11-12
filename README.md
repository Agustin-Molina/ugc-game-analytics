# UGC Game Analytics — Roblox & Fortnite

Dos datasets vivos, recolectados automáticamente con GitHub Actions:

- **Roblox experiences**: snapshots de CCU, visitas, favoritos y votos de todo lo que aparece
  en los charts de explore y en un barrido diario de búsqueda por keywords (long tail), con
  cadencia por tier (hot cada hora, warm diario, cold semanal).
- **Fortnite islands**: métricas diarias (plays, jugadores únicos, peak CCU, minutos,
  favoritos, recomendaciones, retención D1/D7) y curva horaria de las islas con tracción, con
  cadencia por tier (hot diario, warm cada 5 días, cold cada 7).

Preguntas que busca responder: qué tiene un juego para **explotar** o para **retener**, qué
**nichos** tienen demanda creciente y poca oferta, y si con los primeros 3–7 días se puede
**predecir** qué va a explotar.

Los datos viven en un repo aparte: **[ugc-game-analytics-data](https://github.com/Agustin-Molina/ugc-game-analytics-data)**.

## Estado

| Fase | Estado |
|---|---|
| 0 — Colector mínimo | ✅ todos los crons activos (2026-11-10) |
| 1 — Robustez (tiers, calidad, keywords, compactación) | ✅ completa (2026-11-11) |
| 2 — Enriquecimiento | en curso: ícono, miniaturas, gamepasses, géneros unificados, features de título, historial del creador, clones y métricas derivadas listos; falta mudar el almacenamiento (~500 MB) |
| 3 — Análisis | en curso: score de oportunidad por nicho (Fortnite, preliminar hasta tener 2 semanas) |

## Fuentes

| Plataforma | Endpoint | Uso |
|---|---|---|
| Fortnite | `api.fortnite.com/ecosystem/v1/islands` (cursor, `size` ≤ 1000) | catálogo completo (~287k islas) |
| Fortnite | `/islands/{code}/metrics/{day\|hour}?from&to` | métricas; la API guarda **solo 7 días** |
| Roblox | `apis.roblox.com/explore-api/v1/get-sorts` (`device=all&country=all`) | ~26 charts (Top Trending, Up-and-Coming, …) |
| Roblox | `games.roblox.com/v1/games?universeIds=` (lotes de 50) | detalles: playing, visits, favoritos, género |
| Roblox | `games.roblox.com/v1/games/votes?universeIds=` (lotes de 100) | likes / dislikes |
| Roblox | `apis.roblox.com/search-api/omni-search?searchQuery=&pageType=all` (40/página, cursor) | descubrimiento por keywords |
| Roblox | `thumbnails.roblox.com/v1/games/icons` y `/v1/games/multiget/thumbnails` (lotes de 100) | ícono y miniaturas del carrusel |
| Roblox | `apis.roblox.com/game-passes/v1/universes/{id}/game-passes?passView=Full` (paginado) | gamepasses: nombre, precio, a la venta |
| Roblox | `games.roblox.com/v2/groups/{id}/games` (100/página, 30 req/min) y `/v2/users/{id}/games` (50/página) | historial del creador: todos sus juegos públicos con visitas |

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
- `events` — name_change, description_change, update, icon_change, thumbnail_change
  (old/new value).
- `search_positions` — snapshot_at, keyword, rank, universe_id, root_place_id, player_count,
  up_votes, down_votes, is_sponsored, age_rating (ranking de búsqueda por keyword, 1 vez por día).
- `tier_history` — changed_at, universe_id, from_tier, to_tier, reason (cada cambio de tier).
- `media` — log de cambios (como `games`): icon_state, icon_hash (identifica la imagen del
  ícono; es el mismo en todos los tamaños), thumbnail_ids (assets del carrusel, en orden).
- `gamepasses` — log de cambios por (universe_id, pass_id): name, description, price (null
  si no está a la venta), is_for_sale, icon_asset_id, created_at, updated_at, is_listed
  (false si desapareció del juego).
- `asset_checks` — snapshot_at, universe_id, media_ok, gamepasses_status, n_gamepasses (una
  fila por juego consultado: acota entre qué días cambió algo).
- `creator_games` — snapshot_at, creator_type, creator_id, universe_id, root_place_id, name,
  created_at, updated_at, place_visits (todos los juegos públicos de cada creador consultado,
  con visitas acumuladas a esa fecha).
- `creator_checks` — snapshot_at, creator_type, creator_id, status, n_games, truncated (una
  fila por creador consultado).

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
- `tier_history` — changed_at, island_code, from_tier, to_tier, reason.

**Ambas:** `quality` — checked_at, check, status (ok/warn/error/skip), value, unit,
warn_above, error_above, detail (resultado diario de los checks de calidad).

### Tiers de tracking

#### Roblox

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

#### Fortnite

La API guarda solo 7 días, así que la cadencia define qué se pierde. Cada consulta trae los
últimos 7 días de la isla, así que el tier sale de su última consulta:

| tier | se consulta | criterio (por defecto) |
|---|---|---|
| hot | diario | peak CCU ≥ 50 en los últimos 7 días, peak de los últimos 2 días ≥ 20 y ≥ 2× el de los 4 anteriores, o isla nueva en el catálogo (primeros 3 días) |
| warm | cada 5 días (2 de solapamiento) | algún día con ≥ 5 jugadores únicos en la ventana, o todavía sin consultar |
| cold | cada 7 días (justo la ventana) | consultada y sin datos (~80 % del catálogo) |

Warm y cold se reparten por hash del código en slots diarios. Las atrasadas (cadencia + 1 día)
entran igual, y si la corrida no llega a todo antes del deadline, lo que queda afuera es cold.
La curva horaria (islas con peak ≥ 50) se pide solo desde la última que ya tenemos, para no
guardar 7 veces las mismas horas de las islas hot. Medido sobre el primer grupo: ~1 % hot,
~15 % warm, ~84 % cold → ~46k consultas por día en régimen (antes 57k), con las hot al día.

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

- **continuidad**: hueco máximo entre corridas horarias de Roblox, juegos e islas atrasados
  respecto de la cadencia de su tier (en Fortnite, pasado el atraso la API ya borró días que
  no tenemos), antigüedad del último barrido de búsqueda y de la última corrida de Fortnite;
- **valores imposibles**: negativos, retención fuera de [0, 1], peak CCU > jugadores únicos,
  minutos por jugador mayores que el bucket, visitas que bajan;
- **duplicados** en snapshots y en el log de dimensiones, nulls y snapshots sin juego en la
  dimensión.

Cada check da ✅ ok / ⚠️ warn / ❌ error (o ⏭️ skip si falta historia). El resultado queda en
el Step Summary y en la tabla `quality`. Con algún error el workflow falla, así GitHub avisa
por mail. Hallazgo: la API de Roblox devuelve `favoritedCount` negativo en algunos juegos.
Se guarda tal cual y queda como aviso.

### Géneros unificados y features de título

[`pipeline/features.py`](pipeline/features.py) agrega dos vistas por plataforma:
`<plataforma>_game_features` (clave + features) y `<plataforma>_games_enriched` (la dimensión
con las features). Se calculan al leer: cambiar la taxonomía reclasifica toda la historia sin
tocar los datos.

- **`genre_unified`**: taxonomía común Roblox ↔ Fortnite de 25 géneros
  ([`config/genres.yaml`](config/genres.yaml)). En Roblox sale de la taxonomía oficial
  (`genre_l1` / `genre_l2`); en Fortnite, de las tags, por prioridad (una isla con
  `tycoon` + `pvp` es tycoon). Fortnite suma `genres` (todos los que matchean) y
  `licensed_ip` (Star Wars, LEGO, Squid Game…).
- **Título**: largo, palabras, emojis, números, mayúsculas, `!`, corchetes y su texto
  (`bracket_tags`), etiquetas de update / beta / evento dentro de los corchetes (`[UPD]`,
  `(BETA)`, `[🎃HALLOWEEN]`; "New York RP" no cuenta), multiplicadores (`2X`), `title_clean`
  (sin corchetes ni emojis) y `title_keywords` (keywords de `config/keywords.yaml`).
- **Clones por título** ([`pipeline/clones.py`](pipeline/clones.py), vista `title_clones`,
  entre las dos plataformas): familia de títulos con las mismas palabras ("Steal a Brainrot",
  "STEAL THE BRAINROTS", "Steal a Brainrot 2"), más los títulos únicos que agregan una
  palabra ("Steal a Brainrot Duels"). Tamaño de la familia (total y por plataforma), orden de
  aparición (`clone_rank`) y fecha del primero. En Fortnite las familias más grandes son
  nombres genéricos ("1v1 build fight": 6.700 islas), que también miden saturación.
- **Historial del creador** (`<plataforma>_creator_history`): en Roblox, juegos previos del
  creador, sus visitas máximas y totales, y cuántos pasaron el millón de visitas (de
  `creator_games`; son visitas a la fecha del listado, no a la del lanzamiento). En Fortnite,
  otras islas del creador y el mejor peak CCU diario observado en ellas.

```python
con.sql("select genre_unified, count(*) from fortnite_game_features group by 1 order by 2 desc")
```

### Métricas derivadas

[`pipeline/metrics.py`](pipeline/metrics.py) arma una serie diaria por juego
(`roblox_daily`, `fortnite_daily`; solo días cerrados) y, sobre ella,
`<plataforma>_game_metrics`. El día 0 es el de la primera tracción (CCU ≥ umbral):

| métrica | qué mide |
|---|---|
| `days_to_peak`, `days_since_peak` | días desde la primera tracción hasta el máximo, y desde el máximo |
| `slope_d1_7`, `slope_d1_14` | pendiente de ln(1 + peak diario) en los días 0–6 y 0–13 (0,1 ≈ +10 %/día) |
| `half_life_days` | días desde el peak hasta caer a la mitad (null si todavía no cayó) |
| `peak_over_median` | peak / mediana de los peaks diarios: pico aislado vs. tracción pareja |
| `sustained_ratio` | peak promedio de los días 30–60 / peak (la definición de "sostenido") |
| Roblox: `favorites_per_visit`, `like_ratio`, `visits_per_day_7d` | engagement y ritmo actual |
| Fortnite: `favorites_per_player`, `recommendations_per_player`, `retention_d1_mean`, `retention_d7_mean` | engagement y retención |

`traction_censored` marca los juegos que ya tenían tracción cuando empezamos a mirarlos: no
sabemos cuándo despegaron, así que para estudiar lanzamientos hay que filtrarlos. Los juegos
cold se miran una vez por semana: su serie diaria tiene huecos.

### Score de oportunidad por nicho (Fase 3, preliminar)

[`pipeline/opportunity.py`](pipeline/opportunity.py) cruza demanda y competencia por género
unificado. Por ahora Fortnite, que tiene historia diaria completa:

```bash
uv run python -m pipeline.opportunity
```

- **Demanda** de cada isla: jugadores únicos sumados en la última semana cubierta por sus
  consultas (semana completa: los fines de semana tienen el doble de jugadores, así que
  comparar días sueltos engaña). Un día sin datos dentro de la cobertura cuenta como 0 (la
  API no publica menos de 5 jugadores).
- **Por nicho**: islas listadas y activas, demanda total, demanda por isla activa, mediana
  (la isla típica), concentración en el top 10 y crecimiento semana contra semana.
- **Score** = jugadores-día ganados en la semana / islas activas (la definición de este
  proyecto). Necesita 14 días de cobertura; hasta entonces la tabla se ordena por la mediana.
- Sin las islas de Epic, que no compiten con los creadores.

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

## Publicación en Kaggle

[`pipeline/export.py`](pipeline/export.py) arma el dataset para Kaggle: un Parquet por tabla
(los que crecen sin límite, uno por mes), con las features ya calculadas y cada columna
documentada en [`config/kaggle.yaml`](config/kaggle.yaml). La descripción del dataset está en
[`config/kaggle_description.md`](config/kaggle_description.md).

```bash
uv run python -m pipeline.export --out export --owner <usuario-de-kaggle>
```

El workflow `kaggle_weekly.yml` exporta y sube una versión nueva (`workflow_dispatch` con
`create` para la primera publicación, que queda privada). Necesita los secrets
`KAGGLE_USERNAME` y `KAGGLE_API_TOKEN`. Licencia: CC BY-NC 4.0.

Después de cada versión, `kaggle datasets metadata --update` aplica la ficha completa:
descripciones de archivos y columnas, fuentes, frecuencia, portada y **visibilidad**
(`private` en `config/kaggle.yaml`). Todo sale del repo, así que lo que se edite a mano en la
web se pisa el lunes siguiente. Con `workflow_dispatch` + `metadata_only` se actualiza solo
la ficha, sin versión nueva. La portada (`config/kaggle_cover.png`, 560×280) se regenera con:

```bash
uv run --with matplotlib python scripts/kaggle_cover.py --data-dir ../ugc-game-analytics-data
```

## Correr localmente

Requiere [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
uv run python -m collectors.roblox --limit 100          # escribe en ./data
uv run python -m collectors.roblox_search --keywords tycoon,obby --budget-minutes 3
uv run python -m collectors.roblox_assets --limit 50
uv run python -m collectors.roblox_creators --limit 20 --budget-minutes 2
uv run python -m collectors.fortnite --limit 500
uv run python -m collectors.fortnite --codes 9782-7008-5547 --dry-run
uv run python -m pipeline.quality --no-write            # checks de calidad sobre ./data
uv run python -m pipeline.compact --dry-run             # qué particiones se compactarían
```

`--data-dir` (o `UGC_DATA_DIR`) cambia la carpeta de salida. Configuración (tasas, lotes,
umbrales como `hourly_min_peak_ccu`) en [`config/collectors.yaml`](config/collectors.yaml).

> En Windows con *Smart App Control* activo, los binarios de polars/pyarrow pueden estar
> bloqueados; usar WSL (es además el mismo entorno que GitHub Actions).

## Automatización

| Workflow | Frecuencia | Qué hace |
|---|---|---|
| `roblox_hourly.yml` | cada hora (:17) — **activo** | charts + descubiertos por búsqueda + detalles y votos de los juegos que tocan por tier. Cada run es un loop de ~5,5 h ([`scripts/hourly_loop.sh`](scripts/hourly_loop.sh)) que se relanza solo; el cron (cada 3 h) solo retoma la cadena si se corta, porque el cron de GitHub saltea muchas corridas. Después de cada corrida lanza los diarios cuyo cron no disparó ([`scripts/dispatch_missed.sh`](scripts/dispatch_missed.sh), 90 min de margen). Si después el cron llega tarde, el job `guard` de cada diario ([`scripts/ran_today.sh`](scripts/ran_today.sh)) saltea esa segunda corrida |
| `roblox_search.yml` | diario (06:23 UTC) — **activo** | barrido de keywords (~5,5 h, 1 req/min) → `search_positions` |
| `roblox_assets.yml` | diario (08:11 UTC) — **activo** | ícono, miniaturas y gamepasses (hot/warm diario, cold semanal; ~8 min) → `media`, `gamepasses`, `asset_checks`, `events` |
| `roblox_creators.yml` | diario (09:41 UTC) — **activo** | historial de creadores: nuevos primero, después cada uno 1 vez por semana (70 min de presupuesto) → `creator_games`, `creator_checks` |
| `kaggle_weekly.yml` | semanal (lunes 04:29 UTC) — **activo** | exporta y publica una versión nueva del dataset en Kaggle |
| `fortnite_daily.yml` | diario (03:37 UTC) — **activo** | catálogo + métricas de las islas que tocan por tier (~2,5–3,5 h) |
| `quality_daily.yml` | diario (12:53 UTC) — **activo** | checks de calidad → `quality`; falla si hay errores |
| `compact_weekly.yml` | domingos (02:47 UTC) | junta los Parquet de cada partición de hace ≥ 2 días en uno ([`pipeline/compact.py`](pipeline/compact.py)); mismo contenido |
| `keepalive.yml` | mensual (día 3) — **activo** | re-habilita los workflows vía API (regla de 60 días) |

Los workflows hacen checkout del repo de datos en `./data` con una deploy key
(`DATA_REPO_DEPLOY_KEY`) y commitean ahí. Todos se pueden correr a mano con
`workflow_dispatch`.

## Estructura

```
collectors/   common.py (HTTP, reintentos, rate limit, Parquet), roblox.py, roblox_search.py,
              roblox_assets.py, roblox_creators.py, fortnite.py
config/       collectors.yaml, tiers.yaml, keywords.yaml, quality.yaml, genres.yaml
pipeline/     db.py (vistas DuckDB), features.py, clones.py, metrics.py, opportunity.py,
              tiers.py, quality.py, compact.py
tests/        pytest + respx, sin red
scripts/      commit_data.sh
.github/workflows/
```

## Licencia

Código: MIT. Los datos provienen de APIs públicas de Roblox y Epic Games y están sujetos a
sus términos.
