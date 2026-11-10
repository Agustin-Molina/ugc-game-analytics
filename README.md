# UGC Game Analytics — Roblox & Fortnite

Dos datasets vivos, recolectados automáticamente con GitHub Actions:

- **Roblox experiences**: snapshot horario de CCU, visitas, favoritos y votos de todo lo que
  aparece en los charts de explore + todo lo ya trackeado.
- **Fortnite islands**: métricas diarias (plays, jugadores únicos, peak CCU, minutos,
  favoritos, recomendaciones, retención D1/D7) y curva horaria de las islas con tracción.

Preguntas que busca responder: qué tiene un juego para **explotar** o para **retener**, qué
**nichos** tienen demanda creciente y poca oferta, y si con los primeros 3–7 días se puede
**predecir** qué va a explotar.

Los datos viven en un repo aparte: **[ugc-game-analytics-data](https://github.com/Agustin-Molina/ugc-game-analytics-data)**.

## Estado

| Fase | Estado |
|---|---|
| 0 — Colector mínimo | en curso: colectores + tests listos, workflows por `workflow_dispatch` |
| 1 — Robustez (tiers, calidad, keywords, compactación) | pendiente |
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

Rate limits medidos (no todos documentados): Fortnite ~6 req/s sostenidos; Roblox
`/v1/games` ~500 IDs cada 30 s (el header dice 300 req/min, pero el límite real es por IDs).
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
uv run python -m collectors.fortnite --limit 500
uv run python -m collectors.fortnite --codes 9782-7008-5547 --dry-run
```

`--data-dir` (o `UGC_DATA_DIR`) cambia la carpeta de salida. Configuración (tasas, lotes,
umbrales como `hourly_min_peak_ccu`) en [`config/collectors.yaml`](config/collectors.yaml).

> En Windows con *Smart App Control* activo, los binarios de polars/pyarrow pueden estar
> bloqueados; usar WSL (es además el mismo entorno que GitHub Actions).

## Automatización

| Workflow | Frecuencia | Qué hace |
|---|---|---|
| `roblox_hourly.yml` | cada hora (:17) | charts + detalles + votos |
| `fortnite_daily.yml` | diario (03:37 UTC) | catálogo + métricas del grupo del día (~3 h) |
| `keepalive.yml` | mensual | re-habilita los workflows vía API (regla de 60 días) |

Los workflows hacen checkout del repo de datos en `./data` con una deploy key
(`DATA_REPO_DEPLOY_KEY`) y commitean ahí. Todos se pueden correr a mano con
`workflow_dispatch`.

## Estructura

```
collectors/   common.py (HTTP, reintentos, rate limit, Parquet), roblox.py, fortnite.py
config/       collectors.yaml
pipeline/     db.py (vistas DuckDB); fases 1–2: tiers, features, quality, compact
tests/        pytest + respx, sin red
scripts/      commit_data.sh
.github/workflows/
```

## Licencia

Código: MIT. Los datos provienen de APIs públicas de Roblox y Epic Games y están sujetos a
sus términos.
