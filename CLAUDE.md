# UGC Game Analytics — Roblox & Fortnite

## Objetivo del proyecto
Construir y mantener dos datasets vivos (Roblox experiences y Fortnite islands) recolectados
automáticamente con GitHub Actions, para responder:
- ¿Qué tiene un juego/isla para crecer rápido (explosión) y para retener (sostenido)?
- ¿Qué nichos/géneros tienen demanda creciendo y poca oferta (score de oportunidad)?
- Detección temprana: con los primeros 3–7 días de métricas, ¿qué juegos van a explotar?
Uso final: elegir nicho, timing y "empaque" (título, miniatura, tags) para crear juegos propios.
También es un proyecto de portfolio de data: código limpio, README claro, datasets publicados en Kaggle.

## Reglas para Claude Code
- Prioridad absoluta: que los colectores corran cuanto antes. Cada día sin correr es data perdida.
- Trabajá por fases (abajo). Al terminar cada fase: tests pasando, README actualizado, commit.
- Antes de usar un endpoint, verificá que siga existiendo y su formato actual (las APIs cambian).
- Respetá rate limits: concurrencia baja, reintentos con backoff exponencial + jitter, User-Agent identificable.
- Todos los timestamps en UTC. Guardá siempre el timestamp REAL de cada snapshot (`snapshot_at`).
- Nunca commitees secretos. Si hace falta un token (Kaggle, HF, R2), pedímelo para que lo cargue
  yo como GitHub Secret; vos solo referenciás `${{ secrets.NOMBRE }}`.
- Pedime confirmación antes de: crear el repo remoto, hacer push, activar workflows programados,
  publicar datasets.

## Stack
- Python 3.12, `uv` para dependencias, `httpx` (async), `polars`, `duckdb`, `pyarrow`, `pytest`.
- Almacenamiento: Parquet particionado `data/<plataforma>/<tabla>/date=YYYY-MM-DD/*.parquet`.
- Repo público (Actions gratis). Los workflows commitean datos → cuenta como actividad (evita
  que GitHub desactive los crons tras 60 días sin actividad).
- Crons en minutos "raros" (ej. :15, :37), nunca en :00.

## Fuentes de datos
### Fortnite — Fortnite Data API (pública, sin auth)
- Base: https://api.fortnite.com/ecosystem/v1
- `GET /islands` (paginado), `GET /islands/{code}`, `GET /islands/{code}/metrics/{interval}`
  (interval: minute|hour|day; retención solo en day).
- Métricas: plays, uniquePlayers, peakCCU, averageMinutesPerPlayer, favorites, recommendations,
  retention D1/D7. Solo islas públicas/descubribles; mínimo 5 jugadores únicos.
- CLAVE: la API solo guarda 7 días. Estrategia: dividir todas las islas en 5 grupos por hash del code
  y consultar un grupo por día → cobertura total sin huecos. Islas en tier hot: diario.

### Roblox — APIs web públicas (sin auth, solo snapshot actual)
- Detalles en lote: `games.roblox.com/v1/games?universeIds=...` (playing, visits, favoritedCount,
  genre, creator, created, updated, maxPlayers).
- Votos en lote: `games.roblox.com/v1/games/votes?universeIds=...`
- placeId → universeId: `apis.roblox.com/universes/v1/places/{placeId}/universe`
- Descubrimiento: charts/sorts de explore (Top Trending, Up-and-Coming, Top Playing Now, etc.)
  + barrido por keywords con omni-search. Verificar endpoints actuales antes de implementar.
- Gamepasses del juego (Fase 2): verificar endpoint actual.
- No hay retención ni historia: la serie temporal la construimos nosotros.

## Modelo de datos (ambas plataformas, columnas propias donde aplique)
- `games` (dimensión): id, platform, name, description, creator_id, creator_name, creator_type,
  genre_raw, genre_unified, tags, max_players, age_rating, created_at, updated_at,
  first_seen_at, first_traction_at (primer snapshot con CCU >= umbral), tier.
- `snapshots` (hechos): id, snapshot_at, ccu/playing, visits, favorites, up_votes, down_votes
  (Roblox) | plays, unique_players, peak_ccu, avg_minutes_per_player, favorites,
  recommendations, retention_d1, retention_d7 (Fortnite), interval.
- `events`: id, detected_at, event_type (name_change, description_change, update, thumbnail_change),
  old_value, new_value.
- `tier_history`: id, changed_at, from_tier, to_tier, reason.

## Tiers de tracking
- hot: Roblox cada 1–2 h; Fortnite diario. Top de charts, crecimiento fuerte, nuevos con señales.
- warm: diario. Juegos con algo de tracción.
- cold: semanal. Todo lo demás, para detectar si "despierta".
- `pipeline/tiers.py` promueve/degrada con reglas configurables en `config/tiers.yaml`.
- Importante (sesgo de supervivencia): trackear también juegos nuevos que NO despegan.

## Fases
### Fase 0 — Colector mínimo (hacer YA)
- [x] Estructura del repo, `pyproject.toml`, README inicial.
- [x] `collectors/fortnite.py`: paginar /islands, registrar islas nuevas, bajar métricas del grupo del día.
- [x] `collectors/roblox.py`: charts principales + IDs ya trackeados, detalles y votos en lote.
- [x] Escritura a Parquet particionado.
- [x] Workflows: `fortnite_daily.yml`, `roblox_hourly.yml` (+ `workflow_dispatch` para correr a mano).
- [x] Probar localmente y con `workflow_dispatch` antes de activar los crons.
- Criterio de listo: 3 corridas seguidas exitosas en Actions con datos commiteados.

### Fase 1 — Robustez (semanas 1–2)
- [x] Reintentos/backoff, logs claros, resumen de cada corrida (filas, errores, duración).
- [x] Checks de calidad: snapshots faltantes, valores imposibles, duplicados.
- [x] Tiers hot/warm/cold + promoción automática.
- [x] Descubrimiento de Roblox por keywords para captar lanzamientos nuevos.
- [x] Workflow semanal de compactación de Parquet chicos.

### Fase 2 — Enriquecimiento (semanas 2–4)
- [x] Historial del creador (juegos previos, mejor peak previo).
- [x] Features de título: emojis, números, corchetes [UPD], largo, palabras clave.
- [x] Gamepasses (cantidad, precios) en Roblox.
- [x] Detección de clones por similitud de nombre/descripción (por título; la descripción queda para más adelante).
- [x] Taxonomía de géneros unificada Roblox↔Fortnite (`config/genres.yaml`).
- [x] Detección de eventos (cambios de nombre/descripción/update).
- [x] Métricas derivadas: días hasta el peak, pendiente días 1–7 y 1–14, vida media de la caída,
      favoritos/visita, like ratio, peak/mediana.
- [ ] Mover almacenamiento a dataset de Kaggle o Hugging Face si el repo pasa ~500 MB.

### Fase 3 — Análisis (desde mes 2)
- [ ] Notebook de EDA.
- [ ] Score de oportunidad por nicho = crecimiento de demanda / cantidad de juegos compitiendo.
- [ ] Definir éxito: "explosión" (peak CCU >= X en 14 días desde first_traction) y
      "sostenido" (CCU día 30–60 >= 30% del peak). Umbrales por plataforma.
- [ ] Modelo de detección temprana (necesita ~6–8 semanas de lanzamientos observados).
- [ ] Dashboard (Streamlit o similar) + publicación periódica en Kaggle.

## Estructura del repo
```
collectors/   fortnite.py, roblox.py, common.py (http, retries, escritura)
pipeline/     tiers.py, features.py, quality.py, compact.py
config/       tiers.yaml, genres.yaml, keywords.yaml
data/         parquet particionado
notebooks/
tests/
.github/workflows/
```