#!/usr/bin/env bash
# Corre el colector horario de Roblox en loop dentro de un mismo job de Actions: una corrida
# por hora en el minuto <minuto> (UTC), commiteando los datos después de cada una, hasta
# agotar <presupuesto_min>. El workflow después se relanza solo (workflow_dispatch).
#
# Motivo: el cron de GitHub saltea muchas corridas (2 de 16 el 2026-11-10); el sleep dentro
# de un job no.
#
# Uso: scripts/hourly_loop.sh <dir_datos> <minuto> <presupuesto_min> [args del colector]
# Después de cada corrida lanza los workflows diarios cuyo cron no disparó
# (scripts/dispatch_missed.sh).
# Env opcionales (pruebas): LOOP_MARGIN_MIN (25), COLLECT_CMD, COMMIT_CMD, DISPATCH_CMD.
# Exit code 1 si alguna corrida falló (el job queda en rojo, pero el encadenado sigue).
set -uo pipefail

data_dir="$1"
minute="$2"
budget_min="$3"
shift 3
margin_min="${LOOP_MARGIN_MIN:-25}"  # tiempo reservado para la última corrida + commit
collect_cmd="${COLLECT_CMD:-uv run --no-dev python -m collectors.roblox}"
commit_cmd="${COMMIT_CMD:-bash scripts/commit_data.sh}"
dispatch_cmd="${DISPATCH_CMD:-bash scripts/dispatch_missed.sh}"

deadline=$(( $(date +%s) + budget_min * 60 ))
runs=0
failed=0
last_slot=0

# Próximo hh:<minuto>:00 UTC. Si el de esta hora pasó hace menos de 2 min y todavía no se
# corrió, es ese.
next_slot() {
  local now slot
  now=$(date +%s)
  slot=$(( now / 3600 * 3600 + minute * 60 ))
  if (( slot < now - 120 || slot <= last_slot )); then
    slot=$(( slot + 3600 ))
  fi
  echo "$slot"
}

while true; do
  slot=$(next_slot)
  if (( slot + margin_min * 60 > deadline )); then
    break
  fi
  last_slot=$slot
  wait_s=$(( slot - $(date +%s) ))
  if (( wait_s > 0 )); then
    echo "Esperando hasta $(date -u -d "@${slot}" +%H:%M) UTC (${wait_s} s)…"
    sleep "$wait_s"
  fi

  echo "::group::Corrida de las $(date -u +%H:%M) UTC"
  # Traer lo que commitearon los otros workflows (búsqueda, Fortnite) antes de leer.
  git -C "$data_dir" pull -q --rebase origin "$(git -C "$data_dir" rev-parse --abbrev-ref HEAD)"     || echo "::warning::pull del repo de datos falló"
  # shellcheck disable=SC2086  # los comandos se separan por espacios a propósito
  if ! $collect_cmd "$@"; then
    failed=$(( failed + 1 ))
    echo "::warning::la corrida de las $(date -u +%H:%M) terminó con error"
  fi
  # shellcheck disable=SC2086
  $commit_cmd "$data_dir" roblox || failed=$(( failed + 1 ))
  # shellcheck disable=SC2086
  $dispatch_cmd || echo "::warning::no pude revisar los workflows diarios"
  runs=$(( runs + 1 ))
  echo "::endgroup::"
done

echo "Corridas: ${runs} · con error: ${failed}"
(( failed == 0 ))
