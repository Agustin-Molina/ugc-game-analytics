#!/usr/bin/env bash
# Guard de los workflows diarios/semanales: decide si esta corrida se saltea.
#
# Los crons de GitHub llegan a disparar 7-8 h tarde; para entonces scripts/dispatch_missed.sh
# ya lanzó el workflow y el cron tardío lo correría por segunda vez. Una corrida *programada*
# se saltea si hoy (UTC) ya hubo otra corrida del mismo workflow exitosa o en curso. Las
# lanzadas a mano o por el dispatcher (workflow_dispatch) corren siempre.
#
# Escribe `skip=true|false` en $GITHUB_OUTPUT. Usa las variables estándar de Actions
# (GITHUB_EVENT_NAME, GITHUB_WORKFLOW_REF, GITHUB_RUN_ID, GITHUB_REPOSITORY) y GH_TOKEN.
# GH permite reemplazar el binario de gh (tests).
set -euo pipefail

event="${GITHUB_EVENT_NAME:?falta GITHUB_EVENT_NAME}"
out="${GITHUB_OUTPUT:-/dev/stdout}"
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
gh_bin="${GH:-gh}"

skip=false
if [ "${event}" = "schedule" ]; then
  ref="${GITHUB_WORKFLOW_REF:?falta GITHUB_WORKFLOW_REF}"
  workflow="$(basename "${ref%@*}")"
  run_id="${GITHUB_RUN_ID:?falta GITHUB_RUN_ID}"
  today="$(date -u +%F)"
  query="[.[] | select(.databaseId != ${run_id})
              | select(.conclusion == \"success\" or .status == \"in_progress\")
              | .databaseId] | map(tostring) | join(\" \")"
  if others=$("${gh_bin}" run list --repo "${GITHUB_REPOSITORY:?falta GITHUB_REPOSITORY}" \
                --workflow "${workflow}" --created ">=${today}" --limit 50 \
                --json databaseId,status,conclusion -q "${query}"); then
    if [ -n "${others}" ]; then
      skip=true
      msg="Cron tardío de ${workflow}: hoy ya corrió (run ${others}). Se saltea."
      echo "${msg}"
      echo "${msg}" >> "${summary}"
    fi
  else
    # Ante la duda, correr: un duplicado es mejor que un día sin datos.
    echo "::warning::no pude consultar los runs de ${workflow}; corro igual"
  fi
fi
echo "skip=${skip}" >> "${out}"
