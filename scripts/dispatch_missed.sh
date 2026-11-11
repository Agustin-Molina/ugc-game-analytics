#!/usr/bin/env bash
# Lanza los workflows diarios cuyo cron no disparó hoy. Lo llama el loop horario de Roblox
# (scripts/hourly_loop.sh) después de cada corrida: el cron de GitHub saltea muchas
# corridas, el loop no.
#
# Un workflow se lanza si pasó su hora + GRACE_MIN (90) y no tiene ningún run creado hoy
# (UTC). El margen evita duplicados si el cron dispara tarde.
#
# Uso: scripts/dispatch_missed.sh   (necesita GH_TOKEN y GITHUB_REPOSITORY)
set -uo pipefail

grace_min="${GRACE_MIN:-90}"
repo="${GITHUB_REPOSITORY:?falta GITHUB_REPOSITORY}"
ref="${GITHUB_REF_NAME:-main}"
today="$(date -u +%F)"
now_min=$(( 10#$(date -u +%H) * 60 + 10#$(date -u +%M) ))

while read -r workflow hhmm; do
  [ -z "${workflow}" ] && continue
  due=$(( 10#${hhmm%:*} * 60 + 10#${hhmm#*:} + grace_min ))
  if (( now_min < due )); then
    continue
  fi
  if ! n=$(gh run list --repo "${repo}" --workflow "${workflow}" --created ">=${today}" \
             --json databaseId -q length); then
    echo "::warning::no pude consultar los runs de ${workflow}"
    continue
  fi
  if [ "${n}" = "0" ]; then
    echo "El cron de ${workflow} (${hhmm} UTC) no disparó hoy: lo lanzo."
    gh workflow run "${workflow}" --repo "${repo}" --ref "${ref}" \
      || echo "::warning::no pude lanzar ${workflow}"
  fi
done <<'EOF'
fortnite_daily.yml 03:37
roblox_search.yml 06:23
quality_daily.yml 12:53
EOF
