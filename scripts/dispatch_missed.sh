#!/usr/bin/env bash
# Lanza los workflows diarios cuyo cron no disparó hoy. Lo llama el loop horario de Roblox
# (scripts/hourly_loop.sh) después de cada corrida: el cron de GitHub saltea muchas
# corridas, el loop no.
#
# Un workflow se lanza si pasó su hora + GRACE_MIN (90) y no tiene ningún run creado hoy
# (UTC). Si el cron dispara tarde igual, el job `guard` de cada workflow
# (scripts/ran_today.sh) saltea esa segunda corrida. Los semanales llevan el día
# de la semana (0 = domingo, como `date +%w`) y solo se miran ese día.
#
# Uso: scripts/dispatch_missed.sh   (necesita GH_TOKEN y GITHUB_REPOSITORY)
set -uo pipefail

grace_min="${GRACE_MIN:-90}"
repo="${GITHUB_REPOSITORY:?falta GITHUB_REPOSITORY}"
ref="${GITHUB_REF_NAME:-main}"
today="$(date -u +%F)"
now_min=$(( 10#$(date -u +%H) * 60 + 10#$(date -u +%M) ))
weekday="$(date -u +%w)"

while read -r workflow hhmm dow; do
  [ -z "${workflow}" ] && continue
  if [ -n "${dow}" ] && [ "${dow}" != "${weekday}" ]; then
    continue
  fi
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
roblox_assets.yml 08:11
roblox_creators.yml 09:41
quality_daily.yml 12:53
compact_weekly.yml 02:47 0
EOF
