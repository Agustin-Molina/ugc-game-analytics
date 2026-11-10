#!/usr/bin/env bash
# Commitea y pushea los datos nuevos al repo de datos (checkout en $1).
# Uso: scripts/commit_data.sh <dir_repo_datos> <etiqueta>
set -euo pipefail

dir="$1"
label="$2"
cd "$dir"

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

git add -A
if git diff --cached --quiet; then
  echo "Sin cambios en los datos."
  exit 0
fi

git commit -q -m "data(${label}): $(date -u +%Y-%m-%dT%H:%M:%SZ)"
branch="$(git rev-parse --abbrev-ref HEAD)"

for attempt in 1 2 3 4 5; do
  if git push -q origin "HEAD:${branch}"; then
    echo "Push OK (intento ${attempt})."
    exit 0
  fi
  echo "Push rechazado; rebase y reintento (${attempt}/5)…"
  sleep $(( attempt * 5 + RANDOM % 10 ))
  git pull -q --rebase origin "${branch}"
done

echo "No se pudo pushear tras 5 intentos." >&2
exit 1
