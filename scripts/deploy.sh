#!/usr/bin/env bash
# Deploys the latest code on the pipeline server. GitHub Actions runs this over
# SSH after CI passes on main:
#   cd ~/weather-seismic-pipeline && OLD=$(git rev-parse HEAD) \
#     && git pull --ff-only && bash scripts/deploy.sh "$OLD"
# The argument is the commit the server was on before the pull, so the script
# can tell which parts changed.
set -euo pipefail
cd "$(dirname "$0")/.."

OLD_COMMIT="${1:?usage: deploy.sh <commit before pull>}"
CHANGED="$(git diff --name-only "$OLD_COMMIT" HEAD)"
echo "Deploying $(git rev-parse --short HEAD) (was $(git rev-parse --short "$OLD_COMMIT"))"
echo "Changed files:"
echo "${CHANGED:-  (none)}" | sed 's/^/  /'

# Rebuild images whose source changed (producer, api, web, airflow) and
# recreate only containers whose image or configuration changed; everything
# else keeps running untouched.
docker compose up -d --build

# The Spark jobs' code is mounted from phase2/, not built into an image, so
# Docker can't see it change: restart them when it does. They resume from
# their checkpoints.
if grep -q '^phase2/' <<< "$CHANGED"; then
  echo "Spark job code changed: restarting the streaming jobs"
  docker compose restart weather-stream seismic-stream
fi

# --- Smoke test ---
# Every long-running service must be up...
for service in kafka producer weather-stream seismic-stream api web airflow; do
  if [ -z "$(docker compose ps -q --status running "$service")" ]; then
    echo "FAILED: $service is not running"
    docker compose logs --tail 40 "$service"
    exit 1
  fi
done

# ...and the API must answer through the internal network (up to 60 s, since
# a recreated container needs a moment to start).
for _ in $(seq 1 30); do
  if docker compose exec -T web wget -qO- http://api:8000/api/health > /dev/null 2>&1; then
    echo "Deploy OK: all services running, API healthy"
    exit 0
  fi
  sleep 2
done
echo "FAILED: API did not become healthy"
docker compose logs --tail 40 api
exit 1
