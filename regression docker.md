1) Create a local .env
Compose will auto-read this file in the same folder as docker-compose.yml.
# in the same directory as docker-compose.yml
cat > .env <<'EOF'
MSSQL_SA_PASSWORD=YourStrong!Passw0rd   # change to your strong password
DROP_TABLE=false
EOF
If you already have one from CI, just reuse it.
2) (Optional) Build the regression image
If your compose file defines regression-runner with a build: block, Compose will build it for you.
If you use a custom Dockerfile (e.g., tests/RegressionRunner.Dockerfile) and want to prebuild:
docker build \
  -f tests/RegressionRunner.Dockerfile \
  -t tradestore/regression:latest \
  .
If your regression-runner service uses image: tradestore/regression:latest, this prebuild makes the first run faster.
3) Start the full stack with the regression profile
From the directory that has docker-compose.yml:
# Option A: one-shot with profile + build
docker compose --profile regression up -d --build \
  redpanda redpanda-init mongo mssql mssql-init \
  tradedata kafkaconsumer kafkaconsumermssql \
  regression-runner
If you prefer environment variable style:
# Option B: via env var
export COMPOSE_PROFILES=regression
docker compose up -d --build \
  redpanda redpanda-init mongo mssql mssql-init \
  tradedata kafkaconsumer kafkaconsumermssql \
  regression-runner
If you keep a CI override file (e.g., docker-compose.override.ci.yml) for subfolder paths, you can include it locally too:
docker compose -f docker-compose.yml -f docker-compose.override.ci.yml --profile regression up -d --build ...
4) Watch it run
Tail logs for a quick health check:
docker compose logs -f redpanda mongo mssql mssql-init tradedata kafkaconsumer kafkaconsumermssql regression-runner
Typical success signs:
redpanda → “healthy”
mongo → “healthy”
mssql → “healthy”
mssql-init → prints “mssql-init complete.”
regression-runner → prints your regression summary and exits (or keeps running, depending on how you coded it)
5) Run regression on demand (ephemeral)
If your regression-runner is a short job and you want to trigger it again:
# re-run the runner only (removes container after it exits)
docker compose run --rm regression-runner
6) Inspect results
App status:
docker compose ps
Container logs (last 10 minutes):
docker compose logs --since 10m
Mongo quick check:
docker exec -it $(docker compose ps -q mongo) \
  mongosh --quiet --eval "db.getSiblingDB('trade_store').trades.countDocuments()"
MSSQL quick check:
docker exec -it $(docker compose ps -q mssql) \
  /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT COUNT(*) FROM dbo.trades;"
7) Tear down
docker compose --profile regression down -v
