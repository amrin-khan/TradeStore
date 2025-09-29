(Optional) Build the regression image
docker build \
  -f tests/RegressionRunner.Dockerfile \
  -t tradestore/regression:latest \
  .

docker compose --profile regression up -d --build \
  redpanda redpanda-init mongo mssql mssql-init \
  tradedata kafkaconsumer kafkaconsumermssql \
  regression-runner

docker compose logs -f redpanda mongo mssql mssql-init tradedata kafkaconsumer kafkaconsumermssql regression-runner


docker compose run --rm regression-runner


Mongo quick check:
docker exec -it $(docker compose ps -q mongo) \
  mongosh --quiet --eval "db.getSiblingDB('trade_store').trades.countDocuments()"

MSSQL quick check:
docker exec -it $(docker compose ps -q mssql) \
  /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT COUNT(*) FROM dbo.trades;"

Tear down
docker compose --profile regression down -v
