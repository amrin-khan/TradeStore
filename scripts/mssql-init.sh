#!/usr/bin/env bash
set -euo pipefail

SQLCMD="/opt/mssql-tools18/bin/sqlcmd"
[[ -x "$SQLCMD" ]] || SQLCMD="/opt/mssql-tools/bin/sqlcmd"
echo "Using $SQLCMD"

PASS="${MSSQL_SA_PASSWORD:-${SA_PASSWORD:-}}"
if [[ -z "$PASS" ]]; then
  echo "ERROR: MSSQL_SA_PASSWORD/SA_PASSWORD not set"; exit 1
fi

echo "Waiting for SQL Server to accept connections..."
for i in {1..40}; do
  if "$SQLCMD" -S mssql,1433 -U sa -P "$PASS" -C -Q "SELECT 1" -b -h -1 -W >/dev/null 2>&1; then
    echo "SQL Server is ready."; break
  fi
  [[ $i -eq 40 ]] && { echo "Timed out waiting for SQL Server"; exit 1; }
  sleep 2
done
sleep 2

# Optional drop
if [[ "${DROP_TABLE:-false}" == "true" ]]; then
  echo "Dropping dbo.trades if exists..."
  "$SQLCMD" -S mssql,1433 -U sa -P "$PASS" -d master -C -b -i /docker-entrypoint-initdb.d/drop.sql < /dev/null
fi

echo "Ensuring dbo.trades exists..."
"$SQLCMD" -S mssql,1433 -U sa -P "$PASS" -d master -C -b -i /docker-entrypoint-initdb.d/schema.sql < /dev/null

echo "mssql-init complete."
