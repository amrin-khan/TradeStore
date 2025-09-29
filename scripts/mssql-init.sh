#!/usr/bin/env bash
set -euo pipefail

# pick sqlcmd (tools18 first, then legacy)
SQLCMD="/opt/mssql-tools18/bin/sqlcmd"
[[ -x "$SQLCMD" ]] || SQLCMD="/opt/mssql-tools/bin/sqlcmd"
echo "Using $SQLCMD"

# Accept either MSSQL_SA_PASSWORD or SA_PASSWORD from env
PASS="${MSSQL_SA_PASSWORD:-${SA_PASSWORD:-}}"
if [[ -z "$PASS" ]]; then
  echo "ERROR: MSSQL_SA_PASSWORD/SA_PASSWORD not set in container env"
  exit 1
fi

echo "Waiting for SQL Server to accept connections..."
for i in {1..40}; do
  if "$SQLCMD" -S mssql,1433 -U sa -P "$PASS" -C -Q "SELECT 1" -b -h -1 -W >/dev/null 2>&1; then
    echo "SQL Server is ready."
    break
  fi
  [[ $i -eq 40 ]] && { echo "Timed out waiting for SQL Server"; exit 1; }
  sleep 2
done
sleep 2

# Optional drop
if [[ "${DROP_TABLE:-false}" == "true" ]]; then
  echo "Dropping dbo.trades if exists..."
  "$SQLCMD" -S mssql,1433 -U sa -P "$PASS" -d master -C -b <<'SQL'
IF OBJECT_ID('dbo.trades','U') IS NOT NULL DROP TABLE dbo.trades;
SQL
fi

echo "Ensuring dbo.trades exists..."
"$SQLCMD" -S mssql,1433 -U sa -P "$PASS" -d master -C -b <<'SQL'
IF OBJECT_ID('dbo.trades','U') IS NULL
BEGIN
  CREATE TABLE dbo.trades(
    trade_id        varchar(50)  NOT NULL,
    counterparty_id varchar(50)  NOT NULL,
    book_id         varchar(50)  NOT NULL,
    [version]       int          NOT NULL,
    maturity_date   date         NULL,
    created_date    date         NULL,
    expired         bit          NOT NULL,
    CONSTRAINT PK_trades PRIMARY KEY (trade_id,[version],book_id)
  );
END
SQL

echo "mssql-init complete."
