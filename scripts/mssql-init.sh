#!/usr/bin/env bash
set -euo pipefail

echo "Using /opt/mssql-tools/bin/sqlcmd"

# Wait for SQL Server
for i in {1..30}; do
  /opt/mssql-tools/bin/sqlcmd -S mssql,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "SELECT 1" -b && break || sleep 2
done

# Drop table if requested
if [ "${DROP_TABLE:-false}" = "true" ]; then
  /opt/mssql-tools/bin/sqlcmd -S mssql,1433 -U sa -P "$MSSQL_SA_PASSWORD" -d master -C -Q "
    IF OBJECT_ID('dbo.trades','U') IS NOT NULL DROP TABLE dbo.trades;
  "
fi

# Create table if not exists
/opt/mssql-tools/bin/sqlcmd -S mssql,1433 -U sa -P "$MSSQL_SA_PASSWORD" -d master -C -Q "
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
"
