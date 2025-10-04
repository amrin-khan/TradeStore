#!/bin/bash
set -euo pipefail

# Inherit container env for cron-launched runs
if [ -r /proc/1/environ ]; then
  while IFS='=' read -r k v; do
    case "$k" in MSSQL_SA_PASSWORD|MSSQL_DB|TZ|DROP_TABLE) export "$k=$v";; esac
  done < <(tr '\0' '\n' < /proc/1/environ)
fi
[ -f /etc/environment ] && { set -a; . /etc/environment; set +a; }

: "${MSSQL_SA_PASSWORD:?MSSQL_SA_PASSWORD not set}"
DB="${MSSQL_DB:-master}"            # <<< default to master

SQLCMD='/opt/mssql-tools/bin/sqlcmd'
S="mssql,1433"
U="sa"
P="$MSSQL_SA_PASSWORD"

# (Optional) heartbeat
echo "$(date -u +'%F %T UTC') running mark_expired.sh against DB=$DB" >> /var/log/cron.log

# Only run the UPDATE if dbo.trades exists
$SQLCMD -S "$S" -U "$U" -P "$P" -C -d "$DB" -b -Q "
IF OBJECT_ID(N'dbo.trades', N'U') IS NOT NULL
BEGIN
  UPDATE dbo.trades
  SET expired = 1
  WHERE maturity_date < CAST(GETDATE() AS date) AND expired = 0;
END
ELSE
BEGIN
  PRINT 'dbo.trades not found in ' + DB_NAME();
END
"