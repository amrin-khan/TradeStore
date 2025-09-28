# Uses Microsoft sqlcmd tools on Debian and adds cron
FROM mcr.microsoft.com/mssql-tools:latest

# Add cron
RUN apt-get update && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

# Default crontab: run daily at 02:15 UTC
# -C = Trust server cert, -b = fail on error, -Q = query
RUN printf '15 2 * * * /opt/mssql-tools/bin/sqlcmd -S mssql,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -b -Q "UPDATE dbo.trades SET expired=1 WHERE maturity_date < CONVERT(date, GETDATE()) AND expired=0;" >> /var/log/cron.log 2>&1\n' \
    > /etc/cron.d/expiry && chmod 0644 /etc/cron.d/expiry && crontab /etc/cron.d/expiry

# Run cron in foreground so the container stays up
CMD ["bash","-lc","touch /var/log/cron.log && cron -f"]
