docker rm -f mssql 2>/dev/null || true
docker run -d \
  --name mssql \
  --platform=linux/amd64 \
  -e ACCEPT_EULA=Y \
  -e MSSQL_PID=Developer \
  -e SA_PASSWORD='Str0ng!Passw0rd' \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest




  conn = pyodbc.connect(
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=127.0.0.1,1433;"      # because we mapped -p 1433:1433
    "Database=master;"       # or 'master' on first run
    "UID=sa;"
    "PWD=Str0ng!Passw0rd;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;" # dev-only
    "Connection Timeout=5;"
)

