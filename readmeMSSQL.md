docker run -d --name mssql \
  -e "ACCEPT_EULA=Y" \
  -e "SA_PASSWORD=Str0ngP@ssw0rd!" \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest

  Using SQL inside the container 
docker exec -it mssql /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U SA -P 'YourStrong!Passw0rd'

