import pyodbc
from tabulate import tabulate  

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
print("Connected (host)!")
cursor = conn.cursor()
cursor.execute("SELECT @@VERSION")
print(cursor.fetchall())

# "Database=trade_store;"

# #####
# cursor.execute("""
# IF OBJECT_ID('dbo.trades') IS NOT NULL
# BEGIN
#   DROP TABLE dbo.trades;
# END
               
#   CREATE TABLE dbo.trades(
#     trade_id        varchar(50)  NOT NULL,
#     counterparty_id varchar(50)  NOT NULL,
#     book_id         varchar(50)  NOT NULL,
#     [version]       int          NOT NULL,
#     maturity_date   date         NULL,
#     created_date    date         NULL,
#     expired         bit          NOT NULL,
#     CONSTRAINT PK_trades PRIMARY KEY (trade_id,version,book_id)
#   );

# """)
# conn.commit()
# print("Table ensured")



##### -- update expired flag starts
# cursor.execute("""
# update dbo.trades
# set expired = 1
# where maturity_date < CAST(GETDATE() AS date) and expired = 0
# """)
# conn.commit()
# print("Table ensured")
##### -- update expired flag starts

#######delete table

# # Option 1: truncate
# cursor.execute("TRUNCATE TABLE dbo.trades;")

# # Option 2: delete all
# cursor.execute("DELETE FROM dbo.trades;")

# conn.commit()

# cursor.close()
# conn.close()
#######delete table


# query ------ 
def query_as_dicts(sql, params=None):
    conn = pyodbc.connect(
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=127.0.0.1,1433;"
        "Database=master;"
        "UID=sa;"
        "PWD=Str0ng!Passw0rd;"
        "Encrypt=Yes;"
        "TrustServerCertificate=Yes;"
    )    
    cursor = conn.cursor()
    cursor.execute(sql, params or [])
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    print(tabulate(rows, headers=columns, tablefmt="psql"))
    # result = [dict(zip(columns, row)) for row in cursor.fetchall()]
    # cursor.execute("DELETE FROM dbo.trades")
    # conn.commit()

    cursor.close()
    conn.close()
    # return result

# rows = query_as_dicts("SELECT TOP 5 * FROM dbo.trades")
query_as_dicts("SELECT * FROM dbo.trades")
# print(rows)
query_as_dicts("SELECT count(*) as totalrec FROM dbo.trades")


query_as_dicts("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'trades' AND TABLE_SCHEMA = 'dbo';")
query_as_dicts("SELECT k.COLUMN_NAME, tc.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME WHERE tc.TABLE_NAME = 'trades' AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY';")
# print(rows)
