import dlt
import cx_Oracle
import pandas as pd
import os
from dotenv import load_dotenv

lib_dir = "/home/mate01/github/10gbrand/gdal_test/instantclient/instantclient_19_28"
os.environ["LD_LIBRARY_PATH"] = lib_dir + ":" + os.environ.get("LD_LIBRARY_PATH", "")
cx_Oracle.init_oracle_client(lib_dir=lib_dir)

load_dotenv()  # Läser in .env-filen

host = os.getenv("HOST")
port = os.getenv("PORT")
service_name = os.getenv("SERVICE_NAME")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)

# Funktion för att hämta alla tabeller i schemat "GISS"
def get_table_names(conn):
    query = "SELECT table_name FROM all_tables WHERE owner = 'GISS'"
    return pd.read_sql(query, con=conn)["TABLE_NAME"].tolist()

# Funktion för att hämta kolumner av typen SDO_GEOMETRY i en tabell
def get_geometry_columns(conn, table_name):
    query = f"""
    SELECT column_name FROM all_tab_columns
    WHERE owner = 'GISS' AND table_name = '{table_name}' AND data_type = 'SDO_GEOMETRY'
    """
    return pd.read_sql(query, con=conn)["COLUMN_NAME"].tolist()

# Funktion för att bygga SQL-fråga som konverterar geometry-kolumner till WKT
def build_select_with_wkt(table_name, geom_columns, conn):
    # Hämta alla kolumner från tabellen
    query_cols = f"""
    SELECT column_name FROM all_tab_columns
    WHERE owner = 'GISS' AND table_name = '{table_name}'
    ORDER BY column_id
    """
    all_cols = pd.read_sql(query_cols, con=conn)["COLUMN_NAME"].tolist()

    select_parts = []
    for col in all_cols:
        if col in geom_columns:
            # Byt ut geometry-kolumn till SDO_UTIL.TO_WKTGEOMETRY(col) med alias
            select_parts.append(f"SDO_UTIL.TO_WKTGEOMETRY({col}) AS {col}_wkt")
        else:
            select_parts.append(col)

    select_clause = ", ".join(select_parts)
    sql = f"SELECT {select_clause} FROM giss.{table_name}"
    return sql

# dlt pipeline konfiguration
pipeline = dlt.pipeline(
    pipeline_name="oracle_giss_export",
    destination="duckdb",
    dataset_name="oracle_giss",
)

# Resursfunktion som laddar in varje tabell från Oracle som pandas DataFrame och yieldar för dlt
def oracle_giss_tables():
    tables = get_table_names(connection)
    for table in tables:
        print(f'Start export: {table}')
        geom_cols = get_geometry_columns(connection, table)
        sql = build_select_with_wkt(table, geom_cols, connection)
        df = pd.read_sql(sql, con=connection)

        # Om geometrikolumner finns, kan du eventuellt ta bort originalkolumner med SDO_GEOMETRY om de finns med
        # (Just nu har vi bara WKT-kolumner med _wkt-suffix, originalgeometrierna är inte med i SELECT)
        
        yield {
            "table_name": table.lower(),
            "data": df.to_dict(orient="records"),
        }
        print(f'End export: {table}')

# Kör pipeline för varje tabell - använder dlt resource pipeline.run med dynamiskt tabellnamn
for table_info in oracle_giss_tables():
    pipeline.run(table_info["data"], table_name=table_info["table_name"])

print("Export från Oracle GISS schema klar.")

