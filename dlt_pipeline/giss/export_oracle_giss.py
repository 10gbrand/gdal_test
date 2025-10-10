import dlt
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
from sqlalchemy import create_engine
import cx_Oracle  # används fortfarande för LOB-hantering

# Konfigurera loggning
logging.basicConfig(
    filename="oracle_export.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def print_with_time(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# Initiera Oracle Instant Client via LD_LIBRARY_PATH (om behövs)
lib_dir = "/home/mate01/github/10gbrand/gdal_test/instantclient/instantclient_19_28"
os.environ["LD_LIBRARY_PATH"] = lib_dir + ":" + os.environ.get("LD_LIBRARY_PATH", "")

load_dotenv()  # Läser in .env-filen

host = os.getenv("HOST")
port = os.getenv("PORT")
service_name = os.getenv("SERVICE_NAME")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

# Bygg SQLAlchemy engine-string för cx_Oracle
oracle_connection_string = (
    f"oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service_name}"
)
engine = create_engine(oracle_connection_string)

def get_table_names(conn):
    query = "SELECT table_name FROM all_tables WHERE owner = 'GISS'"
    return pd.read_sql(query, con=conn)["TABLE_NAME"].tolist()

def get_geometry_columns(conn, table):
    query = f"""
        SELECT column_name FROM all_tab_columns
        WHERE owner = 'GISS' AND table_name = '{table}'
        AND data_type = 'SDO_GEOMETRY'
    """
    return pd.read_sql(query, con=conn)["COLUMN_NAME"].tolist()

def convert_lob_columns(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, cx_Oracle.LOB)).any():
            df[col] = df[col].apply(lambda x: x.read() if x is not None else None)
    return df

def build_select_with_wkt(table, geom_cols, conn):
    query_cols = f"""
        SELECT column_name FROM all_tab_columns
        WHERE owner = 'GISS' AND table_name = '{table}'
    """
    all_cols = pd.read_sql(query_cols, con=conn)["COLUMN_NAME"].tolist()
    
    select_cols = []
    for col in all_cols:
        if col in geom_cols:
            select_cols.append(f"SDO_UTIL.TO_WKTGEOMETRY({col}) AS {col}_wkt")
        else:
            select_cols.append(col)
    
    select_clause = ", ".join(select_cols)
    sql = f"SELECT {select_clause} FROM giss.{table}"
    return sql

# dlt pipeline konfiguration
pipeline = dlt.pipeline(
    pipeline_name="oracle_giss_export",
    destination="duckdb",
    dataset_name="oracle_giss",
)

def oracle_giss_tables():
    tables = get_table_names(engine)
    for table in tables:
        logging.info(f"Startar export av tabell: {table}")
        print_with_time(f"Start export: {table}")
        try:
            geom_cols = get_geometry_columns(engine, table)
            sql = build_select_with_wkt(table, geom_cols, engine)
            df = pd.read_sql(sql, con=engine)

            df = convert_lob_columns(df)

            logging.info(f"Export av tabell {table} lyckades med {len(df)} rader.")
            print_with_time(f"{len(df)} rader exporterade från tabell: {table}")

            yield {
                "table_name": table.lower(),
                "data": df.to_dict(orient="records"),
            }

        except Exception as e:
            logging.error(f"Fel vid export av tabell {table}: {e}")
            print_with_time(f"⚠️  Fel vid export av tabell {table}: {e}")

        print_with_time(f"End export: {table}")
        logging.info(f"Avslutar export av tabell: {table}")

for table_info in oracle_giss_tables():
    pipeline.run(table_info["data"], table_name=table_info["table_name"])

print("Export från Oracle GISS schema klar.")

