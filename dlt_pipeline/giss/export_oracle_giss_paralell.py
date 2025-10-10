import dlt
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
from sqlalchemy import create_engine
import cx_Oracle
from multiprocessing import Pool, cpu_count

# -------------------------------------------------------------
# LOGGNING & HJÄLPFUNKTIONER
# -------------------------------------------------------------
logging.basicConfig(
    filename="oracle_export.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def print_with_time(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# -------------------------------------------------------------
# ORACLE KONFIGURATION
# -------------------------------------------------------------
lib_dir = "/home/mate01/github/10gbrand/gdal_test/instantclient/instantclient_19_28"
os.environ["LD_LIBRARY_PATH"] = lib_dir + ":" + os.environ.get("LD_LIBRARY_PATH", "")

load_dotenv()

host = os.getenv("HOST")
port = os.getenv("PORT")
service_name = os.getenv("SERVICE_NAME")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

oracle_connection_string = (
    f"oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service_name}"
)
engine = create_engine(oracle_connection_string)

# -------------------------------------------------------------
# HJÄLPFUNKTIONER FÖR ORACLE
# -------------------------------------------------------------
def get_table_names(conn):
    query = "SELECT table_name FROM all_tables WHERE owner = 'GISS'"
    df = pd.read_sql(query, con=conn)
    df.columns = [c.upper() for c in df.columns]
    return df['TABLE_NAME'].tolist()

def get_geometry_columns(conn, table):
    query = f"""
        SELECT column_name FROM all_tab_columns
        WHERE owner = 'GISS' AND table_name = '{table}'
        AND data_type = 'SDO_GEOMETRY'
    """
    df = pd.read_sql(query, con=conn)
    df.columns = [c.upper() for c in df.columns]
    return df['COLUMN_NAME'].tolist()

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
    df = pd.read_sql(query_cols, con=conn)
    df.columns = [c.upper() for c in df.columns]
    all_cols = df['COLUMN_NAME'].tolist()

    select_cols = []
    for col in all_cols:
        if col in geom_cols:
            select_cols.append(f"SDO_UTIL.TO_WKTGEOMETRY({col}) AS {col}_wkt")
        else:
            select_cols.append(col)

    select_clause = ", ".join(select_cols)
    sql = f"SELECT {select_clause} FROM giss.{table}"
    return sql

# -------------------------------------------------------------
# FILSYSTEM / PARQUET DESTINATION
# -------------------------------------------------------------
PARQUET_DIR = "./data/dlt_output/giss_all"
os.makedirs(PARQUET_DIR, exist_ok=True)

# -------------------------------------------------------------
# EXPORTFUNKTION FÖR EN TABELL (multiprocess-trådsäker)
# -------------------------------------------------------------
def export_table(table):
    try:
        print_with_time(f"🚀 Start export: {table}")
        logging.info(f"Start export: {table}")

        geom_cols = get_geometry_columns(engine, table)
        sql = build_select_with_wkt(table, geom_cols, engine)
        df = pd.read_sql(sql, con=engine)
        df = convert_lob_columns(df)

        print_with_time(f"✅ {table}: {len(df)} rader hämtade")
        logging.info(f"Export av {table} lyckades ({len(df)} rader).")

        # Skapa filnamn
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_path = os.path.join(PARQUET_DIR, f"{table.lower()}_{timestamp}.parquet")

        # Skriv till Parquet direkt
        df.to_parquet(parquet_path, index=False)
        print_with_time(f"💾 {table}: sparad till {parquet_path}")

        return (table, "ok", len(df))
    except Exception as e:
        logging.error(f"Fel vid export av {table}: {e}")
        print_with_time(f"⚠️ Fel vid export av {table}: {e}")
        return (table, "error", str(e))

# -------------------------------------------------------------
# HUVUDLOOP – MULTIPROCESS
# -------------------------------------------------------------
def main():
    print_with_time("📡 Hämtar tabellista från Oracle...")
    tables = get_table_names(engine)
    print_with_time(f"Totalt {len(tables)} tabeller hittade i GISS-schema.")

    results = []
    n_processes = min(cpu_count(), 4)
    with Pool(processes=n_processes) as pool:
        results = pool.map(export_table, tables)

    print_with_time("✅ Alla parallella jobb klara.")
    for t, status, info in results:
        print(f"  - {t}: {status} ({info})")

    print_with_time("🎉 Export från Oracle GISS schema klar!")

# -------------------------------------------------------------
if __name__ == "__main__":
    main()
