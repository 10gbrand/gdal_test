## detta ära filen dlt_pipeline/giss/export_oracle_giss_paralell.py

import dlt
import pandas as pd
import os
from dotenv import load_dotenv, dotenv_values
import logging
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import inspect
import cx_Oracle
from multiprocessing import Pool, cpu_count

# -------------------------------------------------------------
# FILSYSTEM / PARQUET DESTINATION
# -------------------------------------------------------------
PARQUET_DIR = "./data/dlt_output/giss_all"
os.makedirs(PARQUET_DIR, exist_ok=True)

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
# HÄMTA ENV
# -------------------------------------------------------------

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
config = dotenv_values(dotenv_path)  # läser filen som en dict

# Filen ligger i samma katalog som scriptet
csv_path = os.path.join(os.path.dirname(__file__), "WANTED_TABLES.csv")

# Läs CSV, en kolumn med tabellnamn, t.ex. kolumnnamn "TABLE"
df = pd.read_csv(csv_path)

# Konvertera till lista med versaler och utan tomma strängar
wanted_tables = [t.strip().upper() for t in df['TABLE'].tolist() if t.strip()]

print("💡 Filtrerade tabeller från CSV:", wanted_tables)

# Hämta DB-parametrar
host = config.get("HOST")
port = config.get("PORT")
service_name = config.get("SERVICE_NAME")
username = config.get("USERNAME")
password = config.get("PASSWORD")

# print("host: ", host)
# print("port: ", port)
# print("service_name: ", service_name)
# print("username: ", username)
# print("password: ", password)


# -------------------------------------------------------------
# ORACLE KONFIGURATION
# -------------------------------------------------------------

lib_dir = "/home/mate01/github/10gbrand/gdal_test/instantclient/instantclient_19_28"
os.environ["LD_LIBRARY_PATH"] = lib_dir + ":" + os.environ.get("LD_LIBRARY_PATH", "")

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
    ## print(df)
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

def build_select_with_wkt_safe(table, engine, convert_numbers_to_text=True, exclude_columns=None):
    """
    Bygger en SQL SELECT-sats för en tabell där:
      - SDO_GEOMETRY-kolumner konverteras till WKT
      - Problematiska numeriska kolumner (t.ex. med None eller negativa mikrotal) hanteras säkert

    Parametrar:
        table (str): Namn på tabellen.
        engine: SQLAlchemy engine.
        convert_numbers_to_text (bool): Om True konverteras alla NUMBER/FLOAT-kolumner till text (TO_CHAR)
                                        för att undvika ORA-22063.
        exclude_columns (list[str]): Lista på kolumner som ska ignoreras vid SELECT.

    Returnerar:
        str: SQL SELECT-sats.
    """

    # Kolumner som ofta orsakar ORA-22063 i GISS.GAVD
    problematic_null_cols = ["NR1", "KEDJAAKTIV", "HISTIMP", "NVBID"]
    exclude_columns = (exclude_columns or []) + ["SE_ANNO_CAD_DATA"]

    query_cols = f"""
        SELECT column_name, data_type
        FROM all_tab_columns
        WHERE owner = 'GISS' AND table_name = '{table}'
    """
    df_cols = pd.read_sql(query_cols, con=engine)
    df_cols.columns = [c.upper() for c in df_cols.columns]

    select_cols = []
    for _, row in df_cols.iterrows():
        col_name = row["COLUMN_NAME"]
        data_type = row["DATA_TYPE"]

        if col_name in exclude_columns:
            continue

        # 1️⃣ Geometrikolumner → WKT
        if data_type == "SDO_GEOMETRY":
            select_cols.append(f"SDO_UTIL.TO_WKTGEOMETRY({col_name}) AS {col_name}_wkt")

        # 2️⃣ Problematiska null-kolumner → ersätt None med text
        elif col_name in problematic_null_cols:
            select_cols.append(f"NVL(TO_CHAR({col_name}), 'NULL') AS {col_name}")

        # 3️⃣ Numeriska kolumner → konvertera till text för säkerhets skull
        elif convert_numbers_to_text and data_type in ("NUMBER", "FLOAT", "DECIMAL"):
            # Säkerhetsfilter för små/negativa tal som kan ge ORA-22063
            select_cols.append(
                f"TO_CHAR(CASE WHEN {col_name} < -1e-6 THEN NULL ELSE {col_name} END) AS {col_name}"
            )

        # 4️⃣ Allt annat → ta som det är
        else:
            select_cols.append(col_name)

    select_clause = ", ".join(select_cols)
    sql = f"SELECT {select_clause} FROM giss.{table}"
    return sql


# -------------------------------------------------------------
# EXPORTFUNKTION FÖR EN TABELL (multiprocess-trådsäker)
# -------------------------------------------------------------
def export_table(table, convert_numbers_to_text=True, exclude_columns=None):
    """
    Exporterar en tabell från Oracle till Parquet, med robust hantering av problematiska kolumner.

    Parametrar:
        table (str): Namn på tabellen som ska exporteras.
        convert_numbers_to_text (bool): Om True konverteras numeriska kolumner till text för att undvika ORA-22063.
        exclude_columns (list[str]): Lista på kolumner som ska ignoreras vid export.

    Returnerar:
        tuple: (table, status, info)
               status = "ok" eller "error"
               info = antal rader eller felmeddelande
    """
    try:
        engine = create_engine(oracle_connection_string)

        print_with_time(f"🚀 Start export: {table}")
        logging.info(f"Start export: {table}")

        sql = build_select_with_wkt_safe(
            table,
            engine,
            convert_numbers_to_text=convert_numbers_to_text,
            exclude_columns=exclude_columns,
        )

        df = pd.read_sql(sql, con=engine)
        df = convert_lob_columns(df)

        print_with_time(f"✅ {table}: {len(df)} rader hämtade")
        logging.info(f"Export av {table} lyckades ({len(df)} rader).")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_path = os.path.join(PARQUET_DIR, f"{table.lower()}_{timestamp}.parquet")

        df.to_parquet(parquet_path, index=False)
        print_with_time(f"💾 {table}: sparad till {parquet_path}")

        return (table, "ok", len(df))

    except Exception as e:
        logging.error(f"Fel vid export av {table}: {e}")
        print_with_time(f"⚠️ Fel vid export av {table}: {e}")
        return (table, "error", str(e))

    finally:
        # 👇 stänger alla connections till Oracle
        try:
            engine.dispose()
        except:
            pass


# -------------------------------------------------------------
# HUVUDLOOP – MULTIPROCESS
# -------------------------------------------------------------
def main():
    print_with_time("📡 Hämtar tabellista från Oracle...")
    tables = get_table_names(engine)
    print_with_time(f"Totalt {len(tables)} tabeller hittade i GISS-schema.")

    # Filtrera mot de tabeller som ska exporteras
    filtered_tables = [t for t in tables if t in wanted_tables]
    print_with_time(f"💡 Filtrerade tabeller: {filtered_tables}")

    # Lista kolumner att utesluta
    exclude_columns = ["SE_ANNO_CAD_DATA", "FIGADVA", "FIGNETTO", "NR1", "KEDJAAKTIV", "HISTIMP", "NVBID"]

    # Multiprocess-export
    n_processes = min(cpu_count(), 4)
    with Pool(processes=n_processes) as pool:
        # Använder starmap för att skicka flera argument
        results = pool.starmap(
            export_table,
            [(table, True, exclude_columns) for table in filtered_tables]
        )

    print_with_time("✅ Alla parallella jobb klara.")
    for t, status, info in results:
        print(f"  - {t}: {status} ({info})")

    print_with_time("🎉 Export från Oracle GISS schema klar!")


# -------------------------------------------------------------
if __name__ == "__main__":
    main()
