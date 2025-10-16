import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import dotenv_values

# -------------------------------------------------------------
# HÄMTA ENV
# -------------------------------------------------------------
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
config = dotenv_values(dotenv_path)  # läser filen som en dict

# Hämta DB-parametrar
host = config.get("HOST")
port = config.get("PORT")
service_name = config.get("SERVICE_NAME")
username = config.get("USERNAME")
password = config.get("PASSWORD")

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
# Kontrollera NUMBER/SDO_GEOMETRY-problem i GAVD
# -------------------------------------------------------------
table = "GAVD"
exclude_column = "SE_ANNO_CAD_DATA"  # kolumn att ignorera

# Hämta kolumner av intresse
query_cols = f"""
SELECT column_name, data_type
FROM all_tab_columns
WHERE owner = 'GISS'
  AND table_name = '{table}'
  AND data_type IN ('NUMBER','FLOAT','DECIMAL')
  AND column_name <> '{exclude_column}'
"""
df_cols = pd.read_sql(query_cols, con=engine)
df_cols.columns = [c.upper() for c in df_cols.columns]  # säkerställ versaler
numeric_cols = df_cols['COLUMN_NAME'].tolist()

print("💡 Numeriska kolumner som kontrolleras:", numeric_cols)

# Loopa igenom kolumner och kolla för problem med negativa/OVF-värden
for col in numeric_cols:
    query = f"SELECT {col} FROM giss.{table} WHERE {col} < 0 OR {col} IS NULL"
    try:
        df_check = pd.read_sql(query, con=engine)
        if not df_check.empty:
            print(f"⚠️ Problem hittades i kolumn {col}:")
            print(df_check.head())
        else:
            print(f"✅ Kolumn {col} ser ok ut.")
    except Exception as e:
        print(f"❌ Fel vid kontroll av kolumn {col}: {e}")

