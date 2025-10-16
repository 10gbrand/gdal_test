import os
import oracledb
from dotenv import load_dotenv
from dlt.sources.sql_database import sql_database
import dlt

# ----------------------------------------------------
# 1. Initiera Oracle thick mode
# ----------------------------------------------------
oracledb.init_oracle_client(
    lib_dir="/home/mate01/github/10gbrand/gdal_test/instantclient/instantclient_19_28"
)

# ----------------------------------------------------
# 2. Ladda miljövariabler
# ----------------------------------------------------
load_dotenv()
host = os.getenv("HOST")
port = os.getenv("PORT")
service_name = os.getenv("SERVICE_NAME")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

# ----------------------------------------------------
# 3. Definiera tabeller med SQL
# ----------------------------------------------------
tables_sql = {
    "GAVD": "SELECT * FROM GISS.GAVD",
    "GPATGAVV": "SELECT * FROM GISS.GPATGAVV"
}

# ----------------------------------------------------
# 4. Funktion för att ladda tabeller via DLT
# ----------------------------------------------------
def load_tables_giss(tables_sql):
    # 4.1 Skapa SQL-databasekälla
    source = sql_database(
        credentials={
            "drivername": "oracle+oracledb",
            "username": username,
            "password": password,
            "host": host,
            "port": port,
            "database": service_name
        }
    )

    # 4.2 Lägg till resurser med SQL direkt
    resources = []
    for name, sql in tables_sql.items():
        # I dlt 1.17.0 skickar man en tuple (resursnamn, SQL)
        resources.append((name, sql))

    # Med .with_resources() kan vi skicka namnet + SQL
    source = source.with_resources(*resources)

    # 4.3 Skapa pipeline
    pipeline = dlt.pipeline(
        pipeline_name="oracle_to_duckdb_pipeline",
        destination="duckdb",
        dataset_name="oracle_to_duckdb_data"
    )

    # 4.4 Kör pipeline och skriv ut resultat
    load_info = pipeline.run(source)
    print("✅ Pipeline körd!")
    print(load_info)

# ----------------------------------------------------
# 5. Huvudprogram
# ----------------------------------------------------
if __name__ == "__main__":
    load_tables_giss(tables_sql)

