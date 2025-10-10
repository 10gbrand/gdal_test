import os
import oracledb
import dlt
from dlt.sources.sql_database import sql_database
from dotenv import load_dotenv

# Viktigt! Aktivera thick mode före DLT körs
oracledb.init_oracle_client(lib_dir="/home/mate01/github/10gbrand/gdal_test/instantclient/instantclient_19_28")

load_dotenv()

host = os.getenv("HOST")
port = os.getenv("PORT")
service_name = os.getenv("SERVICE_NAME")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

# Bygg SQLAlchemy URL som DLT accepterar
#connection_string = f"oracle+oracledb://{username}:{password}@?dsn=//{host}:{port}/{service_name}"

tables = ["GISS.GAVD", "GISS.GPATGAVV"]

def load_tables_giss(tables):
    
    source = sql_database(
        credentials={
            "drivername": "oracle+oracledb",
            "username": username,
            "password": password,
            "host": host,
            "port": port,
            "database": service_name  # för Oracle, service_name används ofta som database
        }
    ).with_resources(*tables)

    pipeline = dlt.pipeline(
        pipeline_name="oracle_to_duckdb_pipeline",
        destination="duckdb",
        dataset_name="oracle_to_duckdb_data"
    )

    load_info = pipeline.run(source)
    print("✅ Pipeline körd!")
    print(load_info)

if __name__ == "__main__":
    load_tables_giss(tables)

