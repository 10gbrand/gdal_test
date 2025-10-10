import oracledb
from sqlalchemy import create_engine

# Aktivera thick mode med din Instant Client-path
oracledb.init_oracle_client(lib_dir="/home/mate01/github/10gbrand/gdal_test/instantclient/instantclient_19_28")

engine = create_engine("oracle+oracledb://mate01:mate01@?dsn=//sveaoradbp01.svea.local:1521/GISSPRES")

with engine.connect() as conn:
    print("✅ Oracle connected (Thick mode)")
