import oracledb
import os
from dotenv import load_dotenv

# ✅ Viktigt: Aktivera thick mode först
oracledb.init_oracle_client(
    lib_dir="/home/mate01/github/10gbrand/gdal_test/instantclient/instantclient_19_28"
)

load_dotenv()

conn = oracledb.connect(
    user=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    dsn=f"{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('SERVICE_NAME')}"
)

print("Using thick mode:", not oracledb.is_thin_mode())

cur = conn.cursor()

# Testa åtkomst till tabeller i schemat GISS
try:
    cur.execute("SELECT COUNT(*) FROM GISS.GAVD")
    print("✅ GISS.GAVD OK:", cur.fetchone())
except Exception as e:
    print("❌ Fel vid åtkomst till GISS.GAVD:", e)

try:
    cur.execute("SELECT COUNT(*) FROM GISS.GPATGAVV")
    print("✅ GISS.GPATGAVV OK:", cur.fetchone())
except Exception as e:
    print("❌ Fel vid åtkomst till GISS.GPATGAVV:", e)

cur.close()
conn.close()

