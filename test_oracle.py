import os
import oracledb  # eller import cx_Oracle

try:
    # Initiera Oracle klientbibliotek explicit om du behöver Thick mode
    oracle_home = os.environ.get("ORACLE_HOME")
    print(f'Oracle_home: {oracle_home}')
    if oracle_home:
        oracledb.init_oracle_client(lib_dir=oracle_home)
    else:
        print("ORACLE_HOME environment variable is not set")
    # or with cx_Oracle:
    # import cx_Oracle
    # cx_Oracle.init_oracle_client(lib_dir="/full/path/till/instantclient_19_28")

    print("Oracle client library loaded successfully!")

    # Skriv ut version för att bekräfta
    print("oracledb version:", oracledb.__version__)
    print("Client version:", oracledb.clientversion())

except Exception as e:
    print("Failed to load Oracle client library:", e)
