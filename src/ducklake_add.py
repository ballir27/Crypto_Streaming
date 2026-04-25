import os
from pathlib import Path

import duckdb
from dotenv import dotenv_values
from loguru import logger

CATALOG = "data/crypto_catalog.ducklake"
DATA_PATH = "data"
# def marts_from_postgres():
# 1. Connect to DuckDB and set up environment
con = duckdb.connect()
con.execute("INSTALL postgres; LOAD postgres;")
con.execute("INSTALL ducklake; LOAD ducklake;")

# 2. Attach Postgres Database
PROJECT_ROOT = Path(__file__).resolve().parent
config = dotenv_values(os.path.join(PROJECT_ROOT, "../.env"))


DB_CONFIG = {
    "host": config.get("POSTGRES_HOST"),
    "database": config.get("POSTGRES_DB"),
    "user": config.get("POSTGRES_USER"),
    "password": config.get("POSTGRES_PASSWORD"),
    "port": config.get("POSTGRES_PORT")
}

# Put pg connection string in format DuckDB expects
pg_conn_str = (
    f"host={DB_CONFIG['host']} "
    f"port={DB_CONFIG['port']} "
    f"dbname={DB_CONFIG['database']} "
    f"user={DB_CONFIG['user']} "
    f"password={DB_CONFIG['password']}"
)

con.execute(f"ATTACH'{pg_conn_str}' AS project_4 (TYPE postgres);")

# 3. Attach DuckLake Storage
con.execute(
            f"ATTACH 'ducklake:{CATALOG}' AS crypto_ducklake"
            f"(DATA_PATH '{DATA_PATH}');")
con.execute("USE crypto_ducklake;")

# 4. Migrate Marts Schema
con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
con.execute("""
    CREATE OR REPLACE TABLE marts.fct_btc AS 
    SELECT * FROM project_4.marts.fct_btc;
""")

logger.info("Schema 'marts' added to DuckLake successfully.")

# For debugging: Verify the data is there
# print("\nRows in project_4.marts.fct_btc:")
# for row in con.execute("SELECT * FROM project_4.marts.fct_btc LIMIT 10;").fetchall():
#     print(" ", row)
