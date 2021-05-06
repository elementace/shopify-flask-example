import os
from typing import Any

import pandas as pd
import psycopg2

DBHOST = os.environ.get("DATABASE_HOST")
DBDATABASE = os.environ.get("DATABASE_NAME")
DBUSER = os.environ.get("DATABASE_USER")
DBPASSWORD = os.environ.get("DATABASE_PASSWORD")


def connect() -> [Any, Any]:
    # Set up a connection to the postgres server.
    conn_string = f"host={DBHOST} port=5432 dbname={DBDATABASE} user={DBUSER} password={DBPASSWORD}"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    return conn, cursor


def execute_pgsql(query: str):
    conn, cursor = connect()
    try:
        cursor.execute(query)
        conn.commit()
        conn.close()
    except Exception as e:
        cursor.execute("rollback;")
        conn.close()
        raise ConnectionError(e)


def get_pgsql_pandas_data(query: str):
    conn, _ = connect()
    data = pd.read_sql_query(query, conn)
    conn.close()
    return data
