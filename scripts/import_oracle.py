"""
import_oracle.py
------------------
This script pulls the latest SEPA datatable from the Oracle database.
It pipes the outputs into a postgres database. 
"""

import oracledb
import psycopg2
from sys import platform
import env_vars as ev


def test_platform():
    if platform == "linux" or platform == "linux2":
        tns_path = ev.LINUX_TNS
    elif platform == "win32":
        tns_path = ev.WINDOWS_TNS
    elif platform == "darwin":
        raise Exception("Darwin machine, unknown TNS file location")

    oracledb.defaults.config_dir = tns_path
    print(f"platform is {platform}, tns path is {tns_path}")
    return oracledb.defaults.config_dir


def create_cxs():
    """Create connections to Oracle and Postgres dbs"""
    try:
        oracle_cx = oracledb.connect(user=ev.ORA_UN, password=ev.ORA_PW, dsn=ev.DNS)
        print("Successfully connected to the Oracle database")
        pg_cx = psycopg2.connect(ev.POSTGRES_URL)
        print("Successfully connected to the Postgres database")
    except (oracledb.Error, psycopg2.OperationalError) as e:
        print(f"Error: {e}")

    return oracle_cx, pg_cx


def get_pg_create_table_query(table_name, columns):
    """Generates a PostgreSQL CREATE TABLE query with dynamic columns."""
    columns_with_types = ", ".join([f"{col} text" for col in columns])
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types});"


def pipe_data(oracle_cx: oracledb.Connection, pg_cx: psycopg2.extensions.connection):
    """Pipes Oracle data into PostgreSQL db."""
    orcl_cursor = oracle_cx.cursor()
    pg_cursor = pg_cx.cursor()

    orcl_cursor.execute("SELECT * FROM SEPADATA WHERE ROWNUM = 1")
    columns = [desc[0] for desc in orcl_cursor.description]

    create_table_query = get_pg_create_table_query("public.oracle_copy", columns)
    pg_cursor.execute(create_table_query)

    orcl_cursor.execute("SELECT * FROM SEPADATA")
    oracle_data = orcl_cursor.fetchall()
    placeholders = ", ".join(["%s"] * len(columns))
    insert_query = f"INSERT INTO public.oracle_copy VALUES ({placeholders})"
    for row in oracle_data:
        pg_cursor.execute(insert_query, row)

    pg_cx.commit()
    orcl_cursor.close()
    pg_cursor.close()
    pg_cx.close()
    oracle_cx.close()


if __name__ == "__main__":
    tns = test_platform()
    oracle_cx, pg_cx = create_cxs()
    pipe_data(oracle_cx, pg_cx)
