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
    except oracledb.Error as e:
        print(f"Error connecting to the Oracle database: {e}")

    try:
        pg_cx = psycopg2.connect(ev.POSTGRES_URL)
        print("Successfully connected to the Postgres database")
    except psycopg2.OperationalError as e:
        print(f"Error connecting to the Postgres database: {e}")

    if oracle_cx is None or pg_cx is None:
        raise Exception("Failed to create one or more database connections.")

    return oracle_cx, pg_cx


def convert_pg_types_to_oracle(oracle_type):
    oracle2pg = {
        "<DbType DB_TYPE_NUMBER>": "NUMERIC",
        "<DbType DB_TYPE_VARCHAR>": "VARCHAR",
        "<DbType DB_TYPE_DATE>": "DATE",
    }
    return oracle2pg[str(oracle_type)]


def get_pg_create_table_query(table_name, columns_and_types: dict):
    """Generates a PostgreSQL CREATE TABLE query with dynamic columns."""
    columns_with_types = ", ".join(
        [
            f"{col} {convert_pg_types_to_oracle(oracle_type)}"
            for (col, oracle_type) in columns_and_types.items()
        ]
    )

    return f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types});"


def pipe_data(oracle_cx: oracledb.Connection, pg_cx: psycopg2.extensions.connection):
    """Pipes Oracle data into PostgreSQL db."""
    orcl_cursor = oracle_cx.cursor()
    pg_cursor = pg_cx.cursor()
    pg_cursor.execute("DROP TABLE IF EXISTS public.oracle_copy")

    orcl_cursor.execute("SELECT * FROM SEPADATA WHERE ROWNUM = 1")
    columns_and_types = {}
    for value in orcl_cursor.description:
        columns_and_types[value[0]] = value[1]  # grabs column and oracle data type

    create_table_query = get_pg_create_table_query(
        "public.oracle_copy", columns_and_types
    )
    pg_cursor.execute(create_table_query)

    orcl_cursor.execute("SELECT * FROM SEPADATA")
    oracle_data = orcl_cursor.fetchall()
    placeholders = ", ".join(["%s"] * len(columns_and_types))
    insert_query = f"INSERT INTO public.oracle_copy VALUES ({placeholders})"
    for row in oracle_data:
        pg_cursor.execute(insert_query, row)

    pg_cx.commit()
    orcl_cursor.close()
    pg_cursor.close()
    pg_cx.close()
    oracle_cx.close()
    print("successfully imported oracle into postgres!")


if __name__ == "__main__":
    tns = test_platform()
    oracle_cx, pg_cx = create_cxs()
    pipe_data(oracle_cx, pg_cx)
