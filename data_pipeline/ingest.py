"""
DuckDB Ingestion Module
Handles creating, appending, and querying DuckDB tables from DataFrames and PyArrow Tables.
"""

import duckdb
import pandas as pd
import pyarrow as pa


DB_PATH = "findata.db"


def get_connection(db_path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection (creates the file if it doesn't exist)."""
    return duckdb.connect(db_path)


def ingest_dataframe(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pd.DataFrame,
    replace: bool = True,
) -> int:
    """
    Ingest a pandas DataFrame into DuckDB.

    Args:
        con:        DuckDB connection
        table_name: Target table name
        df:         pandas DataFrame to ingest
        replace:    If True, drop and recreate. If False, append.

    Returns:
        Number of rows inserted
    """
    if replace:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    else:
        # Check if table exists
        exists = con.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()[0]

        if exists:
            con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        else:
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    return count


def ingest_arrow_tables(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    arrow_tables: list[pa.Table],
) -> int:
    """
    Ingest a list of PyArrow Tables into DuckDB (zero-copy).

    DuckDB natively reads Arrow format — no data conversion overhead.

    Args:
        con:          DuckDB connection
        table_name:   Target table name
        arrow_tables: List of PyArrow Tables to ingest

    Returns:
        Number of rows inserted
    """
    import time

    con.execute(f"DROP TABLE IF EXISTS {table_name}")

    insert_times = []

    for i, arrow_table in enumerate(arrow_tables):
        t0 = time.perf_counter()
        if i == 0:
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_table")
        else:
            con.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")
        t1 = time.perf_counter()
        insert_times.append(t1 - t0)

    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    if insert_times:
        import numpy as np
        p50 = np.percentile(insert_times, 50) * 1000
        p99 = np.percentile(insert_times, 99) * 1000
        print(f"  DuckDB Insert — p50: {p50:.3f} ms, p99: {p99:.3f} ms per batch")

    return count


def query(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    return con.execute(sql).fetchdf()


def list_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    """List all tables in the connected DuckDB database."""
    result = con.execute("SHOW TABLES").fetchdf()
    return result["name"].tolist() if not result.empty else []
