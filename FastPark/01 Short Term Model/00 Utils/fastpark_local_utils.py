import os
import sys
import urllib

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# Load environment variables from .env if present.
load_dotenv()


# Database defaults.
DSN = os.getenv("DSN", "AzureConnection")
USERNAME = os.getenv("USERNAME", "")


def get_engine(dsn: str = DSN, username: str = USERNAME):
    """
    Create and return a SQLAlchemy engine using an ODBC DSN and username.
    """
    try:
        conn_str = f"DSN={dsn};UID={username};"
        params = urllib.parse.quote_plus(conn_str)
        return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    except Exception as ex:
        print(f"Error creating SQLAlchemy engine: {ex}")
        sys.exit(1)


def read_sql(engine, sql: str, params=None, parse_dates=None):
    """
    Execute a SQL query and return a pandas DataFrame.
    """
    if parse_dates:
        return pd.read_sql(text(sql), con=engine, params=params, parse_dates=parse_dates)
    return pd.read_sql(text(sql), con=engine, params=params)


def _normalise_iso_date(d: str | None) -> str | None:
    """Convert input to strict YYYY-MM-DD or return None."""
    if d is None:
        return None
    ts = pd.to_datetime(d, errors="raise")
    return ts.strftime("%Y-%m-%d")


def query(
    table: str,
    columns: list[str],
    where: list[str] | None = None,
    params: dict | None = None,
    date_column: str | None = None,
    end_column: str | None = None,
    start: str | None = None,
    end: str | None = None,
    distinct: bool = False,
    order_by: str | None = None,
    overlap: bool = False,
    or_events: bool = False,
    engine=None,
    query_option: str | None = None,
):
    """
    Execute a dynamic SQL SELECT query with optional filters and date bounds.
    """
    start = _normalise_iso_date(start)
    end = _normalise_iso_date(end)

    if (start or end) and not date_column:
        raise ValueError(
            "Date filtering requires 'date_column' when 'start' or 'end' is provided."
        )

    engine = engine or get_engine()

    select_clause = "SELECT DISTINCT " if distinct else "SELECT "
    select_clause += ", ".join(columns)

    sql = f"{select_clause} FROM {table} WHERE 1=1"

    final_params = dict(params) if params else {}

    if where:
        for condition in where:
            sql += f" AND ({condition})"

    if date_column and end_column and start and end and or_events:
        sql += (
            f" AND ( ({date_column} >= :start AND {date_column} < :end) "
            f"OR    ({end_column} >= :start AND {end_column} < :end) )"
        )
        final_params["start"] = start
        final_params["end"] = end

    elif date_column and end_column and start and end:
        if overlap:
            sql += f" AND {date_column} < :end AND {end_column} >= :start"
        else:
            sql += f" AND {date_column} >= :start AND {end_column} < :end"
        final_params["start"] = start
        final_params["end"] = end

    elif date_column and start:
        sql += f" AND {date_column} >= :start"
        final_params["start"] = start
        if end:
            sql += f" AND {date_column} < :end"
            final_params["end"] = end

    if order_by:
        sql += f" ORDER BY {order_by}"

    if query_option:
        opt = query_option.strip()
        if not opt.upper().startswith("OPTION"):
            raise ValueError("query_option must start with OPTION")
        sql += f" {opt}"

    return read_sql(engine, sql, params=final_params)


def step(prev: float, msg: str) -> float:
    """
    Print progress message with elapsed seconds and return new timestamp.
    """
    import time

    now = time.perf_counter()
    print(f"    ✓ {msg}   [{now - prev:0.2f}s]")
    return now
