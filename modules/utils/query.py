#modules/utils/query.py

from modules.utils.db import get_engine
from modules.utils.sql import read_sql
from sqlalchemy import text
import pandas as pd


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
        engine = None,
):
    """
    Executes a dynamic SQL SELECT query with optional filters, parameters, and date bounds
    
    Date filtering modes
    --------------------
    1) Single timestamp column:
       WHERE date_col >= :start AND date_col < :end

    2) Interval columns + overlap=True:
       WHERE row_start < :end AND row_end >= :start

    3) Interval columns + or_events=True:
       WHERE (
           (row_start >= :start AND row_start < :end)
        OR (row_end   >= :start AND row_end   < :end)
       )
       Use this when your "event" can be either start or end, and end may be NULL.

    
    Parameters
    ----------
    table: str
        Fully qualified table name
    columns: list[str]
        List of columns to select
    where: list[str], optional
        List of WHERE conditions to append (ANDed together)
    params: dict, optional
        Additional named parameters for SQL bind variables
    date_column: str, optional
        Column used for >= start and <end date filtering
    end_column: str, optional
        optional end column for interval-style date filtering
    start: str, optional
        Start date (inclusive) for date filtering
    end: str, optional
        End date (exclusive) for date filtering
    distinct: bool, optional
        whether to apply SELECT DISTINCT.
    order_by: str, optional
        ORDER BY clause without the keyword
    overlap : bool, optional
    or_events: bool, optional
    engine: optional
        
    Returns
    ---------
    pandas.DataFrame
        DataFrame containing the query results
    """
    #--- Normalise incoming dates ---
    start = _normalise_iso_date(start)
    end = _normalise_iso_date(end)

    #Validation: can't use start and end without date_column
    if (start or end) and not date_column:
            raise ValueError(
                "Date filtering requires 'date_column' when 'start' or 'end' is provided."
            )


    engine = engine or get_engine()

    #SELECT
    select_clause = "SELECT DISTINCT " if distinct else "SELECT "
    select_clause += ", ".join(columns)

    sql = f"{select_clause} FROM {table} WHERE 1=1"

    #Paremeters dictionary
    final_params = dict(params) if params else {}

    #WHERE conditions
    if where:
        for condition in where:
            sql += f" AND ({condition})"


    #Date filtering
    if date_column and end_column and start and end and or_events:
        sql += (
            f" AND ( ({date_column} >= :start AND {date_column} < :end) "
            f"OR    ({end_column} >= :start AND {end_column} < :end) )"
        )
        final_params["start"] = start
        final_params["end"] = end
    
    elif date_column and end_column and start and end:
        #interval columns (start, end)
        if overlap:
            #any intersection with [start, end)
            sql += f" AND {date_column} < :end AND {end_column} >= :start"
        else:
            #fullt contained in [start, end)
            sql += f" AND {date_column} >= :start AND {end_column} < :end"
        final_params['start'] = start
        final_params['end'] = end

    elif date_column and start:
        #single timestamp column
        sql += f" AND {date_column} >= :start"
        final_params['start'] = start
        if end:
            sql += f" AND {date_column} < :end"
            final_params['end'] = end

    
    
    #ORDER BY
    if order_by:
        sql += f" ORDER BY {order_by}"

    
    #Execute
    return read_sql(engine, sql, params=final_params)