#modules/utils/query.py

from modules.utils.db import get_engine
from modules.utils.sql import read_sql
from sqlalchemy import text

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
        overlap: bool = False
):
    """
    Executes a dynamic SQL SELECT query with optional filters, parameters, and date bounds
    
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
        If true and both date_column and end_column are provided, returns rows that overlap the window
        (row_start < end AND row_end >= start). If False, returns rows fully contained within the window 
        (row_start >= start AND row_end < end). Default is False (non-overlapping).
        
    Returns
    ---------
    pandas.DataFrame
        DataFrame containing the query results
    """
    
    #Validation: can't use start and end without date_column
    if (start or end) and not date_column:
            raise ValueError(
                "Date filtering requires 'date_column' when 'start' or 'end' is provided."
            )


    engine = get_engine()

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
    if date_column and start and not end_column:
        #single timestamp column
        sql += f" AND {date_column} >= :start"
        final_params['start'] = start
        if end:
            sql += f" AND {date_column} < :end"
            final_params['end'] = end

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
    
    #ORDER BY
    if order_by:
        sql += f" ORDER BY {order_by}"
    
    #Execute
    return read_sql(engine, sql, params=final_params)