#modules/utils/sql.py
import pandas as pd
from sqlalchemy import text

def read_sql(engine, sql: str, params=None, parse_dates=None):
    """
    Executes a SQL query and returns the results as a pandas DataFrame. 

    Parameters
    ----------
    engine: sqlalchemy.engine.Engine
        A SQLAlchemy engine to use for executing the query.
    sql: str
        The SQL query to execute.
    params: dict, optional
        Dictionary of SQL bind parameters
    parse_dates: list[str], optional
        List of columns to parse as datetime.

    Returns
    ---------
    pandas.DataFrame
        The result set returned by the SQL Query.
    """
    if parse_dates:
        return pd.read_sql(text(sql), con=engine, params=params, parse_dates=parse_dates)
    return pd.read_sql(text(sql), con=engine, params=params)