#modules/utils/dates.py
from ast import List

import pandas as pd

def to_datetime(df: pd.DataFrame, columns: Union[str, List[str]]) -> pd.DataFrame:
    """
    Converts specified columns in a DataFrame to datetime format. 

    Parameters
    ----------
    df: pandas.DataFrame
        The DataFrame containing the columns to convert.
    columns: str or list[str]
       Column name or list of column names to convert.

    Returns
    ---------
    pandas.DataFrame
        The input DataFrame with specified columns converted to datetime.
    """
    x = df.copy()
    cols = [columns] if isinstance(columns, str) else columns = list(columns)
    for col in cols:
        x[col] = pd.to_datetime(x[col], errors='coerce')
    return x

def add_date_parts(df: pf.DataFrame, col: str) -> pd.DataFrame:
    """
    Adds date components derived from datetime column.
    
    Parameters
    ----------
    df: pandas.Dataframe
        Input DataFrame containing the source datetime column
    col: str
        Name of the source datetime column (must be convertible to datetime)

    Returns
    ----------
    pandas.DataFrame
        Copy of the input DataFrame with additional columns:
        f"{col}_date" : python date object (datetime.date)
        f"{col}_year" : year as integer
        f"{col}_month" : month number (1-12)
        f"{col}_month_name": abbreviated month name (e.g., "Jan")
        The source column is not modified
    """

    x = df.copy()
    t = pd.to_datetime(x[col], errors='coerce')
    x[f"{col}_date"] = t.dt.date
    x[f"{col}_year"] = t.dt.year
    x[f"{col}_month"] = t.dt.month
    x[f"{col}_month_name"] = t.dt.strftime("%b")
    
    return x
