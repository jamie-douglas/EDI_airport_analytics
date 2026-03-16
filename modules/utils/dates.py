#modules/utils/dates.py
from typing import Union, List

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
    cols = [columns] if isinstance(columns, str) else list(columns)
    for col in cols:
        x[col] = pd.to_datetime(x[col], errors='coerce')
    return x

def add_date_parts(df: pd.DataFrame, col: str, day=None, year=None, month=None, month_name=None, hour=None, hour_label=None) -> pd.DataFrame:
    """
    Append common date/time parts derived from a datetime column.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame containing the source datetime column.
    col : str
        Name of the source datetime column (must be convertible to datetime).

    Returns
    -------
    pandas.DataFrame
        Copy of df with the following additional columns:
        - f"{col}_date"       : date (datetime.date)
        - f"{col}_year"       : year (int)
        - f"{col}_month"      : month number 1–12 (int)
        - f"{col}_month_name" : abbreviated month name (e.g., "Jan")
        - f"{col}_hour"       : hour-of-day 0–23 (int)
        - f"{col}_hour_label" : hour label "HH:00" (str)
    """
    x = df.copy()
    t = pd.to_datetime(x[col], errors="coerce")
    if day:
        x[f"Day"] = t.dt.date
    if year:
        x[f"Year"] = t.dt.year
    if month:
        x[f"Month"] = t.dt.month
    if month_name:
        x[f"Month Name"] = t.dt.strftime("%b")
    if hour:
        x[f"Hour"] = t.dt.hour
    if hour_label:
        x[f"Hour Label"] = t.dt.strftime("%H:00")
    return x

