
#modules/analytics/grouping.py

import pandas as pd
from typing import List

def group_unique(df: pd.DataFrame, by_cols: List[str], id_col: str = "PassengerID") -> pd.DataFrame:
    """
    Groups by one or more columns and computes the number of unique values of a specified ID column
    
    Parameters
    ----------
    df: pd.DataFrame
        Input DataFrame
    by_cols: list[str]
        Columns to group by.
    id_col: str, default "PassengerID"
    
    Returns
    ------------
    pandas.DataFrame
        DataFrame with the grouping columns and 'Unique Count' - number of unique id_col values"""
    
    out = (df.groupby(by_cols, dropna=False)[id_col]
           .nunique()
           .reset_index(name="Unique Count")
    )

    return out


def group_sum(df: pd.DataFrame, by_cols: List[str], value_col: str,
              out_col: str) -> pd.DataFrame:
    """
    Groups by input columns and computes the sum of a specified column.

    Parameters
    ----------
    df: pandas.DataFrame
        Input DataFrame
    by_cols: List of str
        Grouping columns
    value_col: str
        Column to aggregate
    out_col: str
        Name of the output summed column. 
    
    Returns
    ---------
    pandas.DataFrame
        Grouped DataFrame with a single aggregated column
    """

    return (
        df.groupby(by_cols, dropna=False)[value_col]
        .sum()
        .rename(out_col)
        .reset_index()
    )