
#modules/analytics/grouping.py

import pandas as pd
from typing import List, Optional, Union
from modules.utils.dates import assign_effective_month

# -----------------------------
# GROUPING FUNCTIONS FOR GENERAL USE
# -----------------------------


def group_unique(df: pd.DataFrame, by_cols: List[str], id_col: str = "Passenger ID") -> pd.DataFrame:
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

def group_average(df: pd.DataFrame, by_cols: List[str], value_col: str,
              out_col: str) -> pd.DataFrame:
    """
    Groups by input columns and computes the average of a specified column.

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
        .mean()
        .rename(out_col)
        .reset_index()
    )

def stats_grouping(df: pd.DataFrame, by_cols: List[str], value_col: str, prefix: Optional[str] = None) -> pd.DataFrame:
    """
    Groups by input columns common statitstical aggregates (mean, median, min, max, std) for a specified numeric column
    
    Parameters
    ----------
    
    df: pandas.DataFrame
        Input DataFrame
    by_cols: List of str
        Grouping columns
    value_col: str
        Column to aggregate
    prefix: str, optional
        Optional prefix for output column names. If None, value_col is used as prefix.  
    
    Returns
    ---------
    pandas.DataFrame
        Grouped DataFrame with columns:
            by_cols...
            {prefix} mean
            {prefix} median
            {prefix} min
            {prefix} max
            {prefix} std

    """

    
    agg_df = (
            df.groupby(by_cols, dropna=False)[value_col]
            .agg(
                Mean="mean",
                Median="median",
                Min="min",
                Max="max",
                Std="std",
            )
            .reset_index()
        )

    if prefix:
        agg_df = agg_df.rename(columns={
            "Mean": f"{prefix} Mean",
            "Median": f"{prefix} Median",
            "Min": f"{prefix} Min",
            "Max": f"{prefix} Max",
            "Std": f"{prefix} Std",
        })

    return agg_df


# -----------------------------
# EFFECTIVE MONTH GROUPING AND COUNTING (FOR ACROSS MONTH SPILLOVER CORRECTION)
# -----------------------------
def ensure_effective_month(
    df: pd.DataFrame,
    *,
    id_col: str = "Passenger ID",
    date_col: str = "Operation Date",
    ad_col: str = "A/D",
    out_col: str = "Effective Month",
    window_start: Optional[Union[str, pd.Timestamp]] = None,
) -> pd.DataFrame:
    """
    Ensures the DataFrame contains an 'Effective Month' column using the
    cross‑midnight / cross‑month reassignment rule (with optional same A/D constraint).

    If the column already exists and is datetime‑typed, a shallow copy is returned unchanged.
    """
    if out_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[out_col]):
        return df.copy()

    return assign_effective_month(
        df.copy(),
        id_col=id_col,
        date_col=date_col,
        ad_col=ad_col,
        out_col=out_col,
        window_start=window_start,
    )

def group_unique_by_effective_month(
    df: pd.DataFrame,
    *,
    id_col: str = "Passenger ID",
    date_col: str = "Operation Date",
    ad_col: str = "A/D",
    out_col: str = "Month",
    window_start: Optional[Union[str, pd.Timestamp]] = None,
) -> pd.DataFrame:
    """
    Groups PRM data by Effective Month and returns a unique Passenger count per corrected month.

    Output Columns
    --------------
    Month         : Timestamp at month start (after corrections)
    Unique Count  : deduped count of Passenger ID
    """
    eff = ensure_effective_month(
        df,
        id_col=id_col,
        date_col=date_col,
        ad_col=ad_col,
        out_col="Effective Month",
        window_start=window_start,
    )

    return (
        eff.groupby("Effective Month", dropna=False)[id_col]
           .nunique()
           .reset_index(name="Unique Count")
           .rename(columns={"Effective Month": out_col})
    )

def count_distinct_id_by_effective_month(
    df: pd.DataFrame,
    *,
    id_col: str = "Passenger ID",
    date_col: str = "Operation Date",
    ad_col: str = "A/D",
    window_start: Optional[Union[str, pd.Timestamp]] = None,
) -> int:
    """
    Counts distinct (Passenger ID, Effective Month) pairs using the reassignment rule.
    This is the 'full count' used for yearly totals and multi-year growth.
    """
    eff = ensure_effective_month(
        df,
        id_col=id_col,
        date_col=date_col,
        ad_col=ad_col,
        out_col="Effective Month",
        window_start=window_start,
    )

    return int(eff[[id_col, "Effective Month"]].drop_duplicates().shape[0])

def ensure_effective_month(
    df: pd.DataFrame,
    *,
    id_col: str = "Passenger ID",
    date_col: str = "Operation Date",
    ad_col: str = "A/D",
    out_col: str = "Effective Month",
    window_start: Optional[Union[str, pd.Timestamp]] = None,
) -> pd.DataFrame:
    """
    Ensure the DataFrame contains an 'Effective Month' column.
    If the column already exists and is a datetime64 dtype, it is preserved.

    Parameters
    ----------
    df : pandas.DataFrame
        Input PRM dataset.
    id_col : str, default "Passenger ID"
        Passenger identifier.
    date_col : str, default "Operation Date"
        Date column used to derive Effective Month if missing.
    ad_col : str, default "A/D"
        Arrival/Departure indicator column.
    out_col : str, default "Effective Month"
        Name of the month column to ensure.
    window_start : str or pandas.Timestamp, optional
        Start date (inclusive) that prevents reassignment into pre-window months.

    Returns
    -------
    pandas.DataFrame
        Copy of df with a valid Effective Month column.
    """
    if out_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[out_col]):
        return df.copy()

    return assign_effective_month(
        df.copy(),
        id_col=id_col,
        date_col=date_col,
        ad_col=ad_col,
        out_col=out_col,
        window_start=window_start,
    )

def group_unique_by_effective_month(
    df: pd.DataFrame,
    *,
    id_col: str = "Passenger ID",
    date_col: str = "Operation Date",
    ad_col: str = "A/D",
    out_col: str = "Month",
    window_start: Optional[Union[str, pd.Timestamp]] = None,
) -> pd.DataFrame:
    """
    Group PRM records by Effective Month and count unique passengers.

    Parameters
    ----------
    df : pandas.DataFrame
        Input PRM dataset.
    id_col : str, default "Passenger ID"
        Unique passenger identifier.
    date_col : str, default "Operation Date"
        Raw date column used to compute Effective Month.
    ad_col : str, default "A/D"
        Direction indicator used for spillover logic.
    out_col : str, default "Month"
        Name of the output month column.
    window_start : str or pandas.Timestamp, optional
        Prevents reassignment into months earlier than the window.

    Returns
    -------
    pandas.DataFrame
        Monthly PRM unique counts with:
            ['Month', 'Unique Count']
    """
    eff = ensure_effective_month(
        df,
        id_col=id_col,
        date_col=date_col,
        ad_col=ad_col,
        out_col="Effective Month",
        window_start=window_start,
    )

    grouped = (
        eff.groupby("Effective Month", dropna=False)[id_col]
           .nunique()
           .reset_index(name="Unique Count")
           .rename(columns={"Effective Month": out_col})
    )
    return grouped

def count_distinct_id_by_effective_month(
    df: pd.DataFrame,
    *,
    id_col: str = "Passenger ID",
    date_col: str = "Operation Date",
    ad_col: str = "A/D",
    window_start: Optional[Union[str, pd.Timestamp]] = None,
) -> int:
    """
    Count unique (Passenger ID, Effective Month) pairs for correct
    full‑period PRM demand totals and multi‑year growth.

    Parameters
    ----------
    df : pandas.DataFrame
        Input PRM dataset.
    id_col : str, default "Passenger ID"
        Unique passenger identifier.
    date_col : str, default "Operation Date"
        Raw date column used to derive Effective Month.
    ad_col : str, default "A/D"
        Direction indicator used for spillover correction.
    window_start : str or pandas.Timestamp, optional
        Guards against pushing Effective Month into pre-window months.

    Returns
    -------
    int
        Total number of distinct (Passenger ID, Effective Month) combinations.
    """
    eff = ensure_effective_month(
        df,
        id_col=id_col,
        date_col=date_col,
        ad_col=ad_col,
        out_col="Effective Month",
        window_start=window_start,
    )

    return int(eff[[id_col, "Effective Month"]].drop_duplicates().shape[0])
