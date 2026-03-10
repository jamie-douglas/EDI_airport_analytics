#modules/analytics/timeseries.py

import pandas as pd
from typing import List, Optional, Union

def bucket_time(df: pd.DataFrame, time_col: str, freq: str,
                out_col: str = "TimeBucket") -> pd.DataFrame:
    
    """
    Floors a timestamp column to a specified interval (e.g. '5min', '15min') and
    writes the result into a new column.
    
    Parameters
    ----------
    df: pandas.DataFrame
        Input DataFrame
    time_col: str
        Column containing the datetime values to bucket
    freq: str
        Pandas offset alias for flooring (e.g., '5min', '15min').
    out_col: str, optional
        Name of the output bucket column
    
    Returns
    ---------
    pandas.DataFrame
        Copy of DataFrame with an additional column containing the bucketed time values.
    """

    x = df.copy()
    x[time_col] = pd.to_datetime(x[time_col], errors='coerce')
    x[out_col] = x[time_col].dt.floor(freq)
    return x

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

def rolling_sum(df: pd.DataFrame, time_col: str, value_col: str, 
                window: Union[str, int], out_col: str = "RollingSum",
                groupby_keys: Optional[List[str]] = None, min_periods: int = 1,) -> pd.DataFrame:
    """
    Computes a rolling sum over time, optionally grouped by keys. Returns only the key columns and time column and the new rolling column so callers 
    can merge on key without index dependent behaviour.

    Parameters
    ----------
    df: pandas.DataFrame
        Input DataFrame
    time_col: str
        Timestamp column
    value_col: str
        Column containing numeric values
    window: str or int
        Rolling window size (e.g., "60min" or integer periods)
    out_col: str, default "RollingSum"
        Name of the output column for the rolling sum
    groupby_keys: list of str, optional
        Keys for grouped rolling operations (e.g., per Date)
    min_periods: int, default 1
        Minimum observations in window to have a value (1 gives partial sums for early slots)
    
    Returns
    --------
    pandas.DataFrame
        DataFrame with a new rolling sum column
    """

    x = df.copy()
    x[time_col] = pd.to_datetime(x[time_col], errors='coerce')

    keep_cols = ([*(groupby_keys + [time_col])])

    if groupby_keys:
        x = x.sort_values(groupby_keys + [time_col])
        rolled = (
            x.groupby(groupby_keys)
            .rolling(window=window, on=time_col, min_periods=min_periods)[value_col]
            .sum()
            .reset_index(level=groupby_keys, drop=True)
        )
        out = x[keep_cols].copy()
        out[out_col] = rolled.to_numpy()
    else:
        x = x.sort_values(time_col)
        rs = (
            x.set_index(time_col)[value_col]
            .rolling(window=window, min_periods=min_periods)
            .sum()
        )
        out = x[keep_cols].copy()
        out[out_col] = rs.to_numpy()

    return out

def peak_rolling_window(df: pd.DataFrame, time_col: str, roll_col: str, bucket_minutes: int, bucket_count: int):
    """
    Computes the peak rolling-window metrics where each row timestamp marks the START of a bucket. The window end is the END of the final bucket
    (e.g. start + bucket_minutes)

    Parameters
    ----------
    df: pandas.DataFrame
        Input DataFrame
    time_col: str
        Column containing timestamps
    roll_col: str
        Column containing rolling sum values
    bucket_minutes: int
        Size of each bucket in minutes (e.g., 5, 15, 3)
    bucket_count: int
        Number of buckets in the rolling window (e.g. 12 for 5-min, 4 for 15 min). 

    Returns
    ---------
    (float, pandas.Timestamp, pandas.Timestamp)
        Tuple containing (peak_value, window_start_stimestamp, window_end_timestamp)
    """
    if df.empty or df[roll_col].dropna().empty:
        return 0.0, pd.NaT, pd.NaT

    #find the row with the highest rolling sum
    peak_row = df.loc[df[roll_col].idxmax()]
    peak_val = float(peak_row[roll_col])

    #This timestamp is the start of the final bucket of the window
    peak_bucket_start = pd.to_datetime(peak_row[time_col])

    #Window start = peak_bucket_start - (N -1) * bucket minutes
    window_start = peak_bucket_start - pd.Timedelta(minutes=(bucket_count - 1) * bucket_minutes)

    #Window end = peak_bucket_start + bucket minutes (END of last bucket)
    window_end = peak_bucket_start + pd.Timedelta(minutes=bucket_minutes)

    return peak_val, window_start, window_end

