#modules/utils/dates.py
from typing import Union, List, Optional
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




def assign_effective_month(
    df: pd.DataFrame,
    *,
    id_col: str = "Passenger ID",
    date_col: str = "Operation Date",
    ad_col: str = "A/D",
    out_col: str = "Effective Month",
    window_start: Optional[Union[str, pd.Timestamp]] = None,
) -> pd.DataFrame:
    """
    Derive an 'Effective Month' for each PRM job to correct month‑boundary double counting
    caused by jobs that legitimately span midnight.

    Reassignment Rule
    -----------------
    A job is considered part of the *previous* month if:
        1. It occurs exactly one day after the previous job for the same Passenger ID, AND
        2. The month changes between those two days, AND
        3. The 'A/D' (Arrival/Departure indicator) is the same for both jobs.
           (This prevents misclassifying true return‑trips as spillover events.)

    Guard Condition
    ---------------
    If `window_start` is supplied, Effective Month will never be reassigned into a month
    earlier than the reporting window.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset containing at least id_col, date_col, and optionally ad_col.
    id_col : str, default "Passenger ID"
        Unique passenger identifier.
    date_col : str, default "Operation Date"
        Date column (will be coerced to datetime).
    ad_col : str, default "A/D"
        Arrival/Departure indicator column.
    out_col : str, default "Effective Month"
        Name of the output month column (month-start Timestamp).
    window_start : str or pandas.Timestamp, optional
        First day (inclusive) of the active reporting window.

    Returns
    -------
    pandas.DataFrame
        Copy of df with an added datetime64[ns] column `out_col`.
    """
    if df.empty:
        out = df.copy()
        out[out_col] = pd.NaT
        return out

    x = df.copy()
    x[date_col] = pd.to_datetime(x[date_col], errors="coerce")
    x = x.sort_values([id_col, date_col])

    # Normalised date for consecutive-day logic
    x["_d"] = x[date_col].dt.normalize()
    x["_prev_d"] = x.groupby(id_col)["_d"].shift(1)
    x["_daydiff"] = (x["_d"] - x["_prev_d"]).dt.days

    # Month periods
    x["_curr_M"] = x["_d"].dt.to_period("M")
    x["_prev_M"] = x["_prev_d"].dt.to_period("M")

    # A/D matching
    has_ad = ad_col in x.columns
    if has_ad:
        x["_prev_AD"] = x.groupby(id_col)[ad_col].shift(1)

    # Guard against reassigning into pre-window months
    min_month = (
        pd.Timestamp(window_start).to_period("M")
        if window_start is not None else x["_curr_M"].min()
    )

    # Identify midnight spillover rows
    same_ad = (x[ad_col] == x["_prev_AD"]) if has_ad else True
    spillover = (
        (x["_daydiff"] == 1) &
        (x["_prev_M"] != x["_curr_M"]) &
        (x["_prev_M"] >= min_month) &
        same_ad
    )

    # Assign corrected month
    eff_m = x["_curr_M"].copy()
    eff_m.loc[spillover] = x.loc[spillover, "_prev_M"]
    x[out_col] = eff_m.dt.to_timestamp(how="start")

    # Clean up scaffolding
    drop_cols = ["_d", "_prev_d", "_daydiff", "_curr_M", "_prev_M"]
    if has_ad:
        drop_cols.append("_prev_AD")
    x.drop(columns=drop_cols, inplace=True, errors="ignore")

    return x
