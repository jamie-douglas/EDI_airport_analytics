
# modules/analytics/growth.py
from __future__ import annotations
import pandas as pd
from typing import Callable, Dict, Any, Optional


def period_growth(
    loader_fn: Callable[..., pd.DataFrame],
    *,
    start: str,
    end: str,
    years_back: int = 3,
    id_col: str = "BookingReference",
    loader_kwargs: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Generic multi-year growth calculator for any operational dataset.

    This function **does NOT load history into memory**. Instead, it
    re-runs the loader function independently for each comparison window,
    which makes it scalable even when total data volume is very large

    Parameters
    ----------
    loader_fn : callable
        A function that loads a dataset for a given date window.
        Must accept keyword arguments:
            loader_fn(start=start_iso_string, end=end_iso_string, **loader_kwargs)
        and return a DataFrame containing the transaction ID column.
    start : str
        Start date (inclusive) of the *current* reporting period. Format: 'YYYY-MM-DD'.
    end : str
        End date (exclusive) of the *current* reporting period.
    years_back : int, default 3
        How many previous years to compare against.
    id_col : str, default 'BookingReference'
        Column name that uniquely identifies a transaction.
        This must exist in the DataFrame returned by loader_fn.
    loader_kwargs : dict, optional
        Any keyword args that should always be passed to loader_fn,
        e.g. for FastPark:
            {"overlap": True, "or_events": True}

    Returns
    -------
    pandas.DataFrame
        Columns:
            Period          → Year of the comparison period
            Count           → Number of unique transactions
            Absolute Change → Count(current) - Count(previous)
            Percent Change  → Percent difference vs previous
    """

    loader_kwargs = loader_kwargs or {}

    def _count_transactions(start_iso: str, end_iso: str) -> int:
        df = loader_fn(start=start_iso, end=end_iso, **loader_kwargs)
        if df.empty:
            return 0
        return int(df[id_col].nunique())

    # Current period
    results = []
    current_year = pd.Timestamp(start).year

    current_count = _count_transactions(start, end)
    results.append({
        "Period": current_year,
        "Count": current_count,
        "Absolute Change": None,
        "Percent Change": None,
    })

    # Previous years
    for k in range(1, years_back + 1):
        s_prev = (pd.Timestamp(start) - pd.DateOffset(years=k)).strftime("%Y-%m-%d")
        e_prev = (pd.Timestamp(end) - pd.DateOffset(years=k)).strftime("%Y-%m-%d")

        prev_count = _count_transactions(s_prev, e_prev)
        abs_change = current_count - prev_count
        pct_change = (abs_change / prev_count * 100.0) if prev_count > 0 else None

        results.append({
            "Period": pd.Timestamp(s_prev).year,
            "Count": prev_count,
            "Absolute Change": abs_change,
            "Percent Change": pct_change,
        })

    return pd.DataFrame(results)
