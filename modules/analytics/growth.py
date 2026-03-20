
# modules/analytics/growth.py
from __future__ import annotations
import pandas as pd
from typing import Callable, Dict, Any, Optional



# def period_growth(
#     loader_fn: Callable[..., pd.DataFrame],
#     *,
#     start: str,
#     end: str,
#     years_back: int = 3,
#     id_col: str = "BookingReference",
#     loader_kwargs: Optional[Dict[str, Any]] = None,
#     count_strategy: Optional[Callable[[pd.DataFrame], int]] = None,
# ) -> pd.DataFrame:
#     """
#     Calculate multi‑year demand growth by repeatedly loading comparable time windows.

#     This function evaluates demand over a set of aligned date ranges: the current
#     reporting period, followed by the same calendar window in previous years.
#     The function does not load all years into memory at once. Instead, it re‑invokes
#     the supplied loader function for each window, allowing it to scale to
#     large operational datasets.

#     A custom counting strategy may be supplied to define how "demand" is measured.
#     When provided, the strategy replaces the default behaviour of counting distinct
#     values in `id_col`. This enables specialised rules, such as counting distinct
#     (Passenger ID, Effective Month) pairs for PRM analysis.

#     Parameters
#     ----------
#     loader_fn : callable
#         A function that loads data for a period. Must support:
#             loader_fn(start=<iso string>, end=<iso string>, **loader_kwargs)
#         and must return a pandas.DataFrame containing the identifier used for counting.
#     start : str
#         Start of the reporting window (inclusive), formatted 'YYYY-MM-DD'.
#     end : str
#         End of the reporting window (exclusive), formatted 'YYYY-MM-DD'.
#     years_back : int, default 3
#         Number of historical comparison years to include.
#     id_col : str, default "BookingReference"
#         Identifier column used when no custom counting strategy is supplied.
#     loader_kwargs : dict, optional
#         Additional keyword arguments to pass to loader_fn.
#     count_strategy : callable(df) -> int, optional
#         Custom function for calculating the period’s demand metric. If not provided,
#         the default is the number of unique values in `id_col`.

#     Returns
#     -------
#     pandas.DataFrame
#         A table with one row per comparison period containing:
#             Period           : The year label.
#             Count            : The computed demand measure for that period.
#             Absolute Change  : Difference between the current period and this row's period.
#             Percent Change   : Percentage difference vs current period
#                                (None if the previous count is zero).
#     """
#     loader_kwargs = loader_kwargs or {}

#     def _count_for_period(start_iso: str, end_iso: str) -> int:
#         """Load one comparison window and compute its demand metric."""
#         df = loader_fn(start=start_iso, end=end_iso, **loader_kwargs)
#         if df.empty:
#             return 0
#         if count_strategy is not None:
#             return int(count_strategy(df))
#         return int(df[id_col].nunique())

#     results = []
#     current_year = pd.Timestamp(start).year

#     # ---- Current period ----
#     current_count = _count_for_period(start, end)
#     results.append({
#         "Period": current_year,
#         "Count": current_count,
#         "Absolute Change": None,
#         "Percent Change": None,
#     })

#     # ---- Prior years ----
#     for k in range(1, years_back + 1):
#         s_prev = (pd.Timestamp(start) - pd.DateOffset(years=k)).strftime("%Y-%m-%d")
#         e_prev = (pd.Timestamp(end)   - pd.DateOffset(years=k)).strftime("%Y-%m-%d")

#         prev_count = _count_for_period(s_prev, e_prev)
#         abs_diff   = current_count - prev_count
#         pct_diff   = (abs_diff / prev_count * 100.0) if prev_count > 0 else None

#         results.append({
#             "Period": pd.Timestamp(s_prev).year,
#             "Count": prev_count,
#             "Absolute Change": abs_diff,
#             "Percent Change": pct_diff,
#         })

#     return pd.DataFrame(results)


def period_growth(
    loader_fn: Callable[..., pd.DataFrame],
    *,
    start: str,
    end: str,
    years_back: int = 3,
    id_col: str = "BookingReference",
    loader_kwargs: Optional[Dict[str, Any]] = None,
    count_strategy: Optional[Callable[[pd.DataFrame], int]] = None,
) -> pd.DataFrame:
    """
    Calculate year‑over‑year growth by evaluating demand over aligned time windows.

    For each comparison period, this function:
        1. Forms the correct historical window by shifting the supplied
           (start, end) dates back by N whole calendar years.
        2. Loads each window exactly once using `loader_fn`.
        3. Applies a domain‑specific counting rule via `count_strategy` when supplied
           (e.g., distinct (Passenger ID, Effective Month) in PRM analysis).
        4. Computes absolute and percentage change relative to the current period.

    This approach avoids re‑loading data unnecessarily and ensures reproducible
    comparisons across matching calendar windows, regardless of how the underlying
    data is stored (e.g., PRM’s yyyymmdd integer date columns).

    Parameters
    ----------
    loader_fn : callable
        Function that loads data for a given date window.
        Must support:
            loader_fn(start="<YYYY-MM-DD>", end="<YYYY-MM-DD>", **loader_kwargs)
        and return a pandas.DataFrame containing at least the identifier used
        by the counting strategy or `id_col`.
    start : str
        Start date of the current reporting period (inclusive). Format 'YYYY-MM-DD'.
    end : str
        End date of the current reporting period (exclusive). Format 'YYYY-MM-DD'.
    years_back : int, default 3
        Number of historical comparison years to include.
    id_col : str, default "BookingReference"
        Identifier column used for default counting when no `count_strategy` is provided.
    loader_kwargs : dict, optional
        Extra keyword arguments to pass to `loader_fn` for every period.
    count_strategy : callable(df) -> int, optional
        Domain-specific counting function. When supplied, this overrides the default
        unique‑ID count. Useful for defining “demand” in domain-appropriate terms,
        such as distinct (Passenger ID, Effective Month) for PRM.

    Returns
    -------
    pandas.DataFrame
        A growth comparison table with one row per period containing:
            • Period          : Year label
            • Count           : Demand metric for the period
            • Absolute Change : Current minus the comparison period
            • Percent Change  : Percentage difference (None if denominator is zero)
    """
    loader_kwargs = loader_kwargs or {}

    # ----------------------------------------------------------------------
    # 1. Build the list of windows to load (current + prior N years)
    # ----------------------------------------------------------------------
    current_start = pd.Timestamp(start)
    current_end   = pd.Timestamp(end)

    windows = [(current_start, current_end)]
    for k in range(1, years_back + 1):
        windows.append((
            current_start - pd.DateOffset(years=k),
            current_end   - pd.DateOffset(years=k),
        ))

    # ----------------------------------------------------------------------
    # 2. Load all windows exactly once
    # ----------------------------------------------------------------------
    loaded_dfs = []
    for s, e in windows:
        df = loader_fn(
            start=s.strftime("%Y-%m-%d"),
            end=e.strftime("%Y-%m-%d"),
            **loader_kwargs
        )
        loaded_dfs.append(df)

    # ----------------------------------------------------------------------
    # 3. Apply the appropriate counting rule to each window
    # ----------------------------------------------------------------------
    counts = []
    for df in loaded_dfs:
        if df.empty:
            counts.append(0)
        elif count_strategy is not None:
            counts.append(int(count_strategy(df)))
        else:
            counts.append(int(df[id_col].nunique()))

    # ----------------------------------------------------------------------
    # 4. Build the results table
    # ----------------------------------------------------------------------
    current_count = counts[0]
    result_rows = [{
        "Period": current_start.year,
        "Count": current_count,
        "Absolute Change": None,
        "Percent Change": None,
    }]

    for idx in range(1, years_back + 1):
        prev_year  = (current_start - pd.DateOffset(years=idx)).year
        prev_count = counts[idx]

        abs_change = current_count - prev_count
        pct_change = (abs_change / prev_count * 100.0) if prev_count > 0 else None

        result_rows.append({
            "Period": prev_year,
            "Count": prev_count,
            "Absolute Change": abs_change,
            "Percent Change": pct_change,
        })

    return pd.DataFrame(result_rows)
