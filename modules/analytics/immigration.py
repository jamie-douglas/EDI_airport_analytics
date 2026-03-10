
# modules/analytics/immigration.py
from __future__ import annotations
from typing import Optional
import pandas as pd
from modules.config import (
    IMM_SEASON_SUMMER_START,
    IMM_SEASON_SUMMER_END,
    IA1_SUMMER_HOURS,
    IA1_BASELINE_OPEN_DAYS,
    IA1_BASELINE_DAY_HOURS,
    IA1_BASELINE_NIGHT_HOURS,
    IA2_ALWAYS_OPEN,
)

# Convert config timestamps once
_SUMMER_START = pd.Timestamp(IMM_SEASON_SUMMER_START)
_SUMMER_END   = pd.Timestamp(IMM_SEASON_SUMMER_END)

def _in_summer_window(ts: pd.Timestamp) -> bool:
    """
    Checks whether a timestamp falls within the configured summer window.

    Parameters
    ----------
    ts : pandas.Timestamp

    Returns
    -------
    bool
    """
    return _SUMMER_START <= ts < _SUMMER_END


def _time_in_ranges(hour: int, ranges: list[tuple[int, int]]) -> bool:
    """
    Helper to check if an hour is within any of a list of (start_hour, end_hour) ranges.

    Parameters
    ----------
    hour : int
        Hour of day (0–23).
    ranges : list of (int, int)
        List of hour ranges.

    Returns
    -------
    bool
    """
    for start, end in ranges:
        if start <= hour < end:
            return True
    return False


def ia1_is_open(date_any, hour: int) -> bool:
    """
    Determines whether IA1 is open for the supplied date and hour using
    configuration-based seasonal and baseline rules.

    Summer season rules come from config (IA1_SUMMER_HOURS), and apply only
    when the timestamp falls within the configured summer season window.

    Outside that period, the baseline rules in config apply.

    Parameters
    ----------
    date_any : datetime-like
        Date to evaluate.
    hour : int
        Hour of day (0–23).

    Returns
    -------
    bool
        True if IA1 is open for the given date and hour.
    """
    ts = pd.to_datetime(date_any)
    weekday = ts.weekday()  # Monday=0 .. Sunday=6

    # Summer seasonal schedule
    if _in_summer_window(ts):
        day_ranges = IA1_SUMMER_HOURS.get(weekday, [])
        return _time_in_ranges(hour, day_ranges)

    # Baseline rules
    if weekday not in IA1_BASELINE_OPEN_DAYS:
        return False

    # Check daytime and night ranges
    if _time_in_ranges(hour, IA1_BASELINE_DAY_HOURS):
        return True
    if _time_in_ranges(hour, IA1_BASELINE_NIGHT_HOURS):
        return True

    return False


def ia2_is_open(*_, **__) -> bool:
    """
    Returns IA2 open status based on configuration.

    Returns
    -------
    bool
    """
    return IA2_ALWAYS_OPEN


def peak_immigration_day(flights: pd.DataFrame) -> Optional[pd.Timestamp]:
    """
    Determine the true Peak Immigration Day:
        = calendar day with the highest total International arrivals
          based on Immigration Arrival Time = Schedule + 20 minutes.

    This MUST be bucket-independent (5 vs 10 vs 15 min should not matter).
    """

    # Keep only arrivals
    f = flights[flights["A/D"] == "A"].copy()
    if f.empty:
        return None

    # Compute immigration arrival timestamp
    f["Imm_Arrival"] = f["Schedule"] + pd.Timedelta(minutes=20)

    # Keep international only
    f_intl = f[f["Sector"] == "International"].copy()
    if f_intl.empty:
        return None

    # Group by immigration arrival date
    daily_total = (
        f_intl.groupby(f_intl["Imm_Arrival"].dt.date)["Pax"]
              .sum()
    )

    # Return the max-date
    return daily_total.idxmax()
