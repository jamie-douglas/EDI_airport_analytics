
# modules/domain/tactical.py
from __future__ import annotations

from datetime import timedelta
from typing import List, Tuple, Optional, Dict

import numpy as np
import pandas as pd

# utilities (no DB access here)
from modules.utils.dates import to_datetime, add_date_parts

# analytics helpers
from modules.analytics.timeseries import bucket_time, rolling_sum
from modules.analytics.peaks import peak_day
from modules.analytics.immigration import ia1_is_open

# config: IA1/IA2 throughput & capacity (centralised and editable)
from modules.config import IA1_TPH, IA2_TPH, IA1_CAX, IA2_CAX


# ======================================================================
# DAILY A/B/C SUMMARY
# ======================================================================

def daily_summary(
    flights: pd.DataFrame,
    a_threshold: Optional[float] = None,
    b_threshold: Optional[float] = None,
) -> Tuple[pd.DataFrame, float, float]:
    """
    Build a daily arrivals/departures summary and label each day A/B/C.

    If thresholds are not provided, they are derived from the distribution of
    daily totals:
      - A threshold: 90th percentile (rounded to nearest 500)
      - B threshold: 50th percentile (rounded to nearest 500)

    Parameters
    ----------
    flights : pandas.DataFrame
        Columns:
          - 'Schedule' (datetime-like)
          - 'A/D' ('A' or 'D')
          - 'Pax' (numeric)
          - 'Sector' (string; not used here)
    a_threshold : float, optional
        Passenger threshold above which a day is ranked 'A'.
    b_threshold : float, optional
        Passenger threshold for 'B' days (between B and A inclusive).

    Returns
    -------
    (pandas.DataFrame, float, float)
        summary, a_threshold_used, b_threshold_used

        summary columns:
          'Schedule Date' (date), 'A', 'D', 'Total', 'Total_k', 'Ranking', 'Date_Label'

    Raises
    ------
    ValueError
        If A threshold is not strictly greater than B threshold.
    """
    df = flights.copy()
    df = to_datetime(df, "Schedule")
    df["Schedule Date"] = df["Schedule"].dt.date

    piv = (
        df.groupby(["Schedule Date", "A/D"])["Pax"]
          .sum()
          .reset_index()
          .pivot(index="Schedule Date", columns="A/D", values="Pax")
          .fillna(0)
          .reset_index()
    )

    piv["Total"] = piv.get("A", 0) + piv.get("D", 0)
    piv["Total_k"] = (piv["Total"] / 1000).round().astype(int).astype(str) + "k"

    if a_threshold is None or b_threshold is None:
        a_threshold = round((piv["Total"].quantile(0.9) / 500), 0) * 500
        b_threshold = round((piv["Total"].quantile(0.5) / 500), 0) * 500

    if a_threshold <= b_threshold:
        raise ValueError("A threshold must be greater than B threshold.")

    conds = [
        piv["Total"] > a_threshold,
        (piv["Total"] >= b_threshold) & (piv["Total"] <= a_threshold),
    ]
    piv["Ranking"] = np.select(conds, ["A", "B"], default="C")
    piv["Date_Label"] = pd.to_datetime(piv["Schedule Date"]).dt.strftime("%d-%b")
    return piv, float(a_threshold), float(b_threshold)


# ======================================================================
# ARRIVALS — HOURLY & 15-MIN
# ======================================================================

def arrivals_per_hour(flights: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a flights table to a 24‑slot hourly arrivals grid by sector, per day.

    Steps
    -----
    1) Keep arrivals ('A'), coerce 'Schedule' to datetime.
    2) Floor 'Schedule' to the hour into 'Schedule_H'.
    3) Derive day parts from 'Schedule_H' (date, hour, label).
    4) Build a 24‑slot skeleton per day and pivot sector totals wide.

    Parameters
    ----------
    flights : pandas.DataFrame
        Columns:
          - 'Schedule' (datetime-like)
          - 'A/D' ('A' or 'D')
          - 'Pax' (numeric)
          - 'Sector' (string)

    Returns
    -------
    pandas.DataFrame
        Columns:
          - 'Date' (date)         : calendar day of the bucketed hour
          - 'Hour' (int)          : 0..23
          - 'Hour_Label' (str)    : 'HH:00'
          - one numeric column per sector (CTA, Domestic, International, ...)
    """
    # 1) arrivals only, normalise time
    df = flights[flights["A/D"] == "A"].copy()
    df = to_datetime(df, "Schedule")

    # 2) floor to hour
    df = bucket_time(df, time_col="Schedule", freq="h", out_col="Schedule_H")

    # 3) add date parts from the FLOORED timestamp; map to canonical 'Date'/'Hour'
    df = add_date_parts(df, "Schedule_H")
    df["Date"] = df["Schedule_H_date"]
    df["Hour"] = df["Schedule_H_hour"]
    df["Hour_Label"] = df["Schedule_H_hour_label"]

    # 4) full skeleton per day, 24 hours
    hours = pd.DataFrame({"Hour": range(24)})
    days = pd.DataFrame({"Date": sorted(df["Date"].dropna().unique())})
    skeleton = days.merge(hours, how="cross")
    skeleton["Hour_Label"] = skeleton["Hour"].map(lambda h: f"{h:02d}:00")

    # totals per day-hour-sector → wide
    totals = (
        df.groupby(["Date", "Hour", "Sector"])["Pax"]
          .sum()
          .reset_index()
          .pivot(index=["Date", "Hour"], columns="Sector", values="Pax")
          .fillna(0)
          .reset_index()
    )

    out = skeleton.merge(totals, on=["Date", "Hour"], how="left").fillna(0)
    return out




from datetime import timedelta
import pandas as pd
from modules.utils.dates import to_datetime, add_date_parts
from modules.analytics.timeseries import bucket_time, rolling_sum

def arrivals_per_15min(flights: pd.DataFrame) -> pd.DataFrame:
    """
    Convert flights to a 15‑minute arrivals grid at immigration arrival time.

    Steps
    -----
    1) Keep arrivals ('A') and coerce 'Schedule' to datetime.
    2) Compute 'Immigration Arrival' = 'Schedule' + 20 minutes.
    3) Floor to 15-minute buckets as 'Time_15'.
    4) Derive canonical 'Date' and 'Hour' (and 'Hour_Label') from 'Time_15'.
    5) Aggregate passenger totals per (Date, Time_15, Sector) and pivot wide.
    6) Compute a per-day rolling 60-minute sum for 'International' (4 × 15 min).

    Parameters
    ----------
    flights : pandas.DataFrame
        Columns:
          - 'Schedule' (datetime-like)
          - 'A/D' ('A' or 'D')
          - 'Pax' (numeric)
          - 'Sector' (string)

    Returns
    -------
    pandas.DataFrame
        Columns:
          - 'Date' (date)            : calendar day of the 15‑min bucket
          - 'Time_15' (datetime64[ns]): 15‑minute bucket timestamp
          - 'Hour' (int)             : 0..23, derived from 'Time_15'
          - 'Hour_Label' (str)       : 'HH:00', derived from 'Time_15'
          - one numeric column per sector (CTA, Domestic, International, ...)
          - 'Intl_Rolling_Hour' (float): rolling 60‑minute sum of 'International' by day
    """
    # 1) arrivals only, normalise
    df = flights[flights["A/D"] == "A"].copy()
    df = to_datetime(df, "Schedule")

    # 2) +20 min walk-time to immigration
    df["Immigration Arrival"] = df["Schedule"] + timedelta(minutes=20)

    # 3) floor to 15-min buckets
    df = bucket_time(df, time_col="Immigration Arrival", freq="15min", out_col="Time_15")

    # 4) derive canonical Date/Hour from the FLOORED timestamp
    df = add_date_parts(df, "Time_15")
    df["Date"] = df["Time_15_date"]
    df["Hour"] = df["Time_15_hour"]
    df["Hour_Label"] = df["Time_15_hour_label"]

    # 5) totals per (Date, Time_15, Sector) → wide
    totals = (
        df.groupby(["Date", "Time_15", "Sector"])["Pax"]
          .sum()
          .reset_index()
          .pivot(index=["Date", "Time_15"], columns="Sector", values="Pax")
          .fillna(0)
          .reset_index()
          .sort_values(["Date", "Time_15"])
    )

    # Add Hour and Hour_Label back to totals (derived from Time_15 to keep it canonical)
    parts = add_date_parts(totals, "Time_15")
    totals["Hour"] = parts["Time_15_hour"]
    totals["Hour_Label"] = parts["Time_15_hour_label"]

    # 6) rolling 60-minute sum of International (4 × 15min) per day
    rolled = rolling_sum(
        totals,
        time_col="Time_15",
        value_col="International",
        window=4,                      # 4 × 15 min = 60 min
        out_col="Intl_Rolling_Hour",
        groupby_keys=["Date"],
    )
    totals["Intl_Rolling_Hour"] = rolled["Intl_Rolling_Hour"]

    return totals



# ======================================================================
# PEAKS & SECURITY
# ======================================================================

def peak_arrival_day(hourly_arrivals: pd.DataFrame, sector_cols: List[str]) -> Tuple[pd.Timestamp, float]:
    """
    Identify the date with the maximum total arrivals across selected sectors.

    Parameters
    ----------
    hourly_arrivals : pandas.DataFrame
        Output of arrivals_per_hour(...).
    sector_cols : list[str]
        Sector columns to include in the total.

    Returns
    -------
    (pandas.Timestamp, float)
        peak_day, total_arrivals_on_peak_day

    Notes
    -----
    Uses analytics.peaks.peak_day to handle NA/empty safely.
    """
    df = hourly_arrivals.copy()
    df["Hourly Total"] = df[sector_cols].sum(axis=1)
    daily = df.groupby("Date")["Hourly Total"].sum()
    d, v = peak_day(daily)
    return d, float(v)


def peak_security_day(security: pd.DataFrame) -> dict:
    """
    Find the day with the highest total *security* passengers.

    This works on the raw 15‑minute (or forecast) security series by:
      - summing 'Pax' across each calendar day,
      - returning the day with the largest total and its value.

    IMPORTANT
    ---------
    - This is intentionally different from the *peak rolling hour*:
      • peak_security_day(...) → highest *daily* total 'Pax' (may be on Day X)
      • peak_security_hour(...) → highest *60‑minute window* (may be on Day Y)

    Parameters
    ----------
    security : pandas.DataFrame
        Security table with columns:
        - 'Date' (date): calendar day of each row
        - 'Pax' (numeric): forecast passenger count for that row

    Returns
    -------
    dict
        {
          'Date'  : date,
          'Total' : float
        }
    """
    per_day = security.groupby("Date")["Pax"].sum()
    if per_day.empty:
        return {"Date": pd.NaT, "Total": 0.0}
    peak_day = per_day.idxmax()
    peak_total = float(per_day.loc[peak_day])
    return {"Date": peak_day, "Total": peak_total}



def security_rolling_hour(
    security: pd.DataFrame,
    window_slots: int = 4,
    opening_hour: int = 3,
) -> pd.DataFrame:
    """
    Calculate 60‑minute rolling sums for 'Pax', 'Staff', and 'Total' after an opening hour.

    Parameters
    ----------
    security : pandas.DataFrame
        Columns: 'Forecast DateTime' (15‑minute grid), 'Pax', 'Staff', 'Total', 'Date'
    window_slots : int, default 4
        Count of 15‑minute slots in the rolling window (4 ⇒ 60 minutes).
    opening_hour : int, default 3
        Discard rows before this local hour.

    Returns
    -------
    pandas.DataFrame
        Same rows with added:
          'Rolling Hour Pax', 'Rolling Hour Staff', 'Rolling Hour Total'
    """
    
    df = security[security["Forecast DateTime"].dt.hour >= opening_hour].copy()
    df = df.sort_values(["Date", "Forecast DateTime"])

    # Ensure types align for merges
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    key_cols = ["Date", "Forecast DateTime"]

    for src, out in [("Pax", "Rolling Hour Pax"),
                     ("Staff", "Rolling Hour Staff"),
                     ("Total", "Rolling Hour Total")]:

        rolled = rolling_sum(
            df.copy(),  # safe copy for function internals
            time_col="Forecast DateTime",
            value_col=src,
            window=window_slots,
            out_col=out,
            groupby_keys=["Date"],
        )

        # Expect the rolled frame to include Date + time + out
        # Coerce same key types
        rolled["Date"] = pd.to_datetime(rolled["Date"]).dt.date

        # Keep only keys + out_col, then merge
        df = df.merge(
            rolled[key_cols + [out]],
            on=key_cols,
            how="left",
            validate="one_to_one"
        )

    return df



def peak_security_hour(security_rh: pd.DataFrame) -> Dict[str, object]:
    """
    Extract the highest rolling‑hour window and its metrics from a security series.

    Parameters
    ----------
    security_rh : pandas.DataFrame
        Output of security_rolling_hour(...).

    Returns
    -------
    dict
        {
          'Date'         : date,
          'Window Start' : 'HH:MM',
          'Window End'   : 'HH:MM',
          'Pax RH'       : int,
          'Staff RH'     : int,
          'Total RH'     : int
        }
    """
    peak = security_rh.loc[security_rh["Rolling Hour Total"].idxmax()]
    center = peak["Forecast DateTime"]
    return {
        "Date": peak["Date"],
        "Window Start": (center - pd.Timedelta(minutes=45)).strftime("%H:%M"),
        "Window End":   (center + pd.Timedelta(minutes=15)).strftime("%H:%M"),
        "Pax RH":   int(peak["Rolling Hour Pax"]),
        "Staff RH": int(peak["Rolling Hour Staff"]),
        "Total RH": int(peak["Rolling Hour Total"]),
    }


def security_peak_utilisation(peak_info: Dict[str, object], capacity_line: float) -> float:
    """
    Compute the utilisation percentage at the security peak rolling-hour window.

    Parameters
    ----------
    peak_info : dict
        Output of peak_security_hour(...), must include 'Total RH'.
    capacity_line : float
        Reference rolling-hour capacity (e.g., SECURITY_CAX).

    Returns
    -------
    float
        Utilisation percentage (0..100). Returns 0.0 if capacity_line <= 0.
    """
    total_rh = float(peak_info.get("Total RH", 0))
    if capacity_line <= 0:
        return 0.0
    return (total_rh / capacity_line) * 100.0



# ======================================================================
# IMMIGRATION — 15‑MIN QUEUE (TPH/4)
# ======================================================================


def _ensure_15min_skeleton(day_df: pd.DataFrame, day_col: str = "Date", slot_col: str = "Time_15") -> pd.DataFrame:
    """
    Ensure a contiguous 15‑minute grid for a single day (00:00..23:45), left‑joining
    any existing per‑slot arrivals and defaulting missing values to zero.

    Parameters
    ----------
    day_df : pandas.DataFrame
        Input DataFrame containing at least [day_col, slot_col] and (optionally) 'International'.
        All rows must belong to the same day.
    day_col : str, default "Date"
        Column that holds the calendar day.
    slot_col : str, default "Time_15"
        Column that holds the left edge of the 15‑minute slot (datetime64[ns]).

    Returns
    -------
    pandas.DataFrame
        A 96‑row (00:00..23:45) 15‑minute skeleton for the day with columns:
        [day_col, slot_col, 'Hour', 'International' (filled to 0 if missing)].
    """
    x = day_df.copy().sort_values(slot_col)
    if x.empty:
        return x

    d = x[day_col].iloc[0]
    day_start = pd.to_datetime(d)
    skeleton = pd.DataFrame({
        slot_col: pd.date_range(day_start, day_start + pd.Timedelta(hours=23, minutes=45), freq="15min")
    })
    skeleton[day_col] = d
    skeleton["Hour"] = skeleton[slot_col].dt.hour

    cols_to_merge = [c for c in x.columns if c in {day_col, slot_col, "International"}]
    out = skeleton.merge(x[cols_to_merge], on=[day_col, slot_col], how="left")
    out["International"] = pd.to_numeric(out.get("International", 0), errors="coerce").fillna(0)
    return out


def immigration_queue_15m(
    arrivals_15: pd.DataFrame,
    peak_day: object,
    ia1_tph: float = IA1_TPH,
    ia2_tph: float = IA2_TPH,
    ia1_cax: float = IA1_CAX,
    ia2_cax: float = IA2_CAX,
) -> pd.DataFrame:
    """
    Model a simple immigration queue over 15‑minute slots for one day.

    The queue evolves per slot t as:
        Q_next = max(0, arrivals_t + Q_prev - throughput_t),
    where
        throughput_t = (ia2_tph + IA1_Open * ia1_tph) / 4.

    IA1_Open is evaluated per slot using an hourly rule:
        ia1_is_open(peak_day, Hour).

    Parameters
    ----------
    arrivals_15 : pandas.DataFrame
        Output of arrivals_per_15min(...); must include:
          'Date', 'Time_15', 'Hour', 'International' (numeric).
    peak_day : date-like
        Day to simulate (usually the peak International day).
    ia1_tph : float, default from config
        IA1 hourly throughput.
    ia2_tph : float, default from config
        IA2 hourly throughput.
    ia1_cax : float, default from config
        IA1 hall capacity (for plotting a capacity line).
    ia2_cax : float, default from config
        IA2 hall capacity (for plotting a capacity line).

    Returns
    -------
    pandas.DataFrame
        Subset for 'peak_day' with added columns:
          'IA1_Open' (bool), 'Throughput_15' (float), 'Capacity' (float), 'Overflow' (float)
    """
    # Slice the selected day and ensure a full 96‑slot (00:00..23:45) grid so capacity/shading
    # align exactly to IA1 open/close edges even when a boundary slot has zero arrivals.
    df = arrivals_15[arrivals_15["Date"] == peak_day].copy().sort_values("Time_15")
    if df.empty:
        return df
    df = _ensure_15min_skeleton(df, day_col="Date", slot_col="Time_15")

    # IA1 open rule — choose ONE of the following:
    # A) Left‑edge semantics (slot is open if its START hour is open) — recommended:
    df["IA1_Open"] = df["Hour"].apply(lambda h: bool(ia1_is_open(peak_day, int(h))))

    # B) Right‑edge semantics (slot is open if its END lies in an open hour) — alternative:
    # df["Slot_End"] = pd.to_datetime(df["Time_15"]) + pd.Timedelta(minutes=15)
    # df["IA1_Open"] = df["Slot_End"].dt.hour.apply(lambda h: bool(ia1_is_open(peak_day, int(h))))

    # Per‑slot throughput and capacity
    df["Throughput_15"] = (ia2_tph + df["IA1_Open"].astype(int) * ia1_tph) / 4.0
    df["Capacity"]      =  ia2_cax + df["IA1_Open"].astype(int) * ia1_cax

    # Simple overflow recursion
    overflow = []
    q_prev = 0.0
    for _, r in df.iterrows():
        arrivals = float(r.get("International", 0.0))
        q = max(0.0, arrivals + q_prev - float(r["Throughput_15"]))
        overflow.append(q)
        q_prev = q
    df["Overflow"] = overflow
    return df
