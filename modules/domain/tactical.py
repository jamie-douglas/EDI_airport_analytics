
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

def arrivals_per_slots(flights: pd.DataFrame, slot_minutes: int = 15) -> pd.DataFrame:
    """
    Convert flights into an arrivals grid at arbitraty slot resolution (e.g., 5, 10, 15 minutes) using Immigration Arrival Time (Schedule + 20 mins)
    Steps
    -----
    1) Keep arrivals ('A') and ensure 'Schedule' to datetime.
    2) Compute 'Immigration Arrival' = 'Schedule' + 20 minutes.
    3) Floor to slot_minute buckets (e.g., 5min, 15min) _> new timestamp column)
    4) Extract canonical 'Date' and 'Hour' (and 'Hour_Label') from this bucket timestamp
    5) Aggregate passenger totals per (Date, 'Time_{slot_minutes}', Sector) and pivot wide bysector
    6) Compute a rolling 60-minute International arrivals per day using window = 60/ slot_minutes

    Parameters
    ----------
    flights : pandas.DataFrame
        Columns:
          - 'Schedule' (datetime-like)
          - 'A/D' ('A' or 'D')
          - 'Pax' (numeric)
          - 'Sector' (string)
    slot_minutes: int
        slot size in minutes (e.g. 5, 10, 15)

    Returns
    -------
    pandas.DataFrame
        One row per (Date, 'Time_{slot_minutes}') with Columns:
          - 'Date' (date)            : calendar day of the 15‑min bucket
          - 'Tim_{slot_minutes}' (datetime64[ns]): bucket timestamp
          - 'Hour' (int)             : 0..23, derived from 'Time_{slot_minutes}'
          - 'Hour_Label' (str)       : 'HH:00', derived from 'Time_{slot_minutes}'
          - one numeric column per sector (CTA, Domestic, International, ...)
          - 'Intl_Rolling_Hour' (float): rolling 60‑minute sum of 'International' by day
    """
    # 1) arrivals only, normalise
    df = flights[flights["A/D"] == "A"].copy()
    df = to_datetime(df, "Schedule")

    # 2) +20 min walk-time to immigration
    df["Immigration Arrival"] = df["Schedule"] + timedelta(minutes=20)

    

    # 3) floor to 15-min buckets
    freq = f"{slot_minutes}min"
    time_col = f"Time_{slot_minutes}"
    df = bucket_time(df, time_col="Immigration Arrival", freq=freq, out_col=time_col)

    # 4) derive canonical Date/Hour from the FLOORED timestamp
    df = add_date_parts(df, time_col)
    df["Date"] = df[f"{time_col}_date"]
    df["Hour"] = df[f"{time_col}_hour"]
    df["Hour_Label"] = df[f"{time_col}_hour_label"]

    # 5) totals per (Date, Time_{slot_minutes}, Sector) → wide
    totals = (
        df.groupby(["Date", time_col, "Sector"])["Pax"]
          .sum()
          .reset_index()
          .pivot(index=["Date", time_col], columns="Sector", values="Pax")
          .fillna(0)
          .reset_index()
          .sort_values(["Date", time_col])
    )

    # Add Hour and Hour_Label back to totals (derived from Time_{slot_minutes} to keep it canonical)
    parts = add_date_parts(totals, time_col)
    totals["Hour"] = parts[f"{time_col}_hour"]
    totals["Hour_Label"] = parts[f"{time_col}_hour_label"]

    # 6) rolling 60-minute sum of International per day
    window = int(60/slot_minutes)
    rolled = rolling_sum(
        totals,
        time_col=time_col,
        value_col="International",
        window=window,                     
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


def _ensure_slot_skeleton(day_df: pd.DataFrame, slot_minutes: int, day_col: str = "Date", slot_col: str = "SlotTime") -> pd.DataFrame:
    """
    Build a contiguous slot-sized grid for a single day (00:00...23:59),
    left-join incoming per-slot data, and default missing arrivals to 0

    Parameters
    ----------
    day_df : pandas.DataFrame
        Input DataFrame containing at least [day_col, slot_col] and (optionally) 'International'.
    slot_minutes : int
        Slot size in minutes (used to build full-day grid).
    day_col : str, default "Date"
        Column that holds the calendar day.
    slot_col : str, default "SlotTime"
        Column that holds the left edge of the slot (datetime64[ns]).

    Returns
    -------
    pandas.DataFrame
        A dull day's worth of slots (e.g. 288 rows for 5-min slots)
    """
    x = day_df.copy().sort_values(slot_col)
    if x.empty:
        return x

    d = x[day_col].iloc[0]
    day_start = pd.to_datetime(d)

    #Full day grid
    skeleton = pd.DataFrame({
        slot_col: pd.date_range(day_start, day_start + pd.Timedelta(days=1) - timedelta(minutes=slot_minutes), freq=f"{slot_minutes}min")
    })
    skeleton[day_col] = d
    skeleton["Hour"] = skeleton[slot_col].dt.hour

    cols_to_merge = [c for c in x.columns if c in {day_col, slot_col, "International"}]
    out = skeleton.merge(x[cols_to_merge], on=[day_col, slot_col], how="left")
    out["International"] = pd.to_numeric(out.get("International", 0), errors="coerce").fillna(0)
    return out


def immigration_queue_slots(
    slots_df: pd.DataFrame,
    peak_day: object,
    slot_minutes: int = 15,
    ia1_tph: float = IA1_TPH,
    ia2_tph: float = IA2_TPH,
    ia1_cax: float = IA1_CAX,
    ia2_cax: float = IA2_CAX,
) -> pd.DataFrame:
    """
    Model a simple immigration queue at arbitraty slot resolution (5,10, 15 minutes).

    Queue Equation:
    ---------------
        Q_next = max(0, arrivals_t + Q_prev - throughput_t),
    where throughput_t = (ia2_tph + IA1_Open * ia1_tph) * slot_minutes/60).

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

    time_col = f"Time_{slot_minutes}"

    df = (
        slots_df[slots_df["Date"] == peak_day]
        .copy()
        .sort_values(time_col)
    )

    if df.empty:
        return df
    
    #Build full-day slot grid (handles missing boundary slots)
    df = _ensure_slot_skeleton(df, slot_minutes, day_col="Date", slot_col=time_col)

    #Throughput fraction per slot: slot_minutes / 60
    factor = slot_minutes / 60.0

    # IA1 open rule — choose ONE of the following:
    # A) Left‑edge semantics (slot is open if its START hour is open) — recommended:
    df["IA1_Open"] = df["Hour"].apply(lambda h: bool(ia1_is_open(peak_day, int(h))))

    # B) Right‑edge semantics (slot is open if its END lies in an open hour) — alternative:
    # df["Slot_End"] = pd.to_datetime(df["Time_15"]) + pd.Timedelta(minutes=15)
    # df["IA1_Open"] = df["Slot_End"].dt.hour.apply(lambda h: bool(ia1_is_open(peak_day, int(h))))

    # Per‑slot throughput and capacity
    df["Throughput"] = (ia2_tph + df["IA1_Open"].astype(int) * ia1_tph) * factor
    df["Capacity"]      =  ia2_cax + df["IA1_Open"].astype(int) * ia1_cax

    # Simple overflow recursion
    overflow = []
    q_prev = 0.0
    for _, r in df.iterrows():
        arrivals = float(r.get("International", 0.0))
        q = max(0.0, arrivals + q_prev - float(r["Throughput"]))
        overflow.append(q)
        q_prev = q
    df["Overflow"] = overflow

    df["slot_minutes"] = slot_minutes

    return df[
        ["Date", time_col, "Hour", "International",
         "IA1_Open", "Throughput", "Capacity", "Overflow", "slot_minutes"]
    ]

def _ensure_15min_skeleton(day_df: pd.DataFrame,
                           day_col="Date",
                           slot_col="Time_15") -> pd.DataFrame:
    """
    Build a contiguous per 15 minute grid for a single day

    Parameters
    ----------
    day_df : pandas.DataFrame
        Input DataFrame containing at least [day_col, slot_col] and (optionally) 'International'.
    day_col : str, default "Date"
        Column that holds the calendar day.
    slot_col : str, default "Time_15"
        Column that holds the left edge of the 15‑minute slot (datetime64[ns]).

    Returns
    -------
    pandas.DataFrame
        A dull day's worth of slots (e.g. 288 rows for 5-min slots)
    """
    x = day_df.copy().sort_values(slot_col)
    if x.empty:
        return x

    d = x[day_col].iloc[0]
    day_start = pd.to_datetime(d)

    skeleton = pd.DataFrame({
        slot_col: pd.date_range(
            day_start,
            day_start + pd.Timedelta(days=1) - pd.Timedelta(minutes=15),
            freq="15min"
        )
    })
    skeleton[day_col] = d
    skeleton["Hour"] = skeleton[slot_col].dt.hour

    out = skeleton.merge(x[[day_col, slot_col, "International"]], 
                         on=[day_col, slot_col], how="left")
    out["International"] = out["International"].fillna(0)
    return out


def immigration_queue_15m_all_days(
    pax_15: pd.DataFrame,
    ia1_tph: float = IA1_TPH,
    ia2_tph: float = IA2_TPH,
    ia1_cax: float = IA1_CAX,
    ia2_cax: float = IA2_CAX,
) -> pd.DataFrame:
    """
    Compute the 15-minute immigration queue for every day present in pax_15.

    Parameters
    ----------
    pax_15 : pandas.DataFrame
        Output of arrivals_per_15min(...), including:
        - 'Date' (date), 'Time_15' (datetime64[ns]), 'Hour' (int),
        - 'International' (numeric)
    ia1_tph, ia2_tph, ia1_cax, ia2_cax : float
        Capacity/throughput constants (from config).

    Returns
    -------
    pandas.DataFrame
        One row per 15-min slot for each day:
        ['Date','Time_15','Hour','International','IA1_Open','Throughput_15','Capacity','Overflow']
    """
    if pax_15.empty:
        return pax_15.copy()

    out = []
    for d in sorted(pax_15["Date"].unique()):
        day = pax_15[pax_15["Date"] == d][["Date", "Time_15", "International"]].copy()
        day = _ensure_15min_skeleton(day, day_col="Date", slot_col="Time_15")
        day["IA1_Open"] = day["Hour"].apply(lambda h: bool(ia1_is_open(d, int(h))))
        day["Throughput_15"] = (ia2_tph + day["IA1_Open"].astype(int) * ia1_tph) / 4.0
        day["Capacity"]      =  ia2_cax + day["IA1_Open"].astype(int) * ia1_cax

        overflow, prev = [], 0.0
        for _, r in day.iterrows():
            arrivals = float(r.get("International", 0.0))
            q = max(0.0, arrivals + prev - float(r["Throughput_15"]))
            overflow.append(q); prev = q
        day["Overflow"] = overflow
        out.append(day)

    return pd.concat(out, ignore_index=True)


def immigration_queue_slots_all_days(
    slots_df: pd.DataFrame,
    slot_minutes: int,
    ia1_tph: float = IA1_TPH,
    ia2_tph: float = IA2_TPH,
    ia1_cax: float = IA1_CAX,
    ia2_cax: float = IA2_CAX,
) -> pd.DataFrame:
    """
    Compute the immigration queue at arbitrary slot resolution (5, 10, 15 minutes)
    for *every* day present in a slot‑sized arrivals grid.

    Steps
    -----
    1) Identify all distinct calendar days in the slot arrivals table.
    2) For each day:
         a) Extract that day's slot‑level arrivals.
         b) Run immigration_queue_slots(...) for the full day.
    3) Concatenate results across all days into a single DataFrame.

    Parameters
    ----------
    slots_df : pandas.DataFrame
        Output of arrivals_per_slots(...), must include for all days:
          - 'Date'              (date)
          - 'Time_X'            (datetime64)
          - 'Hour'              (int: 0..23)
          - 'International'     (numeric arrivals per slot)

    slot_minutes : int
        Slot size in minutes (e.g., 5, 10, 15). Determines throughput fraction
        and number of slots per day (e.g., 288 slots if 5 minutes).

    ia1_tph, ia2_tph : float
        Hourly throughputs for IA1 and IA2 (from config).

    ia1_cax, ia2_cax : float
        Capacity (hall size) for IA1 and IA2 (from config).

    Returns
    -------
    pandas.DataFrame
        Concatenated queue results for all days, with columns:
          - 'Date'
          - 'Time_{slot_minutes}'
          - 'Hour'
          - 'International'
          - 'IA1_Open'     (bool)
          - 'Throughput'   (float per slot)
          - 'Capacity'     (float)
          - 'Overflow'     (float)
        One row per slot per day.
    """
    if slots_df.empty:
        return pd.DataFrame()

    out = []
    for d in sorted(slots_df["Date"].unique()):
        day_df = slots_df[slots_df["Date"] == d]
        q = immigration_queue_slots(
            day_df,
            peak_day=d,
            slot_minutes=slot_minutes,
            ia1_tph=ia1_tph,
            ia2_tph=ia2_tph,
            ia1_cax=ia1_cax,
            ia2_cax=ia2_cax,
        )
        out.append(q)

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()



def immigration_overflow_windows(
    imm_all: pd.DataFrame, 
    time_col: str,
    criterion: str = "queue_gt_capacity",  # "queue_gt_capacity" | "queue_gt_zero" | "rolling_gt_throughput"
) -> pd.DataFrame:
    """
    Compress breach periods into contiguous windows per day for ANY slot size

    criterion:
      - "queue_gt_capacity"   : breach when Overflow > Capacity      (matches the chart)
      - "queue_gt_zero"       : breach when Overflow > 0             (legacy throughput view)
      - "rolling_gt_throughput": breach when rolling 60‑min INTL > rolling 60‑min throughput

    Parameters
    ----------
    imm_all:
        Must include:
            - 'Date
            - dynamic timestamp column (time_col)
            -'Overflow'
            -'Capacity'
            -'International'
            -'Throughput' (slot-based) or Throughput_15 (legacy)
            -optional: 'slot_minutes
    time_col : str
        timestamp column to use e.g. 'Time_5', 'Time_10'
    criterion: str
        Breach definition:
        - "queue_gt_capacity"   : breach when Overflow > Capacity      (matches the chart)
        - "queue_gt_zero"       : breach when Overflow > 0             (legacy throughput view)
        - "rolling_gt_throughput": breach when rolling 60‑min INTL > rolling 60‑min throughput

    Returns a table with ['Date','Start','End','Duration_Minutes','Max_Overflow'].
    """
    #Handle empty input
    if imm_all.empty:
        return pd.DataFrame(columns=["Date","Start","End","Duration_Minutes","Max_Overflow"])

    #sort by date and dynamic timestamp
    x = imm_all.sort_values(["Date", time_col]).copy()
    x[time_col] = pd.to_datetime(x[time_col], errors="coerce")

    #clean numeric columns
    x["Overflow"] = pd.to_numeric(x["Overflow"], errors="coerce")
    x["Capacity"] = pd.to_numeric(x["Capacity"], errors="coerce")

    # ---- choose breach mask
    if criterion == "queue_gt_capacity":
        x["__breach__"] = x["Overflow"] > x["Capacity"]

    elif criterion == "queue_gt_zero":
        x["__breach__"] = x["Overflow"] > 0

    elif criterion == "rolling_gt_throughput":
        # Determine throughout column name
        if "Throughput" in x.columns:
            thr_col = "Throughput" #slot-based
        else:
            thr_col = "Throughput_15" #legacy fall back
        
        #determine slots per hour (rolling 60 min window width)
        if "slot_minutes" in x.columns and x["slot_minutes"].notna().any():
            sm = int(pd.to_numeric(x["slot_minutes"], errors="coerce").dropna().iloc[0])
            slots_per_hour = max(1, int(round( 60/sm)))
        else:
            slots_per_hour = 4 #15 min legacy

        x["International"] = pd.to_numeric(x.get("International", 0), errors="coerce")
        x[thr_col] = pd.to_numeric(x.get(thr_col, 0), errors="coerce")

        #60 minutes rolling sums
        x["__intl_roll__"] = (
            x.sort_values(["Date", time_col])
             .groupby("Date")["International"]
             .rolling(window=slots_per_hour, min_periods=1)
             .sum()
             .reset_index(level=0, drop=True)
        )
        x["__thr_roll__"] = (
            x.sort_values(["Date", time_col])
             .groupby("Date")[thr_col]
             .rolling(window=slots_per_hour, min_periods=1)
             .sum()
             .reset_index(level=0, drop=True)
        )
        x["__breach__"] = x["__intl_roll__"] > x["__thr_roll__"]
    else:
        raise ValueError(f"Unknown criterion: {criterion}")

    #build windows
    windows = []
    for d, df_day in x.groupby("Date", sort=True):
        times = df_day[time_col].to_numpy()
        flags = df_day["__breach__"].to_numpy()
        if len(times) == 0:
            continue

        #infer slot duration from data
        if len(times) > 1:
            slot = times[1] - times[0]
        else:
            slot = pd.Timedelta(minutes=15) #fallback only
        
        start_i = None

        for i in range(len(flags) + 1):
            current = flags[i] if i < len(flags) else False  # sentinel closes
            if current and start_i is None:
                start_i = i
            if (not current) and start_i is not None:
                start = times[start_i]
                end   = times[i] if i < len(times) else times[-1] + slot
                seg   = df_day.iloc[start_i:i]
                seg_cap = pd.to_numeric(seg["Capacity"], errors="coerce").fillna(0)
                seg_down = seg["Overflow"] - seg_cap
                max_downhall = float(seg_down.max()) if len(seg_down) else 0.0
                dur   = int((end - start) / np.timedelta64(1, "m"))
                windows.append({"Date": d, "Start": start, "End": end,
                                "Duration_Minutes": dur, "Max_DownHall": max_downhall})
                start_i = None

    return pd.DataFrame(windows)