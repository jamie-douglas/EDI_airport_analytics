#modules/domain/checkin.py
import pandas as pd

#Generic loader + Excel writer

#Analytics helpers
from modules.analytics.timeseries import bucket_time, rolling_sum, peak_rolling_window
from modules.analytics.penetration import simple_penetration



# ====================================================================================
# ANALYTICS
# ====================================================================================


def compute_peak_rolling_hour(cupps_df: pd.DataFrame, kiosk_df: pd.DataFrame):
    """
    Computes peak 1-hour passenger window from CUPPS + SITA data using 5-minute buckets.

    CUPPS buckets on 'FirstPNRAction' (start of interaction), SITA on 'Start DateTime'.
    Merges per-5-minute totals, computes a time-based rolling 60-min window, and
    returns the peak window statistics.

    Parameters
    ----------
    cupps_df : pandas.DataFrame
        CUPPS transactions DataFrame including 'FirstPNRAction' and 'CIP At EGate'.
    kiosk_df : pandas.DataFrame
        Kiosk transactions DataFrame including 'Start DateTime' and 'Flight ID'.

    Returns
    -------
    (pandas.DataFrame, float, pandas.Timestamp, pandas.Timestamp)
        Tuple of:
        - roll_df : DataFrame with columns ['Bucket', 'CUPPS_Pax', 'SITA_Pax', 'Total_Pax', 'Rolling1h']
        - peak_value : float, maximum rolling-hour passenger count
        - window_start : pandas.Timestamp, start of peak window (bucket-based)
        - window_end : pandas.Timestamp, end of last 5-min bucket in the peak window
    """
    # Bucket to 5-minute intervals
    cup = bucket_time(cupps_df, "FirstPNRAction", "5min", out_col="Bucket5")
    kis = bucket_time(kiosk_df, "Start DateTime", "5min", out_col="Bucket5")

    # Aggregate per bucket
    cup_agg = (
        cup.groupby("Bucket5")["CIP At EGate"]
           .sum()
           .rename("CUPPS_Pax")
           .reset_index()
    )
    kis_agg = (
        kis.groupby("Bucket5")["Flight ID"]
           .count()
           .rename("SITA_Pax")
           .reset_index()
    )

    # Merge and compute totals
    pax = cup_agg.merge(kis_agg, on="Bucket5", how="outer").fillna(0.0)
    pax = pax.rename(columns={"Bucket5": "Bucket"})
    pax["Total_Pax"] = pax["CUPPS_Pax"] + pax["SITA_Pax"]

    # Rolling 60 minutes (time-based)
    roll = rolling_sum(
        pax, time_col="Bucket", value_col="Total_Pax",
        window="60min", out_col="Rolling1h"
    )

    # Peak window (12×5min buckets; end = final bucket start + 5min)
    peak_value, window_start, window_end = peak_rolling_window(
        roll, time_col="Bucket", roll_col="Rolling1h",
        bucket_minutes=5, bucket_count=12
    )
    return roll, peak_value, window_start, window_end



def compute_durations(cupps_df: pd.DataFrame, kiosk_df: pd.DataFrame):
    """
    Computes average desk and kiosk transaction durations.

    Desk Rule:
        Duration per passenger = (NextPNRAction - FirstPNRAction) / throughput
        where throughput = max('Unique BP Prints', 'BT Prints'), only if > 0.
        Durations over 10 minutes are excluded.

    Kiosk Rule:
        Duration = End - Start
        Only durations > 0 seconds are retained.

    Parameters
    ----------
    cupps_df : pandas.DataFrame
        CUPPS DataFrame containing:
        - 'FirstPNRAction'
        - 'NextPNRAction'
        - 'Unique BP Prints'
        - 'BT Prints'
    kiosk_df : pandas.DataFrame
        Kiosk DataFrame containing:
        - 'Start DateTime'
        - 'End DateTime'

    Returns
    -------
    (float, float)
        Tuple of:
        - avg_desk_secs_per_pax : float
        - avg_kiosk_secs : float
    """
    # ----------------------
    # DESK DURATION (per passenger)
    # ----------------------
    desk = cupps_df.copy()
    desk["FirstPNRAction"] = pd.to_datetime(desk["FirstPNRAction"], errors="coerce")
    desk["NextPNRAction"] = pd.to_datetime(desk["NextPNRAction"], errors="coerce")

    # Raw transaction duration
    desk["TransactionSeconds"] = (
        desk["NextPNRAction"] - desk["FirstPNRAction"]
    ).dt.total_seconds()

    # Compute throughput = max(Unique BP Prints, BT Prints)
    desk["Throughput"] = desk[["Unique BP Prints", "BT Prints"]].max(axis=1)

    # Only divide when throughput > 0
    desk = desk[desk["Throughput"] > 0].copy()
    desk["SecondsPerPax"] = desk["TransactionSeconds"] / desk["Throughput"]

    # Remove outliers > 10 minutes per pax
    desk = desk[desk["SecondsPerPax"] <= 10 * 60]

    # Mean desk duration per passenger
    avg_desk_secs_per_pax = float(desk["SecondsPerPax"].mean()) if not desk.empty else float("nan")

    # ----------------------
    # KIOSK DURATION (>0 only)
    # ----------------------
    kis = kiosk_df.copy()
    kis["Start DateTime"] = pd.to_datetime(kis["Start DateTime"], errors="coerce")
    kis["End DateTime"] = pd.to_datetime(kis["End DateTime"], errors="coerce")

    kis["TransactionSeconds"] = (
        kis["End DateTime"] - kis["Start DateTime"]
    ).dt.total_seconds()

    kis = kis[kis["TransactionSeconds"] > 0]     # your rule
    avg_kiosk_secs = float(kis["TransactionSeconds"].mean()) if not kis.empty else float("nan")

    return avg_desk_secs_per_pax, avg_kiosk_secs


def compute_penetration(cupps_df: pd.DataFrame, kiosk_df: pd.DataFrame, flights_df: pd.DataFrame):
    """
    Computes penetration rate for the loaded period.

    Penetration = (desk CIP sum + number of kiosk transactions) / total flight passengers.

    Parameters
    ----------
    cupps_df : pandas.DataFrame
        CUPPS DataFrame containing 'CIP At EGate'.
    kiosk_df : pandas.DataFrame
        Kiosk DataFrame (row count used as kiosk transactions).
    flights_df : pandas.DataFrame
        Flight DataFrame containing 'Pax'.

    Returns
    -------
    (float, pandas.DataFrame)
        Tuple of:
        - rate : float, penetration rate
        - summary_df : DataFrame with columns ['Total_Numerator','Total_Denominator','Penetration_Rate']
    """
    numerator_df = pd.DataFrame({
        "pax_equiv": [cupps_df["CIP At EGate"].sum() + len(kiosk_df)]
    })
    rate, summary_df = simple_penetration(
        numerator_df=numerator_df, numerator_col="pax_equiv",
        denominator_df=flights_df, denominator_col="Pax"
    )
    return rate, summary_df



def compute_apr_sept_sidecheck(cupps_df: pd.DataFrame,
                               kiosk_df: pd.DataFrame,
                               flights_df: pd.DataFrame,
                               year_start: int):
    """
    Computes a side-check penetration for Apr 1 → Oct 1 within the same year as START.

    Parameters
    ----------
    cupps_df : pandas.DataFrame
        CUPPS DataFrame with 'Actual DateTime' and 'CIP At EGate'.
    kiosk_df : pandas.DataFrame
        Kiosk DataFrame with 'Start DateTime'.
    flights_df : pandas.DataFrame
        Flights DataFrame with 'Actual DateTime' and 'Pax'.
    year_start : int
        Year extracted from the main --start date.

    Returns
    -------
    (float, pandas.DataFrame)
        Tuple of:
        - rate : float, penetration for Apr→Sep
        - summary_df : DataFrame with the same schema as compute_penetration()
    """
    apr_start = pd.Timestamp(year_start, 4, 1)
    oct_start = pd.Timestamp(year_start, 10, 1)

    cup_sub = cupps_df[
        (pd.to_datetime(cupps_df["Actual DateTime"]) >= apr_start) &
        (pd.to_datetime(cupps_df["Actual DateTime"]) < oct_start)
    ]
    kis_sub = kiosk_df[
        (pd.to_datetime(kiosk_df["Start DateTime"]) >= apr_start) &
        (pd.to_datetime(kiosk_df["Start DateTime"]) < oct_start)
    ]
    flt_sub = flights_df[
        (pd.to_datetime(flights_df["Actual DateTime"]) >= apr_start) &
        (pd.to_datetime(flights_df["Actual DateTime"]) < oct_start)
    ]

    numerator_df = pd.DataFrame({
        "pax_equiv": [cup_sub["CIP At EGate"].sum() + len(kis_sub)]
    })
    rate, summary_df = simple_penetration(
        numerator_df=numerator_df, numerator_col="pax_equiv",
        denominator_df=flt_sub, denominator_col="Pax"
    )
    return rate, summary_df



