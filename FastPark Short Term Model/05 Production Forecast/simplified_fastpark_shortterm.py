"""
FastPark Production Forecast
===========================

This script builds a forward-looking forecast for FastPark entries and exits.
It is designed to be run as a production model: it loads the latest booking and
operations history, applies the learned forecast weights and parameters from the
earlier simulation work, and produces a forecast for the next 56 days starting
from the configured run date.

Typical output
--------------
The script produces daily and hourly forecast tables for entries and exits across
`current_run_date` through `current_run_date + forecast_lookahead_days`.
"""

# =============================================================================
# 0. IMPORTS
# =============================================================================

import sys
import pathlib
import time

# Standard library imports
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

# External library imports
import numpy as np
import pandas as pd

MODULE_ROOT = pathlib.Path(__file__).resolve().parents[2]


if str(MODULE_ROOT) not in sys.path:
    sys.path.append(str(MODULE_ROOT))

# Allowed utility imports (must be available in modules/utils)
# Ensure these files (db.py, query.py, progress.py) are accessible in sys.path
from modules.utils.db import get_engine
from modules.utils.query import query
from modules.utils.progress import step

# =============================================================================
# 1. GLOBAL CONFIGURATION FOR PRODUCTION
# =============================================================================

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[0]


if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

PRODUCTION_OUTPUT_DIR = PROJECT_ROOT / "config"
PRODUCTION_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


PRODUCTION_CONFIG = {
    # --- Database connection ---
    # The DSN must exist in the environment where this script runs.
    "db_dsn": "AzureConnection",
    "db_username": "jamie_douglas",

    # --- Forecast run settings ---
    # `current_run_date` is the date the forecast is being produced from.
    # Horizon 0 = today, horizon 1 = tomorrow, etc.
    # Leave as None to use the actual system date.
    "current_run_date": None,
    "forecast_lookahead_days": 56,

    # --- Learned model files ---
    # These CSV files are the outputs from the earlier simulation work and are used
    # to apply the optimised weights and parameters for the forecast.
    "forecast_weights_path": PRODUCTION_OUTPUT_DIR / "forecast_weights.csv",

    # --- Historical lookback ---
    # Use enough history to make the booking curves, trend components, and weekly
    # seasonality stable. The script loads all historical data up to the run date.
    "history_start_date": "2024-01-01",

    # --- FastPark data fields ---
    "asset_name": "FastPark",
    "valid_booking_status": "B",
    "cancelled_status": "CX",
    "unknown_statuses": ["F"],

    # Duration bands are used for the exit-booking completion model. These must stay
    # aligned with the historical duration logic used when the weights were trained.
    "duration_bins_days": [0, 1, 3, 7, 10, 14, 21, 999],
    "duration_labels": [
        "0-1 days", "2-3 days", "4-7 days", "8-10 days",
        "11-14 days", "15-21 days", "22+ days"
    ],

        
    "booking_id_col": "bookingId",
    "booking_created_timestamp_col": "createdAt",
    "cancellation_timestamp_col": "cancelledAt",
    "booking_entry_date_col": "entryDate",
    "booking_exit_date_col": "exitDate",
    "minimum_curve_observations": 10,




    # Operational timestamps used to reconcile bookings to actual movements.
    "actual_entry_timestamp_col": "CheckInEnded",
    "actual_exit_timestamp_col": "ActualCheckedOutDate",

    # --- Output files ---
    # These are the final daily and hourly outputs created by the production run.
    "daily_forecast_output_file": PRODUCTION_OUTPUT_DIR / "fastpark_daily_forecast.csv",
    "hourly_forecast_output_file": PRODUCTION_OUTPUT_DIR / "fastpark_hourly_forecast.csv",
}

# =============================================================================
# 2. UTILITY FUNCTIONS
# =============================================================================

# --- General Utilities ---

def clip_forecast(value: float) -> float:
    """
    Ensures forecast values are non-negative. Returns NaN for non-finite inputs.
    """
    if value is None or pd.isna(value):
        return np.nan
    return max(0.0, float(value))

def safe_mean(values: list | np.ndarray | pd.Series) -> float:
    """
    Calculates the mean, safely ignoring NaN and infinite values.
    Returns NaN if the series is empty after cleaning.
    """
    values = (
        pd.Series(values, dtype=float)
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    if values.empty:
        return np.nan
    return float(values.mean())

def weighted_average(values: list | np.ndarray | pd.Series, weights: list | np.ndarray | pd.Series) -> float:
    """
    Calculates a weighted average, automatically re-normalizing weights
    over only the available (non-NaN) values.
    """
    values = np.asarray(values, dtype=float)
    weights = np.asarray(weights, dtype=float)

    # Identify valid elements where both value and weight are finite and weight is non-negative
    valid = np.isfinite(values) & np.isfinite(weights) & (weights >= 0)

    if not valid.any():
        return np.nan # Return NaN if no valid pairs

    valid_values = values[valid]
    valid_weights = weights[valid]

    weight_total = valid_weights.sum()

    if weight_total <= 0:
        return np.nan # Avoid division by zero if all valid weights sum to zero

    # Calculate weighted average and clip to ensure non-negative output
    return clip_forecast(
        np.average(
            valid_values,
            weights=valid_weights,
        )
    )

def safe_ratio(numerator: float, denominator: float, default: float = np.nan) -> float:
    """
    Calculates a ratio, handling division by zero or NaN values gracefully.
    """
    if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return default
    return float(numerator) / float(denominator)


def get_base_component_names(flow: str) -> List[str]:
    """Return the four fixed Generation 2 components that receive weights."""
    if flow == "entry":
        return [
            "entry_booking_shrunk",
            "entry_weekday",
            "entry_month",
            "entry_trend_month",
        ]
    if flow == "exit":
        return [
            "exit_booking_shrunk",
            "exit_duration",
            "exit_weekday",
            "exit_trend_month",
        ]
    raise ValueError(f"Unknown flow: {flow}")

# --- Data Loading Functions (from Analysis Script) ---

def get_fastpark_bookings(
    start: str | None = None,
    end: str | None = None,
    statuses: list[str] | None = None,
    asset_name: str = "FastPark",
    engine=None,
):
    """
    Queries FastPark booking records from AirportX.v_Bookings.

    Purpose:
        This table represents booking intent:
        - when the booking was created
        - when the customer intends to enter
        - when the customer intends to exit
        - booking status
        - cancellation timestamp
        - price/product/channel information
        - declared inbound/outbound flight information

    Date filtering:
        Uses interval overlap on entryDate and exitDate:
            entryDate < end
            AND exitDate >= start
        This is important because a vehicle may enter before the analysis
        window but exit inside the analysis window.

    Status logic:
        B  = valid / booked
        CX = cancelled
        F  = unknown, analyse separately until confirmed

    Parameters
    ----------
    start : str, optional
        Inclusive start date.
    end : str, optional
        Exclusive end date.
    statuses : list[str], optional
        Booking statuses to include. If None, defaults to ["B", "CX", "F"].
    asset_name : str
        Asset name to filter to. Default is "FastPark".
    engine : optional
        SQLAlchemy engine (obtained via `get_engine`).

    Returns
    -------
    pandas.DataFrame
        FastPark booking records.
    """
    if statuses is None:
        statuses = ["B", "CX", "F"]

    columns = [
        "bookingUuid", "bookingId", "transactionId", "createdAt", "updatedAt",
        "cancelledAt", "channel", "productCode", "productGroup", "productName",
        "productPrice", "productQuantity", "productTotal", "entryDate", "exitDate",
        "status", "assetCode", "assetName", "bookingTotal", "leadtime", "duration",
        "nationality", "inboundAirline", "inboundFlight", "inboundRoute",
        "outboundAirline", "outboundFlight", "outboundRoute", "promoCode",
    ]

    where = ["assetName = :asset_name"]
    params = {"asset_name": asset_name}

    if statuses:
        status_placeholders = []
        for i, status in enumerate(statuses):
            key = f"status_{i}"
            status_placeholders.append(f":{key}")
            params[key] = status
        where.append(f"status IN ({', '.join(status_placeholders)})")

    df = query(
        table="AirportX.v_Bookings",
        columns=columns,
        where=where,
        params=params,
        date_column="entryDate",
        end_column="exitDate",
        start=start,
        end=end,
        overlap=True,
        order_by="entryDate, bookingId",
        engine=engine,
    )
    return df

def get_fastpark_entry_exits(
    start: str | None = None,
    end: str | None = None,
    engine=None,
):
    """
    Queries operational FastPark entry/exit records.

    Purpose:
        This table represents operational reality:
        - when the customer started kiosk check-in
        - when the customer finished kiosk check-in
        - when the key is available
        - expected/advised return date
        - actual checkout date
        - return flight information

    Operational interpretation:
        CheckInEnded: Customer finishes the kiosk process. This is when the key
                      is available and the car can be moved. Use for actual entry demand.
        ActualCheckedOutDate: Use for actual exit demand.

    Date filtering:
        A booking can enter before the analysis window and exit inside it.
        Uses or_events=True so that records are included if either:
            CheckInEnded is inside the window OR ActualCheckedOutDate is inside the window

    Parameters
    ----------
    start : str, optional
        Inclusive start date.
    end : str, optional
        Exclusive end date.
    engine : optional
        SQLAlchemy engine (obtained via `get_engine`).

    Returns
    -------
    pandas.DataFrame
        FastPark operational entry/exit records.
    """
    columns = [
        "BookingReference", "CheckInStarted", "CheckInEnded", "VehicleStatus",
        "CheckInDurationSecs", "ExpectedArrivalDate", "ExpectedReturnDate",
        "ExpectedReturnDateID", "ExpectedReturnTimeID", "ExpectedReturnZuluDateID",
        "ExpectedReturnZuluTimeID", "ReturnFlight", "ReturnFlightOperator",
        "ReturnFlightNumber", "ChromaFlightID", "ChromaDateTime_Local",
        "ActualCheckedOutDate", "CheckedInBy", "CheckedOutBy", "KeyLockerBox",
        "SourceFile", "RecordInsertedBy", "RecordInsertedDateTime",
        "RecordUpdatedBy", "RecordUpdatedDateTime",
    ]

    df = query(
        table="FastPark.v_EntryAndExits",
        columns=columns,
        where=None,
        params=None,
        date_column="CheckInEnded",
        end_column="ActualCheckedOutDate",
        start=start,
        end=end,
        or_events=True, # Important for capturing events that span the window.
        order_by="COALESCE(CheckInEnded, ActualCheckedOutDate), BookingReference",
        engine=engine,
    )
    return df

# --- Data Cleaning and Reconciliation Functions (from Analysis Script) ---

def clean_bookings(bookings: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Cleans and enriches the raw bookings DataFrame, adding date/time features,
    status flags, and recalculated metrics.

    Parameters:
        bookings (pd.DataFrame): Raw DataFrame from `AirportX.v_Bookings`.
        config (dict): Configuration dictionary (e.g., `PRODUCTION_CONFIG`).

    Returns:
        pd.DataFrame: Cleaned bookings DataFrame with derived fields.
    """
    df = bookings.copy()

    df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce")
    df["updatedAt"] = pd.to_datetime(df["updatedAt"], errors="coerce")
    df["cancelledAt"] = pd.to_datetime(df["cancelledAt"], errors="coerce")
    df["entryDate"] = pd.to_datetime(df["entryDate"], errors="coerce")
    df["exitDate"] = pd.to_datetime(df["exitDate"], errors="coerce")

    # Create boolean flags for booking statuses based on configuration.
    df["is_valid_booking"] = df["status"].eq(config["valid_booking_status"])
    df["is_cancelled"] = df["status"].eq(config["cancelled_status"])
    df["is_unknown_status"] = df["status"].isin(config["unknown_statuses"])

    # Extract date and hour components for easier grouping and analysis.
    df["booking_created_date"] = df["createdAt"].dt.date
    df["booking_created_hour"] = df["createdAt"].dt.hour
    df["planned_entry_date"] = df["entryDate"].dt.date
    df["planned_entry_hour"] = df["entryDate"].dt.hour
    df["planned_exit_date"] = df["exitDate"].dt.date
    df["planned_exit_hour"] = df["exitDate"].dt.hour

    # Recalculate lead time (days from booking creation to entry) and
    # planned duration (days from entry to exit) to ensure consistency.
    df["lead_time_days_calc"] = (df["entryDate"] - df["createdAt"]).dt.total_seconds() / 86400
    df["planned_duration_days_calc"] = (df["exitDate"] - df["entryDate"]).dt.total_seconds() / 86400

    # Add calendar features (weekday, month, year, week number) for seasonality.
    df["entry_weekday"] = df["entryDate"].dt.day_name()
    df["entry_weekday_num"] = df["entryDate"].dt.weekday
    df["entry_month"] = df["entryDate"].dt.month
    df["entry_year"] = df["entryDate"].dt.year
    df["entry_week"] = df["entryDate"].dt.isocalendar().week
    df["exit_weekday"] = df["exitDate"].dt.day_name()
    df["exit_month"] = df["exitDate"].dt.month

    # Categorize planned duration into predefined bands for segmented analysis.
    df["planned_duration_band"] = pd.cut(
        df["planned_duration_days_calc"],
        bins=config["duration_bins_days"],
        labels=config["duration_labels"],
        right=True,
        include_lowest=True
    ).astype(str)

    # --- Price Metrics ---
    # These are derived for potential future price-driven forecast components
    # or for diagnostic analysis. Not directly used in the current ensemble.
    df["booking_total_per_quantity"] = (
        df["bookingTotal"] / df["productQuantity"].replace(0, np.nan)
    )
    df["booking_total_per_planned_day"] = (
        df["bookingTotal"] / df["planned_duration_days_calc"].replace(0, np.nan)
    )
    df["booking_total_per_quantity_per_day"] = (
        df["booking_total_per_quantity"] / df["planned_duration_days_calc"].replace(0, np.nan)
    )
    df["product_price_per_planned_day"] = (
        df["productPrice"] / df["planned_duration_days_calc"].replace(0, np.nan)
    )

    return df


def clean_operations(operations: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Cleans and enriches the operational entry/exit DataFrame.
    (Copied from Analysis Script)

    Parameters:
        operations (pd.DataFrame): Raw DataFrame from `FastPark.v_EntryAndExits`.
        config (dict): Configuration dictionary (e.g., `PRODUCTION_CONFIG`).

    Returns:
        pd.DataFrame: Cleaned operations DataFrame with derived fields.
    """
    df = operations.copy()

    df["CheckInStarted"] = pd.to_datetime(df["CheckInStarted"], errors="coerce")
    df["CheckInEnded"] = pd.to_datetime(df["CheckInEnded"], errors="coerce")
    df["ExpectedArrivalDate"] = pd.to_datetime(df["ExpectedArrivalDate"], errors="coerce")
    df["ExpectedReturnDate"] = pd.to_datetime(df["ExpectedReturnDate"], errors="coerce")
    df["ActualCheckedOutDate"] = pd.to_datetime(df["ActualCheckedOutDate"], errors="coerce")
    df["ChromaDateTime_Local"] = pd.to_datetime(df["ChromaDateTime_Local"], errors="coerce")

    # Define the primary actual entry and exit timestamps based on config.
    df["actual_entry_ts"] = df[config["actual_entry_timestamp_col"]]
    df["actual_exit_ts"] = df[config["actual_exit_timestamp_col"]]

    # Extract date and hour components for operational events.
    df["actual_entry_date"] = df["actual_entry_ts"].dt.date
    df["actual_entry_hour"] = df["actual_entry_ts"].dt.hour
    df["actual_exit_date"] = df["actual_exit_ts"].dt.date
    df["actual_exit_hour"] = df["actual_exit_ts"].dt.hour

    # Extract date and hour for expected return.
    df["expected_return_date"] = df["ExpectedReturnDate"].dt.date
    df["expected_return_hour"] = df["ExpectedReturnDate"].dt.hour

    # Create boolean flags for presence of actual check-in/out.
    df["has_actual_checkin"] = df["actual_entry_ts"].notna()
    df["has_actual_checkout"] = df["actual_exit_ts"].notna()

    return df


def reconcile_bookings_to_operations(bookings_clean: pd.DataFrame, operations_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Joins cleaned bookings to operational entry/exit records to create a master
    booking-operation fact table.

    Join Key: `operations_clean.BookingReference` = `bookings_clean.bookingId`.

    If multiple operational records exist for a single booking, it prioritizes
    records with a `ActualCheckedOutDate` (meaning a completed stay) and then
    the latest `ActualCheckedOutDate`.

    Parameters:
        bookings_clean (pd.DataFrame): Cleaned bookings DataFrame.
        operations_clean (pd.DataFrame): Cleaned operations DataFrame.

    Returns:
        pd.DataFrame: Master DataFrame with one row per booking/operation record where possible.
    """
    ops = operations_clean.copy()

    # Create a flag to prioritize operational records with actual checkouts.
    ops["has_checkout"] = ops["ActualCheckedOutDate"].notna().astype(int)

    # Sort and deduplicate operational records to get the latest/most complete
    # record per BookingReference.
    operations_latest = (
        ops.sort_values(
            ["BookingReference", "has_checkout", "ActualCheckedOutDate"],
            ascending=[True, True, True] # Prioritize has_checkout=1, then latest checkout.
        )
        .drop_duplicates(
            subset=["BookingReference"],
            keep="last" # Keep the latest/most complete record.
        )
    )

    # Merge the cleaned bookings with the latest operational records.
    master = bookings_clean.merge(
        operations_latest,
        how="left", # Keep all bookings, add operational data if available.
        left_on="bookingId",
        right_on="BookingReference",
        suffixes=("_booking", "_ops") # Differentiate columns from original tables.
    )

    return master

def create_daily_fastpark_actuals(master: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates master data to create actual daily entry, exit, and movement volumes.
    (Copied from G1 Simulation)

    Entry definition: `actual_entry_ts` (from config["actual_entry_timestamp_col"])
    Exit definition: `actual_exit_ts` (from config["actual_exit_timestamp_col"])

    Parameters:
        master (pd.DataFrame): Master booking-operation DataFrame.

    Returns:
        pd.DataFrame: Daily actuals table with `date`, `entries`, `exits`,
                      `movements`, and `net_flow`.
    """
    # Count unique booking IDs for entries by their actual entry date.
    daily_entries = (
        master.dropna(subset=["actual_entry_ts"])
        .groupby("actual_entry_date")
        .agg(entries=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"actual_entry_date": "date"})
    )

    # Count unique booking IDs for exits by their actual exit date.
    daily_exits = (
        master.dropna(subset=["actual_exit_ts"])
        .groupby("actual_exit_date")
        .agg(exits=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"actual_exit_date": "date"})
    )

    # Combine entries and exits on date, filling missing counts with 0.
    daily = daily_entries.merge(daily_exits, on="date", how="outer").fillna(0)

    # Ensure 'date' is a datetime object and sort the DataFrame.
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date").reset_index(drop=True)

    # Calculate total movements
    daily["movements"] = daily["entries"] + daily["exits"]


    return daily

def create_hourly_fastpark_actuals(master: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates master data to create actual hourly entry, exit, and movement volumes.

    This is essential for hourly forecasting and FTE planning.

    Parameters:
        master (pd.DataFrame): Master booking-operation DataFrame.

    Returns:
        pd.DataFrame: Hourly actuals table with `datetime_hour`, `date`, `hour`,
                      `entries`, `exits`, `movements`
    """
    # Group entries by the hour of their actual entry timestamp.
    entries_hourly = (
        master.dropna(subset=["actual_entry_ts"])
        .assign(datetime_hour=lambda x: x["actual_entry_ts"].dt.floor("h"))
        .groupby("datetime_hour")
        .agg(entries=("bookingId", "nunique"))
        .reset_index()
    )

    # Group exits by the hour of their actual exit timestamp.
    exits_hourly = (
        master.dropna(subset=["actual_exit_ts"])
        .assign(datetime_hour=lambda x: x["actual_exit_ts"].dt.floor("h"))
        .groupby("datetime_hour")
        .agg(exits=("bookingId", "nunique"))
        .reset_index()
    )

    # Combine hourly entries and exits, filling missing counts with 0.
    hourly = entries_hourly.merge(exits_hourly, on="datetime_hour", how="outer").fillna(0)

    # Calculate total movements and net flow.
    hourly["movements"] = hourly["entries"] + hourly["exits"]

    # Extract date, hour, weekday, and month components from the hourly timestamp.
    hourly["date"] = hourly["datetime_hour"].dt.normalize()
    hourly["hour"] = hourly["datetime_hour"].dt.hour
    hourly["weekday"] = hourly["datetime_hour"].dt.day_name()
    hourly["weekday_num"] = hourly["datetime_hour"].dt.weekday
    hourly["month"] = hourly["datetime_hour"].dt.month

    return hourly


def get_bookings_active_as_of(
    bookings: pd.DataFrame,
    cutoff_timestamp: pd.Timestamp,
) -> pd.DataFrame:
    """
    Filters bookings to return only those active on the date of script running/forecast date if in the past. 

    A booking is considered active if:
    - `createdAt` is less than or equal to the `cutoff_timestamp`
    - AND (`cancelledAt` is null OR `cancelledAt` is greater than the `cutoff_timestamp`)
    - AND `status` is not 'F' (unknown status)

    Parameters:
        bookings (pd.DataFrame): Cleaned bookings DataFrame.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time.

    Returns:
        pd.DataFrame: A DataFrame of bookings active at the cutoff.
    """
    cutoff_timestamp = pd.to_datetime(cutoff_timestamp)

    active_mask = (
        bookings["createdAt"].le(cutoff_timestamp)
        &
        (
            bookings["cancelledAt"].isna()
            |
            bookings["cancelledAt"].gt(cutoff_timestamp)
        )
        &
        bookings["status"].ne("F")
    )
    return bookings.loc[active_mask].copy()

def get_known_target_bookings(
    bookings: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    demand_type: str,
) -> pd.DataFrame:
    """
    Returns bookings visible at the forecast cutoff for a specific target date.
    (Copied from G1 Simulation)

    For 'entry' demand, it filters on `entryDate == target_date`.
    For 'exit' demand, it filters on `exitDate == target_date`.
    This function uses the AS-OF booking population.

    Parameters:
        bookings (pd.DataFrame): Cleaned bookings DataFrame.
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time.
        demand_type (str): "entry" or "exit".

    Returns:
        pd.DataFrame: Bookings known for the target date at the cutoff.
    """
    active = get_bookings_active_as_of(
        bookings=bookings,
        cutoff_timestamp=cutoff_timestamp,
    )

    if demand_type == "entry":
        entry_dates = active["entryDate"].dt.normalize()

        result = active.loc[
            entry_dates.eq(target_date.normalize())
        ].copy()

        return result
    elif demand_type == "exit":
        exit_dates = active["exitDate"].dt.normalize()


        result = active.loc[
            exit_dates.eq(target_date.normalize())
        ].copy()

        return result

    else:
        raise ValueError(f"Unknown demand_type: {demand_type}")

def build_historical_booking_curve(
    bookings: pd.DataFrame,
    master: pd.DataFrame,
    target_field: str,
    actual_field: str,
    cutoff_days: List[int],
) -> pd.DataFrame:
    """
    Builds the historical booking visibility curve.

    Historical as-of logic:
    - `createdAt <= cutoff`
    - AND (`cancelledAt` is null OR `cancelledAt > cutoff`)
    - AND `status != "F"`

    This means a booking whose final status is CX remains included at a
    historical cutoff if its cancellation happened after that cutoff.

    Parameters:
        bookings (pd.DataFrame): Cleaned bookings DataFrame.
        master (pd.DataFrame): Master booking-operation DataFrame.
        target_field (str): The booking date field representing the target (e.g., "entryDate", "exitDate").
        actual_field (str): The operational timestamp field for actuals (e.g., "actual_entry_ts", "actual_exit_ts").
        cutoff_days (List[int]): A list of lead times (in days) to calculate visibility for.

    Returns:
        pd.DataFrame: A DataFrame representing the historical booking curve.
    """
    # Use only the booking columns needed for this calculation.
    booking_df = bookings[
        ["bookingId", "createdAt", "cancelledAt", "status", target_field]
    ].copy()

    # Convert relevant columns to datetime objects.
    for column in ["createdAt", "cancelledAt", target_field]:
        booking_df[column] = pd.to_datetime(booking_df[column], errors="coerce")

    booking_df["booking_target_date"] = booking_df[target_field].dt.normalize()

    # Filter out invalid or incomplete booking records.
    booking_df = booking_df[
        booking_df["bookingId"].notna()
        & booking_df["createdAt"].notna()
        & booking_df["booking_target_date"].notna()
        & booking_df["status"].ne("F")
    ].copy()

    # --- Final Actual Operational Demand ---
    # This represents the ground truth that the booking curve aims to predict.
    # Group by the normalized actual movement date and count unique bookings.
    actual_df = master[["bookingId", actual_field]].copy()
    actual_df[actual_field] = pd.to_datetime(actual_df[actual_field], errors="coerce")
    actual_df["target_date"] = actual_df[actual_field].dt.normalize()

    actual_demand = (
        actual_df.dropna(subset=["bookingId", "target_date"])
        .groupby("target_date", as_index=False)
        .agg(actual_demand=("bookingId", "nunique"))
    )

    if actual_demand.empty:
        return pd.DataFrame(columns=["target_date", "lead_days", "bookings_known", "actual_demand", "completion_ratio"])

    historical_target_dates = set(actual_demand["target_date"])

    # Retain only bookings for target dates that have actual operational validation.
    booking_df = booking_df[
        booking_df["booking_target_date"].isin(
            historical_target_dates
        )
    ].copy()

    rows = []

    # --- Optimize by processing all dates for each lead_days checkpoint ---
    # This loop processes each lead time checkpoint once across all relevant
    # historical target dates, which is more efficient than iterating
    # target_date x lead_time.
    for lead_days in cutoff_days:

        # Calculate the cutoff timestamp for each booking based on its target date
        # and the current lead time. Adding 7 hours as forecast is run at 7am
        cutoff_by_booking = (
            booking_df["booking_target_date"]
            - pd.Timedelta(days=lead_days)
            + pd.Timedelta(hours=7)
        )

        # Create a mask for bookings that were active at this specific cutoff.
        # A booking is active if created by cutoff AND (not cancelled OR cancelled after cutoff).
        active_mask = (
            booking_df["createdAt"].le(cutoff_by_booking)
            &
            (
                booking_df["cancelledAt"].isna()
                |
                booking_df["cancelledAt"].gt(cutoff_by_booking)
            )
        )

        # Count unique bookings known at this cutoff for each target date.
        known_counts = (
            booking_df.loc[
                active_mask,
                [
                    "booking_target_date",
                    "bookingId",
                ],
            ]
            .groupby(
                "booking_target_date",
                as_index=False,
            )
            .agg(
                bookings_known=(
                    "bookingId",
                    "nunique",
                )
            )
            .rename(
                columns={
                    "booking_target_date":
                        "target_date",
                }
            )
        )

        # Merge known counts with actual demand to calculate completion ratios.
        horizon_result = actual_demand.merge(
            known_counts,
            on="target_date",
            how="left",
        )

        # Fill any missing known_bookings with 0 and ensure integer type.
        horizon_result["bookings_known"] = (
            horizon_result["bookings_known"]
            .fillna(0)
            .astype(int)
        )

        # Add the current lead_days to the result.
        horizon_result["lead_days"] = (
            lead_days
        )

        rows.append(
            horizon_result
        )

    # Concatenate all results for different lead times.
    curve = pd.concat(
        rows,
        ignore_index=True,
    )

    # Calculate the completion ratio: actual demand / known bookings.
    # Replace 0 known_bookings with NaN to avoid division by zero.
    curve["completion_ratio"] = (
        curve["actual_demand"]
        /
        curve["bookings_known"].replace(
            0,
            np.nan,
        )
    )

    # Return the relevant columns, sorted for consistency.
    return (
        curve[
            [
                "target_date",
                "lead_days",
                "bookings_known",
                "actual_demand",
                "completion_ratio",
            ]
        ]
        .sort_values(
            [
                "target_date",
                "lead_days",
            ]
        )
        .reset_index(drop=True)
    )


def get_booking_curve_factor(
    curve_lookup: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    cutoff_days: int,
    n: int = 20,
) -> float:
    """
    Estimates the historical booking-curve completion factor for a given target date
    and lead time, using only information available at the forecast cutoff.

    This function is deliberately AS-OF aware to prevent look-ahead bias.
    It considers only historical target dates whose final outcome would have
    already been known at the `cutoff_timestamp`.

    Parameters:
        curve (pd.DataFrame): Historical booking-curve dataset (e.g., from `build_historical_booking_curve`).
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        cutoff_days (int): The lead time in days (e.g., 28 for T-28).
        n (int): Maximum number of recent historical observations to use for the median.

    Returns:
        float: The median historical completion factor, or NaN if insufficient data.
    """
    if curve_lookup is None or not curve_lookup:
        return np.nan

    target_date = pd.to_datetime(target_date).normalize()
    cutoff_date = pd.to_datetime(cutoff_timestamp).normalize()

    history = curve_lookup.get(cutoff_days)

    if history is None or history.empty:
        return np.nan

    history["target_date"] = pd.to_datetime(
        history["target_date"],
        errors="coerce",
    ).dt.normalize()

    # --- CRITICAL AS-OF FILTER ---
    # We may ONLY learn from historical target dates whose final outcome
    # would already have been known at the forecast cut-off.
    # Therefore: `historical target date < forecast cut-off`
    # Also exclude the target date itself from historical learning.
    history = history[
        history["target_date"].notna()
        & history["target_date"].lt(cutoff_date)
        & history["target_date"].lt(target_date)
    ]

    # Filter to the specific lead time being tested.
    history = history[
        history["lead_days"].eq(cutoff_days)
    ]

    # We cannot estimate a meaningful completion factor where there were no visible bookings.
    history = history[
        history["bookings_known"].gt(0)
    ]

    # Ensure completion ratio is not NaN.
    history = history[
        history["completion_ratio"].notna()
    ].copy()

    if history.empty:
        return np.nan

    # Prefer the most recent historical observations that were genuinely available.
    history = (
        history
        .sort_values("target_date")
        .tail(n)
    )

    # Median is used because booking curves can contain extreme ratios
    # when only one or two bookings were visible at long lead times.
    return float(
        history["completion_ratio"].median()
    )


def forecast_booking_curve(
    bookings: pd.DataFrame,
    curve: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    demand_type: str,
    data_cache: Dict[Tuple, Any], # Cache for pre-computed values
) -> float:
    """
    Forecasts target demand using the currently visible booking population
    multiplied by the historical completion factor.

    This is the central "how much do we know already?" model.

    Parameters:
        bookings (pd.DataFrame): Cleaned bookings DataFrame.
        curve (pd.DataFrame): Historical booking curve (entry or exit).
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        demand_type (str): "entry" or "exit".
        data_cache (Dict[Tuple, Any]): A dictionary holding pre-computed/cached values.

    Returns:
        float: The booking curve forecast, or NaN if unable to compute.
    """
    target_date_norm = pd.to_datetime(target_date).normalize()
    cutoff_timestamp_norm = pd.to_datetime(cutoff_timestamp).normalize()

    # Calculate lead_days from normalized dates.
    lead_days = int(
        np.floor(
            (target_date_norm - cutoff_timestamp_norm).total_seconds() / 86400
        )
    )

    # --- Try to use pre-computed caches for known bookings and curve factors ---
    # In a production environment, this cache would typically contain the
    # pre-calculated historical data up to the run date, or be built dynamically.
    # For this script, we assume the `data_cache` is populated.

    # Cache key for known bookings.
    known_booking_cache_key = (
        target_date_norm,
        cutoff_timestamp_norm,
        demand_type,
    )
    known_count = data_cache.get("known_booking_cache", {}).get(known_booking_cache_key)

    if known_count is None: # If not in cache, calculate known bookings directly.
        known_bookings_df = get_known_target_bookings(
            bookings=bookings,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            demand_type=demand_type,
        )
        known_count = known_bookings_df["bookingId"].nunique()

        data_cache.setdefault(
            "known_booking_cache",
            {}
        )[known_booking_cache_key] = known_count

        simple_key = (
            target_date_norm,
            demand_type,
        )

        data_cache.setdefault(
            "known_booking_display_cache",
            {}
        )[simple_key] = known_count

    if known_count == 0:
        return 0.0

    # Cache key for curve factor.
    curve_factor_cache_key = (target_date_norm, lead_days, demand_type)
    factor = data_cache.get("curve_factor_cache", {}).get(curve_factor_cache_key)

    if factor is None: # If not in cache, calculate curve factor directly.
        factor = get_booking_curve_factor(
            curve_lookup=(
                data_cache["entry_curve_lookup"]
                if demand_type == "entry"
                else data_cache["exit_curve_lookup"]
            ),
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            cutoff_days=lead_days,
            n=PRODUCTION_CONFIG["minimum_curve_observations"], # Use config for 'n'
        )
        # Store in cache for future calls within the same run.
        data_cache.setdefault("curve_factor_cache", {})[curve_factor_cache_key] = factor

    if pd.isna(factor):
        return np.nan

    return clip_forecast(known_count * factor)


def build_historical_duration_exit_curve(
    bookings: pd.DataFrame,
    master: pd.DataFrame,
    cutoff_days: List[int],
    config: dict,
) -> pd.DataFrame:
    """
    Builds duration-specific exit booking-curve observations efficiently.

    This function measures how much future exit demand is visible at different
    lead times, separated by planned stay length. This is crucial because
    short-duration bookings can create exit demand with limited forward
    visibility, while long-duration bookings may create exit demand that is
    visible much earlier.

    Parameters:
        bookings (pd.DataFrame): Cleaned bookings DataFrame.
        master (pd.DataFrame): Master booking-operation DataFrame.
        cutoff_days (List[int]): A list of lead times (in days) to calculate visibility for.
        config (dict): Configuration dictionary (e.g., `PRODUCTION_CONFIG`).

    Returns:
        pd.DataFrame: A DataFrame representing the historical duration-segmented exit curve.
    """
    # Define duration bins and labels from config.
    duration_bins = config["duration_bins_days"]
    duration_labels = config["duration_labels"]

    # --- Booking Population ---
    # Select and clean relevant columns for booking processing.
    booking_df = bookings[
        ["bookingId", "createdAt", "cancelledAt", "status", "entryDate", "exitDate"]
    ].copy()

    for column in ["createdAt", "cancelledAt", "entryDate", "exitDate"]:
        booking_df[column] = pd.to_datetime(booking_df[column], errors="coerce")

    booking_df["booking_target_date"] = booking_df["exitDate"].dt.normalize()
    booking_df["planned_duration_days_calc"] = (
        (booking_df["exitDate"] - booking_df["entryDate"]).dt.total_seconds() / 86400
    )

    # Create planned duration bands.
    booking_df["duration_band"] = pd.cut(
        booking_df["planned_duration_days_calc"],
        bins=duration_bins,
        labels=duration_labels,
    )

    # Filter out invalid or incomplete booking records.
    booking_df = booking_df[
        booking_df["bookingId"].notna()
        & booking_df["createdAt"].notna()
        & booking_df["booking_target_date"].notna()
        & booking_df["duration_band"].notna()
        & booking_df["status"].ne("F")
    ].copy()

    # Convert duration_band to string to avoid categorical issues in merges/groupbys.
    booking_df["duration_band"] = booking_df["duration_band"].astype(str)

    # --- Final Actual Exits ---
    # This represents the ground truth for exit demand.
    actual_df = master[
        ["bookingId", "entryDate", "exitDate", "actual_exit_ts"]
    ].copy()

    for column in ["entryDate", "exitDate", "actual_exit_ts"]:
        actual_df[column] = pd.to_datetime(actual_df[column], errors="coerce")

    actual_df["target_date"] = actual_df["actual_exit_ts"].dt.normalize()
    actual_df["planned_duration_days_calc"] = (
        (actual_df["exitDate"] - actual_df["entryDate"]).dt.total_seconds() / 86400
    )

    # Create planned duration bands for actuals.
    actual_df["duration_band"] = pd.cut(
        actual_df["planned_duration_days_calc"],
        bins=duration_bins,
        labels=duration_labels,
    )

    # Filter out invalid or incomplete actual records.
    actual_df = actual_df.dropna(
        subset=["bookingId", "target_date", "duration_band"]
    ).copy()
    actual_df["duration_band"] = actual_df["duration_band"].astype(str)

    # Aggregate actual exits by target date and duration band.
    actuals = (
        actual_df
        .groupby(
            ["target_date", "duration_band"],
            as_index=False,
        )
        .agg(
            actual_exits=(
                "bookingId",
                "nunique",
            )
        )
    )

    if actuals.empty:
        return pd.DataFrame(columns=["target_date", "lead_days", "duration_band", "visible_bookings", "actual_exits", "completion_ratio"])

    historical_target_dates = set(actuals["target_date"])

    # Retain only bookings for target dates that have actual operational validation.
    booking_df = booking_df[
        booking_df["booking_target_date"].isin(
            historical_target_dates
        )
    ].copy()

    rows = []

    # --- Optimize by processing all dates for each lead_days checkpoint ---
    for lead_days in cutoff_days:

        # Calculate the cutoff timestamp for each booking.
        cutoff_by_booking = (
            booking_df["booking_target_date"]
            - pd.Timedelta(days=lead_days)
            + pd.Timedelta(hours=7)
        )

        # Create a mask for bookings active at this specific cutoff.
        active_mask = (
            booking_df["createdAt"].le(cutoff_by_booking)
            &
            (
                booking_df["cancelledAt"].isna()
                |
                booking_df["cancelledAt"].gt(cutoff_by_booking)
            )
        )

        # Count visible bookings by target date and duration band.
        visible_counts = (
            booking_df.loc[
                active_mask,
                [
                    "booking_target_date",
                    "duration_band",
                    "bookingId",
                ],
            ]
            .groupby(
                [
                    "booking_target_date",
                    "duration_band",
                ],
                as_index=False,
            )
            .agg(
                visible_bookings=(
                    "bookingId",
                    "nunique",
                )
            )
            .rename(
                columns={
                    "booking_target_date":
                        "target_date",
                }
            )
        )

        # Merge visible counts with actual exits to calculate completion ratios.
        horizon_result = actuals.merge(
            visible_counts,
            on=[
                "target_date",
                "duration_band",
            ],
            how="left",
        )

        # Fill missing visible_bookings with 0 and ensure integer type.
        horizon_result["visible_bookings"] = (
            horizon_result["visible_bookings"]
            .fillna(0)
            .astype(int)
        )

        horizon_result["lead_days"] = (
            lead_days
        )

        # Calculate completion ratio: actual exits / visible bookings.
        horizon_result["completion_ratio"] = (
            horizon_result["actual_exits"]
            /
            horizon_result[
                "visible_bookings"
            ].replace(
                0,
                np.nan,
            )
        )

        rows.append(
            horizon_result
        )

    # Concatenate all results and return, sorted for consistency.
    return (
        pd.concat(
            rows,
            ignore_index=True,
        )
        [
            [
                "target_date",
                "lead_days",
                "duration_band",
                "visible_bookings",
                "actual_exits",
                "completion_ratio",
            ]
        ]
        .sort_values(
            [
                "target_date",
                "lead_days",
                "duration_band",
            ]
        )
        .reset_index(drop=True)
    )


def get_duration_exit_factors_as_of(
    duration_curve: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    lead_days: int,
    minimum_observations: int = 10,
    maximum_observations: int = 20,
) -> pd.DataFrame:
    """
    Returns duration-specific exit completion factors learned only from
    historical target dates completed before the forecast cutoff.

    Parameters:
        duration_curve (pd.DataFrame): Historical duration-segmented exit curve.
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        lead_days (int): The lead time in days (e.g., 28 for T-28).
        minimum_observations (int): Minimum number of historical observations
                                   required for a duration segment to be considered valid.
        maximum_observations (int): Maximum number of recent historical observations
                                   to consider for the median calculation.

    Returns:
        pd.DataFrame: A DataFrame of duration-specific completion factors.
    """
    if duration_curve is None or duration_curve.empty:
        return pd.DataFrame()

    target_date = pd.to_datetime(target_date).normalize()
    cutoff_date = pd.to_datetime(cutoff_timestamp).normalize()

    history = duration_curve.copy()

    history["target_date"] = pd.to_datetime(
        history["target_date"],
        errors="coerce",
    ).dt.normalize()

    # Filter history to only include data available *before* the cutoff date
    # and *before* the target date.
    history = history[
        history["target_date"].notna()
        & history["target_date"].lt(cutoff_date)
        & history["target_date"].lt(target_date)
    ].copy()

    # Filter to the specific lead time being considered.
    history = history[
        history["lead_days"].eq(lead_days)
    ].copy()

    # Ensure completion ratio is valid and visible bookings are present.
    history = history[
        history["completion_ratio"].notna()
        & history["visible_bookings"].gt(0)
    ].copy()

    rows = []

    # Calculate median completion ratio for each duration band.
    for duration_band, group in history.groupby(
        "duration_band",
        observed=False, # `observed=False` keeps all categories even if not present in group.
    ):
        group = (
            group
            .sort_values("target_date")
            .tail(maximum_observations) # Use only the most recent observations.
        )

        if len(group) < minimum_observations:
            continue # Skip if not enough observations for this segment.

        rows.append(
            {
                "duration_band": duration_band,
                "completion_ratio": float(
                    group["completion_ratio"].median() # Use median to handle outliers.
                ),
                "observations": len(group),
            }
        )

    return pd.DataFrame(rows)

def get_duration_adjusted_booking_curve(
    bookings: pd.DataFrame,
    curve: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    demand_type: str,
    historical_duration_factors: pd.DataFrame,
    config: dict,
) -> float:
    """
    Calculates a booking curve forecast adjusted for the duration mix currently visible.

    This addresses the finding that `20 visible bookings` means different things
    depending on whether they are 1-day stays or 14-day stays.

    Parameters:
        bookings (pd.DataFrame): Cleaned bookings DataFrame.
        curve (pd.DataFrame): The overall historical exit booking curve.
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        demand_type (str): "exit" (this function is specific to exit demand).
        historical_duration_factors (pd.DataFrame): DataFrame containing duration-specific
                                                   completion ratios (e.g., from `get_duration_exit_factors_as_of`).
        config (dict): Configuration dictionary (e.g., `PRODUCTION_CONFIG`).

    Returns:
        float: The duration-adjusted exit booking curve forecast, or NaN.
    """
    # Get bookings active at the cutoff for the target date.
    known = get_known_target_bookings(
        bookings=bookings,
        target_date=pd.to_datetime(target_date).normalize(),
        cutoff_timestamp=cutoff_timestamp,
        demand_type=demand_type,
    )

    if known.empty:
        return 0.0

    # If duration factors are not available or empty, fall back to the standard booking curve forecast.
    if historical_duration_factors is None or historical_duration_factors.empty:
        return forecast_booking_curve(
            bookings=bookings,
            curve=curve,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            demand_type=demand_type,
            data_cache={}, # No cache for this fallback call.
        )

    duration_col = "planned_duration_days_calc"

    # Ensure the duration column is present in the known bookings.
    if duration_col not in known.columns:
        return forecast_booking_curve(
            bookings=bookings,
            curve=curve,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            demand_type=demand_type,
            data_cache={},
        )

    # Apply duration bands to the known bookings.
    known["duration_band"] = pd.cut(
        known[duration_col],
        bins=config["duration_bins_days"],
        labels=config["duration_labels"],
    ).astype(str) # Convert to string to match the factors table.

    forecasts = []

    # Iterate through each duration band in the known bookings.
    for band, group in known.groupby(
        "duration_band",
        observed=False,
    ):
        visible = group["bookingId"].nunique()

        # Find the completion factor for this specific duration band.
        factor_row = historical_duration_factors[
            historical_duration_factors["duration_band"].eq(band)
        ]

        # If no factor is found for this band, fall back to the standard booking curve forecast.
        if factor_row.empty:
            return forecast_booking_curve(
                bookings=bookings,
                curve=curve,
                target_date=target_date,
                cutoff_timestamp=cutoff_timestamp,
                demand_type=demand_type,
                data_cache={},
            )

        factor = float(factor_row["completion_ratio"].iloc[0])

        # If factor is not finite or non-positive, fall back.
        if not np.isfinite(factor) or factor <= 0:
            return forecast_booking_curve(
                bookings=bookings,
                curve=curve,
                target_date=target_date,
                cutoff_timestamp=cutoff_timestamp,
                demand_type=demand_type,
                data_cache={},
            )

        forecasts.append(visible * factor)

    # Sum the duration-adjusted forecasts from all bands.
    return clip_forecast(sum(forecasts))


# --- Same-Weekday Historical Functions (from G1 Simulation) ---

def previous_same_weekdays(
    dataframe: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    n: int,
) -> pd.DataFrame:
    """
    Returns the previous `n` observations from a DataFrame with the same weekday
    as the target, considering only data available before the cutoff.

    Parameters:
        dataframe (pd.DataFrame): The historical data (e.g., `daily_fastpark_actuals`).
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        n (int): The number of previous same-weekday occurrences to retrieve.

    Returns:
        pd.DataFrame: A DataFrame containing the selected historical observations.
    """
    df = dataframe.copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    target_date = pd.to_datetime(target_date).normalize()
    cutoff_date = pd.to_datetime(cutoff_timestamp).normalize()

    # Filter for dates before the target date, before the cutoff date,
    # and with the same weekday as the target date.
    result = df[
        (df["date"] < target_date)
        & (df["date"] < cutoff_date)
        & (df["date"].dt.weekday == target_date.weekday())
    ].sort_values("date")

    # Return the last 'n' occurrences.
    return result.tail(n)


def get_same_weekday_history(
    daily_actuals: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    n: int,
    target_col: str,
) -> pd.Series:
    """
    Retrieves historical actual demand for previous occurrences of the same weekday.
    (Copied from G1 Simulation)

    Important: The target date itself is excluded. Any date on/after the forecast
    cut-off is excluded. This prevents look-ahead bias.

    Parameters:
        daily_actuals (pd.DataFrame): Historical daily actuals DataFrame.
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        n (int): The number of previous same-weekday occurrences to retrieve.
        target_col (str): The column in `daily_actuals` representing demand (e.g., "entries", "exits").

    Returns:
        pd.Series: A Series of historical demand values, or an empty Series if none.
    """
    history = previous_same_weekdays(
        dataframe=daily_actuals,
        target_date=target_date,
        cutoff_timestamp=cutoff_timestamp,
        n=n,
    )

    if history.empty:
        return pd.Series(dtype=float)

    return history[target_col].astype(float)


def forecast_same_weekday(
    daily_actuals: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    target_col: str,
    n: int,
    data_cache: Dict[Tuple, Any],
) -> float:
    """
    Calculates a simple average forecast from the previous `n` same-weekday observations.

    Parameters:
        daily_actuals (pd.DataFrame): Historical daily actuals DataFrame.
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        target_col (str): The column representing demand ("entries" or "exits").
        n (int): The number of previous same-weekday occurrences to average.
        data_cache (Dict[Tuple, Any]): A dictionary holding pre-computed/cached values.

    Returns:
        float: The same-"""

   # Cache key for same-weekday history.
    # The cache stores lists of historical values, so we need to retrieve them
    # and then apply the 'n' filter and mean calculation.
    target_date_norm = pd.to_datetime(target_date).normalize()
    cutoff_timestamp_norm = pd.to_datetime(cutoff_timestamp).normalize()

    # Calculate lead_days to match how the cache key was generated in simulation.
    # This is (target_date - cutoff_date) in days.
    lead_days = int(
        (target_date_norm - cutoff_timestamp_norm).days
    )

    same_weekday_cache_key = (target_date_norm, lead_days)
    cached_history = data_cache.get("same_weekday_history_cache", {}).get(same_weekday_cache_key)
    

    if cached_history is not None:
        # Retrieve values for the specific target column (entries/exits).
        values = cached_history.get(target_col, [])
        # Take the last 'n' values for the forecast.
        values = values[-n:] if len(values) >= n else []

        if len(values) >= n:
            return clip_forecast(safe_mean(values))

    # Fallback: If not in cache or cache is insufficient, calculate directly.
    # This should ideally not be hit in production if the cache is pre-populated.
    values = get_same_weekday_history(
        daily_actuals=daily_actuals,
        target_date=target_date,
        cutoff_timestamp=cutoff_timestamp,
        n=n,
        target_col=target_col,
    )

    data_cache.setdefault(
        "same_weekday_history_cache",
        {}
    ).setdefault(
        same_weekday_cache_key,
        {}
    )[target_col] = values.tolist()


    if len(values) < n:
        return np.nan # Not enough historical observations.

    return clip_forecast(values.mean())


def get_weekday_month_history(
    daily_actuals: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    max_n: int, # Max number of observations to consider (e.g., 6 years).
    target_col: str,
    minimum_observations: int, # Minimum observations required for validity.
) -> Tuple[float, int]: # Returns (forecast_value, observation_count)
    """
    Retrieves historical actual demand for previous occurrences of the same
    weekday AND same month as the target, considering only data available
    before the cutoff.
    (Logic adapted from G1 Simulation)

    Parameters:
        daily_actuals (pd.DataFrame): Historical daily actuals DataFrame.
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        max_n (int): Maximum number of recent historical occurrences to consider.
        target_col (str): The column in `daily_actuals` representing demand.
        minimum_observations (int): Minimum number of observations required to return a valid mean.

    Returns:
        Tuple[float, int]: A tuple containing the mean of historical demand values
                           and the count of observations used. Returns (NaN, 0) if
                           insufficient data.
    """
    df = daily_actuals.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()

    target_date_norm = pd.to_datetime(target_date).normalize()
    cutoff_date_norm = pd.to_datetime(cutoff_timestamp).normalize()

    weekday = target_date_norm.weekday()
    month = target_date_norm.month

    # Filter for historical dates that are:
    # - Before the target date.
    # - Before the cutoff date (to avoid look-ahead bias).
    # - Have the same weekday as the target.
    # - Have the same month as the target.
    history = df[
        (df["date"] < target_date_norm)
        & (df["date"] < cutoff_date_norm)
        & (df["date"].dt.weekday == weekday)
        & (df["date"].dt.month == month)
    ].sort_values("date").tail(max_n) # Take up to max_n most recent occurrences.

    if len(history) < minimum_observations:
        return np.nan, 0

    return safe_mean(history[target_col].tolist()), len(history)


def forecast_weekday_month(
    daily_actuals: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    target_col: str,
    data_cache: Dict[Tuple, Any],
    minimum_observations: int,
) -> Tuple[float, int]:
    """
    Calculates a forecast using the mean of same-weekday-same-month historical observations.
    (Logic adapted from G1 Simulation)

    Parameters:
        daily_actuals (pd.DataFrame): Historical daily actuals DataFrame.
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        target_col (str): The column representing demand ("entries" or "exits").
        data_cache (Dict[Tuple, Any]): A dictionary holding pre-computed/cached values.
        minimum_observations (int): Minimum observations required for a valid forecast.

    Returns:
        Tuple[float, int]: A tuple containing the forecast value and the number of observations used.
    """
    target_date_norm = pd.to_datetime(target_date).normalize()
    cutoff_timestamp_norm = pd.to_datetime(cutoff_timestamp).normalize()

    # Calculate lead_days to match how the cache key was generated in simulation.
    lead_days = int(
        (target_date_norm - cutoff_timestamp_norm).days
    )

    weekday_month_cache_key = (target_date_norm, lead_days)
    cached_data = data_cache.get("weekday_month_history_cache", {}).get(weekday_month_cache_key)

    if cached_data is not None:
        values = cached_data.get(target_col, [])
        count = cached_data.get("count", 0)

        if count >= minimum_observations:
            return clip_forecast(safe_mean(values)), count

    # Fallback: If not in cache or insufficient, calculate directly.
    forecast_value, obs_count = get_weekday_month_history(
        daily_actuals=daily_actuals,
        target_date=target_date,
        cutoff_timestamp=cutoff_timestamp,
        max_n=PRODUCTION_CONFIG.get("weekday_month_max_n", 8), # Default to 8 if not in config.
        target_col=target_col,
        minimum_observations=minimum_observations,
    )

    data_cache.setdefault(
        "weekday_month_history_cache",
        {}
    ).setdefault(
        weekday_month_cache_key,
        {}
    )

    data_cache["weekday_month_history_cache"][
        weekday_month_cache_key
    ][target_col] = [forecast_value]

    data_cache["weekday_month_history_cache"][
        weekday_month_cache_key
    ]["count"] = obs_count

    return clip_forecast(forecast_value), obs_count


def calculate_level_trend_factor(
    daily_actuals: pd.DataFrame,
    cutoff_timestamp: pd.Timestamp,
    target_col: str,
    recent_days: int,
    comparison_days: int,
    minimum_factor: float,
    maximum_factor: float,
) -> float:
    """
    Estimates a purely historical demand level adjustment (trend factor)
    as of the forecast cutoff.
    (Copied from G2 Simulation)

    The trend factor is calculated as: `mean(recent_demand) / mean(comparison_demand)`.
    It is clipped to prevent extreme values from short-lived shocks.

    Parameters:
        daily_actuals (pd.DataFrame): Historical daily actuals DataFrame.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        target_col (str): The column representing demand ("entries" or "exits").
        recent_days (int): Number of recent days to average for the 'recent level'.
        comparison_days (int): Number of preceding days to average for the 'comparison level'.
        minimum_factor (float): Lower bound for clipping the trend factor.
        maximum_factor (float): Upper bound for clipping the trend factor.

    Returns:
        float: The clipped trend factor (e.g., 1.0 if no trend or insufficient data).
    """
    cutoff_date = pd.to_datetime(cutoff_timestamp).normalize()

    history = daily_actuals.copy()
    history["date"] = pd.to_datetime(history["date"], errors="coerce").dt.normalize()

    # Filter for historical data available before the cutoff date.
    history = history[
        history["date"].lt(cutoff_date)
    ].sort_values("date")

    # Define the date ranges for recent and comparison periods.
    recent_start = cutoff_date - pd.Timedelta(days=recent_days)
    comparison_start = recent_start - pd.Timedelta(days=comparison_days)

    # Extract demand for the recent and comparison periods.
    recent_demand = history[
        history["date"].ge(recent_start)
    ][target_col]

    comparison_demand = history[
        history["date"].ge(comparison_start)
        & history["date"].lt(recent_start)
    ][target_col]

    # Return 1.0 (no adjustment) if insufficient data for either period.
    if len(recent_demand) < max(7, recent_days // 2): # Heuristic for sufficient data.
        return 1.0
    if len(comparison_demand) < max(14, comparison_days // 2):
        return 1.0

    recent_mean = safe_mean(recent_demand)
    comparison_mean = safe_mean(comparison_demand)

    # Return 1.0 if means are invalid or comparison mean is zero.
    if pd.isna(recent_mean) or pd.isna(comparison_mean) or comparison_mean <= 0:
        return 1.0

    factor = recent_mean / comparison_mean

    # Clip the factor to prevent extreme adjustments.
    return float(
        np.clip(
            factor,
            minimum_factor,
            maximum_factor,
        )
    )

def forecast_trend_adjusted_seasonal(
    seasonal_forecast: float | None,
    trend_factor: float | None,
) -> float:
    """
    Applies a historical demand-level trend factor to a seasonal forecast.

    Parameters:
        seasonal_forecast (float | None): The base seasonal forecast.
        trend_factor (float | None): The trend adjustment factor.

    Returns:
        float: The trend-adjusted forecast, or NaN if inputs are invalid.
    """
    if pd.isna(seasonal_forecast) or pd.isna(trend_factor):
        return np.nan

    return clip_forecast(
        seasonal_forecast * trend_factor
    )


def shrink_booking_forecast(
    booking_forecast: float | None,
    historical_forecast: float | None,
    known_bookings: int | float | None,
    shrinkage_strength: int | float,
) -> float:
    """
    Shrinks an unstable booking-curve forecast toward a historical baseline.
    (Copied from G2 Simulation)

    This mechanism improves stability at longer horizons where booking visibility is low.
    The reliability weight is calculated as `known_bookings / (known_bookings + shrinkage_strength)`.
    Low-visibility forecasts receive more historical support, while high-visibility
    forecasts remain closer to the booking curve.

    Parameters:
        booking_forecast (float | None): The raw booking curve forecast.
        historical_forecast (float | None): The historical baseline forecast.
        known_bookings (int | float | None): The number of known bookings for the target.
        shrinkage_strength (int | float): A parameter controlling the strength of shrinkage.
                                          Higher values lead to more shrinkage.

    Returns:
        float: The shrunk forecast, or NaN if inputs are invalid.
    """
    if pd.isna(booking_forecast):
        return historical_forecast
    if pd.isna(historical_forecast):
        return booking_forecast

    known_bookings_val = max(0.0, float(known_bookings)) if known_bookings is not None else 0.0
    shrinkage_strength_val = max(0.0, float(shrinkage_strength))

    if shrinkage_strength_val == 0:
        return clip_forecast(booking_forecast) # No shrinkage if strength is 0.

    reliability = (
        known_bookings_val
        / (known_bookings_val + shrinkage_strength_val)
    )

    return clip_forecast(
        reliability * booking_forecast
        + (1.0 - reliability) * historical_forecast
    )

# =============================================================================
# 3. PRODUCTION DATA PREPARATION
# =============================================================================

def load_and_clean_production_data(config: dict, sql_engine) -> Dict[str, pd.DataFrame]:
    """
    Loads and cleans all necessary historical data up to the current run date.
    This data forms the basis for calculating forecast components.

    Parameters:
        config (dict): The global configuration dictionary (`PRODUCTION_CONFIG`).
        sql_engine: The SQLAlchemy engine for database connection.

    Returns:
        Dict[str, pd.DataFrame]: A dictionary containing cleaned bookings, operations,
                                 flights, master, daily actuals, hourly actuals,
                                 daily passenger context, and historical curves.
    """
    current_run_date = pd.to_datetime(config["current_run_date"]).normalize()
    history_start_date = config["history_start_date"]

    history_end_date = current_run_date.strftime("%Y-%m-%d")

    booking_extract_end_date = (
        current_run_date
        + pd.Timedelta(days=config["forecast_lookahead_days"])
    ).strftime("%Y-%m-%d")

    bookings_raw = get_fastpark_bookings(
        start=history_start_date,
        end=booking_extract_end_date,
        statuses=[
            config["valid_booking_status"],
            config["cancelled_status"],
            "F",
        ],
        asset_name=config["asset_name"],
        engine=sql_engine,
    )

    operations_raw = get_fastpark_entry_exits(
        start=history_start_date,
        end=history_end_date,
        engine=sql_engine,
    )

    # Clean raw data.
    bookings_clean = clean_bookings(bookings_raw, config)

    operations_clean = clean_operations(operations_raw, config)

    # Reconcile bookings and operations.
    master_df = reconcile_bookings_to_operations(bookings_clean, operations_clean)

    # Create daily and hourly FastPark actuals.
    daily_actuals_df = create_daily_fastpark_actuals(master_df)
    hourly_actuals_df = create_hourly_fastpark_actuals(master_df)


        # Build historical booking curves.
    # The `forecast_horizons_list` needs to include all possible lead_days from 0 to 56.
    # This list will be used to calculate booking curve completion ratios for all relevant horizons.
    forecast_horizons_list = list(range(config["forecast_lookahead_days"] + 1))

    entry_booking_curve = build_historical_booking_curve(
        bookings=bookings_clean,
        master=master_df,
        target_field="entryDate",
        actual_field="actual_entry_ts",
        cutoff_days=forecast_horizons_list,
    )
    exit_booking_curve = build_historical_booking_curve(
        bookings=bookings_clean,
        master=master_df,
        target_field="exitDate",
        actual_field="actual_exit_ts",
        cutoff_days=forecast_horizons_list,
    )
    duration_exit_booking_curve = build_historical_duration_exit_curve(
        bookings=bookings_clean,
        master=master_df,
        cutoff_days=forecast_horizons_list,
        config=config,
    )


    return {
        "bookings_clean": bookings_clean,
        "operations_clean": operations_clean, # Not directly used in forecast, but good for context.
        "master_df": master_df,
        "daily_actuals_df": daily_actuals_df,
        "hourly_actuals_df": hourly_actuals_df,
        "entry_booking_curve": entry_booking_curve,
        "exit_booking_curve": exit_booking_curve,
        "duration_exit_booking_curve": duration_exit_booking_curve,

    }


class ProductionForecastData:
    """
    A container class to hold all pre-loaded and pre-processed data
    needed for the production forecast. This avoids passing many
    individual DataFrames to functions.
    """
    def __init__(self, data: Dict[str, pd.DataFrame], config: Dict[str, Any]):
        self.config = config

        self.bookings_clean = data["bookings_clean"]
        self.master_df = data["master_df"]
        self.daily_actuals_df = data["daily_actuals_df"]
        self.hourly_actuals_df = data["hourly_actuals_df"]

        self.entry_booking_curve = data["entry_booking_curve"]
        self.exit_booking_curve = data["exit_booking_curve"]

        self.duration_exit_booking_curve = data["duration_exit_booking_curve"]

        self.entry_curve_lookup = {
            lead: grp
            for lead, grp in self.entry_booking_curve.groupby("lead_days")
        }

        self.exit_curve_lookup = {
            lead: grp
            for lead, grp in self.exit_booking_curve.groupby("lead_days")
        }

        # Load learned weights and parameters
        self.forecast_weights = pd.read_csv(config["forecast_weights_path"])

        # Pre-process parameters for faster lookup
        self._prepare_parameters()


        # Cache for frequently accessed historical values to avoid redundant calculations
        self.cache: Dict[Tuple, Any] = {
            "known_booking_cache": {},
            "curve_factor_cache": {},
            "same_weekday_history_cache": {},
            "weekday_month_history_cache": {},
            "trend_cache": {},
            "duration_factor_cache": {},
        }

                
                
        self.cache["entry_curve_lookup"] = (
            self.entry_curve_lookup
        )

        self.cache["exit_curve_lookup"] = (
            self.exit_curve_lookup
        )

    def _prepare_parameters(self):

        required_horizons = {
            (flow, horizon)
            for flow in ("entry", "exit")
            for horizon in range(
                self.config["forecast_lookahead_days"] + 1
            )
        }

        weight_df = self.forecast_weights.copy()

        weight_df["demand_type"] = (
            weight_df["demand_type"]
            .astype(str)
            .str.lower()
        )

        weight_df["horizon_days"] = pd.to_numeric(
            weight_df["horizon_days"],
            errors="raise",
        ).astype(int)

        self.weights_lookup = (
            weight_df
            .set_index(
                ["demand_type", "horizon_days"]
            )
            .to_dict(orient="index")
        )

        required_weight_columns = {
            "shrinkage_strength",
            *get_base_component_names("entry"),
            *get_base_component_names("exit"),
        }

        missing_weight_columns = (
            required_weight_columns
            - set(weight_df.columns)
        )

        if missing_weight_columns:

            raise ValueError(
                "forecast_weights.csv missing columns: "
                f"{sorted(missing_weight_columns)}"
            )

        missing_horizons = sorted(
            required_horizons
            - set(self.weights_lookup)
        )

        if missing_horizons:

            raise ValueError(
                "forecast_weights.csv missing rows: "
                f"{missing_horizons}"
            )




def calculate_component_forecasts(
    target_date: pd.Timestamp,
    horizon_days: int,
    data: ProductionForecastData,
) -> Dict[str, Any]:
    """
    Calculate one forecast snapshot for a single target date and lead time.

    The model builds each forecast from the state of the world that would have been
    known at the cutoff for that date. In practical terms, this means:
    - count bookings already visible at the cutoff;
    - apply the learned booking-curve completion factor;
    - add seasonality, trend, and behavioural adjustments;
    - combine signals with the weights selected for this horizon.

    This function is the main engine behind a single target-date forecast.
    """
    cutoff_timestamp = (
        pd.to_datetime(target_date).normalize()
        - pd.Timedelta(days=horizon_days)
        + pd.Timedelta(hours=7)
    )

    components = {
        "target_date": target_date,
        "horizon_days": horizon_days,
        "cutoff_timestamp": cutoff_timestamp,
    }
    

    # --- 1. Raw Components ---
    components["entry_booking"] = forecast_booking_curve(data.bookings_clean, data.entry_booking_curve, target_date, cutoff_timestamp, "entry", data.cache)
    components["exit_booking"] = forecast_booking_curve(data.bookings_clean, data.exit_booking_curve, target_date, cutoff_timestamp, "exit", data.cache)



    duration_factor_key = (
        pd.to_datetime(target_date).normalize(),
        horizon_days,
    )

    duration_factors = data.cache[
        "duration_factor_cache"
    ].get(duration_factor_key)

    if duration_factors is None:

        duration_factors = get_duration_exit_factors_as_of(
            duration_curve=data.duration_exit_booking_curve,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            lead_days=horizon_days,
            minimum_observations=data.config["minimum_curve_observations"],
        )

        data.cache[
            "duration_factor_cache"
        ][duration_factor_key] = duration_factors

    components["exit_duration"] = get_duration_adjusted_booking_curve(
        bookings=data.bookings_clean,
        curve=data.exit_booking_curve,
        target_date=target_date,
        cutoff_timestamp=cutoff_timestamp,
        demand_type="exit",
        historical_duration_factors=duration_factors,
        config=data.config,
    )
    
    # --- 2. Historical & Trend ---

    entry_w = 6
    exit_w = 6

    components["entry_weekday"] = forecast_same_weekday(
        data.daily_actuals_df,
        target_date,
        cutoff_timestamp,
        "entries",
        entry_w,
        data.cache,
    )

    components["exit_weekday"] = forecast_same_weekday(
        data.daily_actuals_df,
        target_date,
        cutoff_timestamp,
        "exits",
        exit_w,
        data.cache,
    )

    components["entry_month"], _ = forecast_weekday_month(
        data.daily_actuals_df,
        target_date,
        cutoff_timestamp,
        "entries",
        data.cache,
        3,
    )

    components["exit_month"], _ = forecast_weekday_month(
        data.daily_actuals_df,
        target_date,
        cutoff_timestamp,
        "exits",
        data.cache,
        3,
    )

    # ==========================================================
    # TREND FACTORS
    # ==========================================================

    trend_key = (
        cutoff_timestamp.normalize(),
        "entries",
    )

    if trend_key not in data.cache["trend_cache"]:
        data.cache["trend_cache"][trend_key] = (
            calculate_level_trend_factor(
                data.daily_actuals_df,
                cutoff_timestamp,
                "entries",
                28,
                84,
                0.7,
                1.3,
            )
        )

    entry_trend_factor = (
        data.cache["trend_cache"][trend_key]
    )

    trend_key = (
        cutoff_timestamp.normalize(),
        "exits",
    )

    if trend_key not in data.cache["trend_cache"]:
        data.cache["trend_cache"][trend_key] = (
            calculate_level_trend_factor(
                data.daily_actuals_df,
                cutoff_timestamp,
                "exits",
                28,
                84,
                0.7,
                1.3,
            )
        )

    exit_trend_factor = (
        data.cache["trend_cache"][trend_key]
    )

    components["entry_trend_month"] = (
        forecast_trend_adjusted_seasonal(
            components["entry_month"],
            entry_trend_factor,
        )
    )

    components["exit_trend_month"] = (
        forecast_trend_adjusted_seasonal(
            components["exit_month"],
            exit_trend_factor,
        )
    )

    components["entry_known_bookings"] = (
        data.cache.get(
            "known_booking_display_cache",
            {},
        ).get(
            (
                pd.to_datetime(target_date).normalize(),
                "entry",
            ),
            0,
        )
    )

    components["exit_known_bookings"] = (
        data.cache.get(
            "known_booking_display_cache",
            {},
        ).get(
            (
                pd.to_datetime(target_date).normalize(),
                "exit",
            ),
            0,
        )
    )

    # --- 3. Finalize Ensemble ---
    for flow in ["entry", "exit"]:
        weight_row = data.weights_lookup[
            (flow, horizon_days)
        ]

        strength = float(
            weight_row["shrinkage_strength"]
        )
        # Gen 2.1 shrinks the raw booking component toward the trend baseline.
        components[f"{flow}_booking_shrunk"] = shrink_booking_forecast(
            components[f"{flow}_booking"],
            components[f"{flow}_trend_month"],
            components[f"{flow}_known_bookings"],
            strength
        )
    return components


# =============================================================================
# 5. DAILY FORECAST GENERATION
# =============================================================================
# The daily model weights the four Generation 2 base components first, then applies
# the winning Generation 2.1 horizon adjustment.

def generate_daily_forecasts(data: ProductionForecastData) -> pd.DataFrame:
    """
    Generate the daily entry and exit forecast across the full forward window.

    The output is one row per target date, where each row contains:
    - the date being forecast;
    - the lead time (horizon) from the run date;
    - the final entry forecast;
    - the final exit forecast;
    - the component values used to build the forecast.

    Parameters:
        data (ProductionForecastData): Pre-loaded historical data, learned parameters,
                                       and lookup tables for the run.

    Returns:
        pd.DataFrame: Daily forecast table with one row for each future day.
    """
    current_run_date = pd.to_datetime(data.config["current_run_date"]).normalize()
    forecast_lookahead_days = data.config["forecast_lookahead_days"]

    forecast_results = []

    # Include today as horizon 0 and continue through the full lookahead window.
    # This matches the historical backtest convention used in the simulation work.
    target_dates = [
        current_run_date + pd.Timedelta(days=d)
        for d in range(0, forecast_lookahead_days + 1)
    ]

    print(f"Generating daily forecasts for {len(target_dates)} dates...")

    # Each target date has one valid horizon value based on its distance from the run date.
    # Example: today = 0, tomorrow = 1, +14 days = 14, ...
    for target_date in target_dates:
        horizon_days = int((target_date - current_run_date).days)

        components = calculate_component_forecasts(
            target_date=target_date,
            horizon_days=horizon_days,
            data=data,
        )

        entry_weights_row = data.weights_lookup[("entry", horizon_days)]
        exit_weights_row = data.weights_lookup[("exit", horizon_days)]


        entry_component_names = get_base_component_names("entry")
        exit_component_names = get_base_component_names("exit")

        base_entry_forecast = weighted_average(
            values=[components.get(name) for name in entry_component_names],
            weights=[float(entry_weights_row.get(name, 0.0)) for name in entry_component_names],
        )
        base_exit_forecast = weighted_average(
            values=[components.get(name) for name in exit_component_names],
            weights=[float(exit_weights_row.get(name, 0.0)) for name in exit_component_names],
        )

        final_entry_forecast = base_entry_forecast
        final_exit_forecast = base_exit_forecast



        forecast_results.append(
            {
                "target_date": target_date.normalize(),
                "horizon_days": horizon_days,

                "entry_forecast": final_entry_forecast,
                "exit_forecast": final_exit_forecast,

                "entry_base_forecast": base_entry_forecast,
                "exit_base_forecast": base_exit_forecast,

                **{
                    k: v
                    for k, v in components.items()
                    if not isinstance(v, pd.Timestamp)
                },
            }
        )

    daily_forecast_df = pd.DataFrame(forecast_results)

    # Clean up component columns that might be NaN if not always used
    for col in daily_forecast_df.columns:
        if isinstance(daily_forecast_df[col].dtype, pd.Float64Dtype):
            daily_forecast_df[col] = daily_forecast_df[col].replace([np.inf, -np.inf], np.nan)


    keep_cols = [
        "target_date",
        "horizon_days",

        "entry_forecast",
        "exit_forecast",

        "entry_base_forecast",
        "exit_base_forecast",

        "entry_booking",
        "exit_booking",

        "entry_booking_shrunk",
        "exit_booking_shrunk",

        "entry_weekday",
        "exit_weekday",

        "entry_month",
        "exit_month",

        "entry_trend_month",
        "exit_trend_month",

        "exit_duration",

        "entry_known_bookings",
        "exit_known_bookings",
    ]
    available_cols = [col for col in keep_cols if col in daily_forecast_df.columns]
    daily_forecast_df = daily_forecast_df[available_cols].copy()
    daily_forecast_df = daily_forecast_df.rename(
        columns={
            "entry_known_bookings": "entry_bookings",
            "exit_known_bookings": "exit_bookings",
        }
    )

    return daily_forecast_df


# =============================================================================
# 6. HOURLY FORECAST GENERATION
# =============================================================================

def build_hourly_profile(
    hourly_actuals: pd.DataFrame,
    target_date: pd.Timestamp,
    cutoff_timestamp: pd.Timestamp,
    demand_type: str,
    n_weeks: int,
) -> Dict[int, float] | None:
    """
    Build a same-weekday hourly profile for either entry or exit demand.

    The purpose is to answer: "Given the total daily demand for this date, how is that
    demand usually distributed across the 24 hours on the same weekday?"

    We use only historical dates that are fully in the past relative to the chosen
    forecast cutoff, so the profile remains consistent with the as-of logic used in the
    rest of the model.

    Parameters:
        hourly_actuals (pd.DataFrame): Historical hourly actuals DataFrame.
        target_date (pd.Timestamp): The date for which the forecast is being made.
        cutoff_timestamp (pd.Timestamp): The "as-of" point in time for the forecast.
        demand_type (str): "entry" or "exit".
        n_weeks (int): The number of previous same-weekday weeks to use for the profile.

    Returns:
        Dict[int, float] | None: Hour (0-23) mapped to that hour's proportion of the day.
    """
    target_date = pd.to_datetime(target_date).normalize()
    cutoff_date = pd.to_datetime(cutoff_timestamp).normalize()

    weekday = target_date.weekday()

    df = hourly_actuals

    # Filter for historical data that is:
    # - Before the target date.
    # - Before the cutoff date (to avoid look-ahead bias).
    # - Has the same weekday as the target.
    historical = df[
        (df["date"] < target_date)
        & (df["date"] < cutoff_date)
        & (df["weekday_num"].eq(weekday))
    ].copy()

    if historical.empty:
        return None

    # Select the most recent `n_weeks` of historical dates.
    selected_dates = (
        historical["date"]
        .drop_duplicates()
        .sort_values()
        .tail(n_weeks)
    )

    if len(selected_dates) < n_weeks: # Ensure enough historical weeks are available.
        return None

    historical = historical[
        historical["date"].isin(
            selected_dates
        )
    ]

    demand_col = "entries" if demand_type == "entry" else "exits"

    # Aggregate hourly totals and reindex to ensure all 24 hours are present.
    hourly_totals = (
        historical
        .groupby("hour")[demand_col]
        .sum()
        .reindex(
            range(24),
            fill_value=0,
        )
        .astype(float)
    )

    total_demand = hourly_totals.sum()

    if total_demand <= 0: # Avoid division by zero.
        return None

    # Calculate the proportion of demand for each hour.
    profile = (
        hourly_totals
        /
        total_demand
    )

    # Basic check to ensure proportions sum to 1 (allowing for floating point inaccuracies).
    if not np.isclose(
        profile.sum(),
        1.0,
        atol=1e-10,
    ):
        raise RuntimeError(
            "Hourly profile does not sum to 1. Check `build_hourly_profile` logic."
        )

    return profile.to_dict()


def distribute_daily_to_hourly(
    daily_forecast: float | None,
    hourly_profile: Dict[int, float] | None,
) -> Dict[int, float] | None:
    """
    Convert a daily forecast into an hourly forecast using the selected hourly profile.

    Example:
    - daily forecast = 100 entries
    - hour 9 profile share = 0.12
    - hourly forecast for hour 9 = 12 entries

    The profile values always sum to 1 across the 24 hours, so this is a simple
    proportional allocation of the daily total.

    Parameters:
        daily_forecast (float | None): The total daily demand forecast.
        hourly_profile (Dict[int, float] | None): Share of the day for each hour.

    Returns:
        Dict[int, float] | None: Hourly forecast by hour of day.
    """
    if hourly_profile is None:
        return None
    if pd.isna(daily_forecast):
        return None

    hourly_forecast = {}
    for hour in range(24):
        proportion = hourly_profile.get(hour, 0.0)
        hourly_forecast[hour] = clip_forecast(daily_forecast * proportion)

    return hourly_forecast


def generate_hourly_forecasts(
    daily_forecasts_df: pd.DataFrame,
    data: ProductionForecastData,
) -> pd.DataFrame:
    """
    Turn daily forecasts into hourly forecasts.

    The daily forecast gives a total for the whole day. The hourly forecast then takes
    that total and spreads it across each hour using a historical weekday profile. This
    allows the model to represent peak and off-peak demand patterns without needing to
    forecast every hour separately.

    We also add the actual booking counts that are scheduled for each target date/hour,
    based on the booking's planned entry or exit date/time, as of the current run cutoff.
    This keeps the forecast model unchanged while providing a useful operational snapshot.

    Parameters:
        daily_forecasts_df (pd.DataFrame): DataFrame containing daily forecasts.
        data (ProductionForecastData): Production data object with historical hourly profiles.

    Returns:
        pd.DataFrame: A long-form table of one row per target date and hour.
    """
    hourly_forecast_results = []

    print("Generating hourly forecasts...")

    # Each date in the daily forecast table maps to a single total forecast for that day.
    # We allocate that total across 24 hours using the best historical profile for the
    # matching weekday and horizon.
    final_daily_forecasts = daily_forecasts_df.copy()
    run_cutoff = pd.to_datetime(data.config["current_run_date"]).normalize() + pd.Timedelta(hours=7)

    active_bookings = data.bookings_clean[
        data.bookings_clean["createdAt"].le(run_cutoff)
        & (
            data.bookings_clean["cancelledAt"].isna()
            |
            data.bookings_clean["cancelledAt"].gt(run_cutoff)
        )
        & data.bookings_clean["status"].ne("F")
    ].copy()

    active_bookings["entry_date_norm"] = (
        pd.to_datetime(active_bookings["entryDate"])
        .dt.normalize()
    )

    active_bookings["exit_date_norm"] = (
        pd.to_datetime(active_bookings["exitDate"])
        .dt.normalize()
    )

    entry_lookup = (
        active_bookings
        .groupby(
            ["entry_date_norm", "planned_entry_hour"]
        )["bookingId"]
        .nunique()
        .to_dict()
    )

    exit_lookup = (
        active_bookings
        .groupby(
            ["exit_date_norm", "planned_exit_hour"]
        )["bookingId"]
        .nunique()
        .to_dict()
    )


    for row in final_daily_forecasts.itertuples(index=False):
        target_date = row.target_date
        target_date_norm = pd.to_datetime(target_date).normalize()

        entry_n_weeks = 6
        exit_n_weeks = 6

        # Build hourly profiles for entry and exit.
        entry_profile = build_hourly_profile(
            hourly_actuals=data.hourly_actuals_df,
            target_date=target_date,
            cutoff_timestamp=run_cutoff,
            demand_type="entry",
            n_weeks=entry_n_weeks,
        )
        exit_profile = build_hourly_profile(
            hourly_actuals=data.hourly_actuals_df,
            target_date=target_date,
            cutoff_timestamp=run_cutoff,
            demand_type="exit",
            n_weeks=exit_n_weeks,
        )

        # Distribute daily forecasts into hourly forecasts.
        hourly_entries = distribute_daily_to_hourly(
            daily_forecast=row.entry_forecast,
            hourly_profile=entry_profile,
        )
        hourly_exits = distribute_daily_to_hourly(
            daily_forecast=row.exit_forecast,
            hourly_profile=exit_profile,
        )


        # Append hourly results for each hour of the day.
        for hour in range(24):
            entry_actual = entry_lookup.get(
                (target_date_norm, hour),
                0
            )

            exit_actual = exit_lookup.get(
                (target_date_norm, hour),
                0
            )

            hourly_forecast_results.append({
                "target_date": target_date,
                "datetime_hour": pd.to_datetime(target_date) + pd.Timedelta(hours=hour),
                "hour": hour,
                "entry_forecast_hourly": hourly_entries.get(hour, np.nan) if hourly_entries else np.nan,
                "exit_forecast_hourly": hourly_exits.get(hour, np.nan) if hourly_exits else np.nan,
                "entry_actual_bookings_hourly": float(entry_actual) if pd.notna(entry_actual) else 0.0,
                "exit_actual_bookings_hourly": float(exit_actual) if pd.notna(exit_actual) else 0.0,
            })

    hourly_forecast_df = pd.DataFrame(hourly_forecast_results)
    return hourly_forecast_df


# =============================================================================
# 7. MAIN EXECUTION PIPELINE
# =============================================================================

def run_production_forecast(config: dict, save_outputs=False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Executes the full FastPark production forecasting pipeline.

    Parameters:
        config (dict): The global configuration dictionary (`PRODUCTION_CONFIG`).

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the daily and hourly
                                           forecasts DataFrames.
    """
    t0 = time.perf_counter()
    print("=" * 80)
    print("FASTPARK PRODUCTION FORECAST GENERATION")
    print("=" * 80)

    # --- 1. Initialize Run Date ---
    if config["current_run_date"] is None:
        # Use today's date if not specified in config.
        current_run_date = pd.Timestamp.today().normalize()
    else:
        current_run_date = pd.to_datetime(config["current_run_date"]).normalize()
    config["current_run_date"] = current_run_date # Update config with resolved date.

    print(f"Forecast run date: {current_run_date.date()}")
    print(f"Forecasting {config['forecast_lookahead_days']} days ahead.")
    print(f"Historical data starts: {config['history_start_date']}")
    print(f"Historical data ends (implicitly): {current_run_date.date()}")

    # --- 2. Establish Database Connection ---
    sql_engine = get_engine(dsn=config["db_dsn"], username=config["db_username"])
    t1 = step(t0, "Established database connection")

    # --- 3. Load and Clean Historical Data ---
    raw_data_dict = load_and_clean_production_data(config, sql_engine)
    t2 = step(t1, "Loaded and cleaned historical data")

    # --- 4. Initialize ProductionForecastData Object ---
    # This object holds all data and pre-processed parameters for efficient access.
    data_for_forecast = ProductionForecastData(raw_data_dict, config)
    t3 = step(t2, "Prepared production forecast data structures")

    # --- 5. Generate Daily Forecasts ---
    daily_forecasts = generate_daily_forecasts(data_for_forecast)
    t4 = step(t3, f"Generated daily forecasts ({len(daily_forecasts)} records)")

    # --- 6. Generate Hourly Forecasts ---
    hourly_forecasts = generate_hourly_forecasts(daily_forecasts, data_for_forecast)
    t5 = step(t4, f"Generated hourly forecasts ({len(hourly_forecasts)} records)")

    # --- 7. Export Results ---
    if save_outputs:
        daily_forecasts.to_csv(config["daily_forecast_output_file"], index=False)
        hourly_forecasts.to_csv(config["hourly_forecast_output_file"], index=False)
        t6 = step(t5, f"Exported forecasts to {config['daily_forecast_output_file']} and {config['hourly_forecast_output_file']}")

    total_runtime = time.perf_counter() - t0
    print(f"\nFastPark Production Forecast completed in {total_runtime:.2f} seconds.")
    print("=" * 80)

    return daily_forecasts, hourly_forecasts


# =============================================================================
# 8. SCRIPT ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    # --- IMPORTANT: Ensure forecast_weights.csv ise available ---
    # This file are crucial for the script to run. They must be generated by
    # the Generation 2 and Generation 2.1 simulation scripts and placed in the
    # `PRODUCTION_OUTPUT_DIR` specified in `PRODUCTION_CONFIG`.
    #
    # Example structure for `forecast_weights.csv`:
    # demand_type,horizon_days,shrinkage_strength,entry_booking_shrunk_weight,entry_weekday_weight,...
    # entry,0,0,1.0,0.0,...
    # exit,0,0,0.8,0.0,...
    #

    # Execute the production forecast pipeline.
    daily_forecast_output, hourly_forecast_output = run_production_forecast(PRODUCTION_CONFIG, save_outputs=True)

    print("\nDaily Forecast Output (first 5 rows):")
    print(daily_forecast_output.head())
    print("\nHourly Forecast Output (first 5 rows):")
    print(hourly_forecast_output.head())
