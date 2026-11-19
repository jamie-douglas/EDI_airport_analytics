import sys
import pathlib
from pathlib import Path
import time

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from modules.utils.query import query
from modules.utils.db import get_engine
from modules.utils.progress import step

import pandas as pd
import numpy as np
import re

# ============================================================
# FASTPARK FORECASTING HISTORICAL ANALYSIS FRAMEWORK
# ============================================================

"""
Purpose:
  Build a historical analysis pipeline for FastPark
  to understand:
    1. Actual entries and exits
    2. Booking behaviour and booking curves
    3. Cancellation and no-show behaviour
    4. Stay duration and exit forecasting logic
    5. Tendency / rolling penetration windows
    6. Passenger, airline, country and seasonality drivers

Core tables:
  AirportX.v_Bookings
  FastPark.v_EntryAndExits
  EAL.FlightPerformance

Key join:
  FastPark.v_EntryAndExits.BookingReference
      =
  AirportX.v_Bookings.bookingId

Important interpretation:
  status = 'B'  -> booked / valid booking, assumed active unless proven otherwise
  status = 'CX' -> cancelled
  status = 'F'  -> unknown meaning, analyse separately until confirmed

"""
# ============================================================

# ============================================================
# 0. PARAMETERS / CONFIGURATION
# ============================================================

def get_analysis_config():
    """
    Create a central configuration dictionary used throughout the analysis.

    Parameters:
        None

    Returns:
        dict:
            Configuration values including:
                - asset_name
                - valid_booking_status
                - cancelled_status
                - unknown_statuses
                - analysis date range
                - lead time checkpoints
                - tendency windows
                - duration bands
                - early / late return thresholds
                - passenger column choice
    """

    config = {
        # Product / asset filtering
        "asset_name": "FastPark",

        # Booking status logic
        "valid_booking_status": "B",
        "cancelled_status": "CX",
        "unknown_statuses": ["F"],

        # Date range for historical analysis
        # Fill these in depending on the history you want to use
        "analysis_start_date": "2024-01-01",
        "analysis_end_date": "2026-04-08", 

        # Lead time checkpoints for booking curve analysis
        # These represent days before entry date
        "lead_time_checkpoints": [56, 42, 28, 21, 14, 10, 7, 5, 3, 2, 1, 0],

        # General rolling windows across all weekdays.
        # Keep available, but optionally skip for entry tendency if same-weekday methods perform better.
        "tendency_windows_weeks": [2, 4, 6, 8, 13],
        "run_general_rolling_entry_tendency": False,
        "run_general_rolling_exit_tendency": False,

        # Same weekday ratio-of-sums methods.
        # Example: to forecast a Friday, use previous n Fridays:
        # sum(entries) / sum(departing_pax)
        "same_weekday_occurrences": [2, 4, 6, 8, 10, 12],

        # Same weekday rolling average of daily penetration.
        # Example: mean(entry_penetration of previous n Fridays)
        "same_weekday_average_occurrences": [2, 4, 6, 8, 10, 12],

        # Weighted same weekday rolling average.
        # More recent same weekdays receive more weight.
        "same_weekday_weighted_occurrences": [2, 4, 6, 8, 10, 12],


        # Duration bands in days
        "duration_bins_days": [0, 1, 3, 7, 10, 14, 21, 999],
        "duration_labels": [
            "0-1 days",
            "2-3 days",
            "4-7 days",
            "8-10 days",
            "11-14 days",
            "15-21 days",
            "22+ days",
        ],

        # Return deviation thresholds
        # These define operationally meaningful early / late returns
        "early_late_threshold_hours": 2,
        "major_deviation_threshold_hours": 6,

        # Passenger field choice for historical passenger analysis
        "historical_pax_col": "Passengers",

        # Flight filtering
        "passenger_flight_flag_col": "IsPassengerFlight",
        "passenger_flight_flag_value": 1,

        # Entry timestamp choice
        "actual_entry_timestamp_col": "CheckInEnded",

        # Exit timestamp choice
        "actual_exit_timestamp_col": "ActualCheckedOutDate",

        # Estimated capacity / occupancy parameters
        # These anchor the reconstructed occupancy series to a known
        # current cars-on-site position.
        "current_cars_on_site": 4031,
        "total_fastpark_capacity": 6008,

        # Date that the current cars-on-site number relates to.
        # If this date is not present in the actuals data, the latest
        # available date on or before this date will be used.
        "occupancy_anchor_date": "2026-08-04",
    }

    return config


# ============================================================
# 1. DATA LOADING FUNCTIONS
# ============================================================

def get_fastpark_bookings(
    start: str | None = None,
    end: str | None = None,
    statuses: list[str] | None = None,
    asset_name: str = "FastPark",
    engine=None,
):
    """
    Query FastPark booking records from AirportX.v_Bookings.

    Purpose
    -------
    This table represents booking intent:
        - when the booking was created
        - when the customer intends to enter
        - when the customer intends to exit
        - booking status
        - cancellation timestamp
        - price/product/channel information
        - declared inbound/outbound flight information

    Date filtering
    --------------
    Uses interval overlap on entryDate and exitDate:

        entryDate < end
        AND exitDate >= start

    This is important because a vehicle may enter before the analysis
    window but exit inside the analysis window.

    Status logic
    ------------
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
        SQLAlchemy engine.

    Returns
    -------
    pandas.DataFrame
        FastPark booking records.
    """

    if statuses is None:
        statuses = ["B", "CX", "F"]

    columns = [
        "bookingUuid",
        "bookingId",
        "transactionId",
        "createdAt",
        "updatedAt",
        "cancelledAt",
        "channel",
        "productCode",
        "productGroup",
        "productName",
        "productPrice",
        "productQuantity",
        "productTotal",
        "entryDate",
        "exitDate",
        "status",
        "assetCode",
        "assetName",
        "bookingTotal",
        "leadtime",
        "duration",
        "nationality",
        "inboundAirline",
        "inboundFlight",
        "inboundRoute",
        "outboundAirline",
        "outboundFlight",
        "outboundRoute",
        "promoCode",
    ]

    where = [
        "assetName = :asset_name",
    ]

    params = {
        "asset_name": asset_name,
    }

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
    Query operational FastPark entry/exit records.

    Purpose
    -------
    This table represents operational reality:
        - when the customer started kiosk check-in
        - when the customer finished kiosk check-in
        - when the key is available
        - expected/advised return date
        - actual checkout date
        - return flight information

    Operational interpretation
    --------------------------
    CheckInStarted:
        Customer starts the kiosk process.

    CheckInEnded:
        Customer finishes the kiosk process.
        This is when the key is available and the car can be moved.

    For actual operational entry demand, we should usually use:
        CheckInEnded

    For actual exit demand, use:
        ActualCheckedOutDate

    Date filtering
    --------------
    A booking can enter before the analysis window and exit inside it.
    Therefore this function uses or_events=True so that records are included
    if either:
        CheckInEnded is inside the window
        OR ActualCheckedOutDate is inside the window

    Parameters
    ----------
    start : str, optional
        Inclusive start date.

    end : str, optional
        Exclusive end date.

    engine : optional
        SQLAlchemy engine.

    Returns
    -------
    pandas.DataFrame
        FastPark operational entry/exit records.
    """

    columns = [
        "BookingReference",
        "CheckInStarted",
        "CheckInEnded",
        "VehicleStatus",
        "CheckInDurationSecs",
        "ExpectedArrivalDate",
        "ExpectedReturnDate",
        "ExpectedReturnDateID",
        "ExpectedReturnTimeID",
        "ExpectedReturnZuluDateID",
        "ExpectedReturnZuluTimeID",
        "ReturnFlight",
        "ReturnFlightOperator",
        "ReturnFlightNumber",
        "ChromaFlightID",
        "ChromaDateTime_Local",
        "ActualCheckedOutDate",
        "CheckedInBy",
        "CheckedOutBy",
        "KeyLockerBox",
        "SourceFile",
        "RecordInsertedBy",
        "RecordInsertedDateTime",
        "RecordUpdatedBy",
        "RecordUpdatedDateTime",
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
        or_events=True,
        order_by="COALESCE(CheckInEnded, ActualCheckedOutDate), BookingReference",
        engine=engine,
    )

    return df


def get_historical_flight_performance(
    start: str | None = None,
    end: str | None = None,
    engine=None,
):
    """
    Query historical passenger flight data from EAL.FlightPerformance.

    Purpose
    -------
    This table provides the airport demand context:
        - arriving passengers
        - departing passengers
        - airline mix
        - country mix
        - domestic/international split
        - flight counts
        - schedule season
        - actual and forecast passenger fields

    Filtering
    ---------
    Use:
        IsPassengerFlight = 1

    Parameters
    ----------
    start : str, optional
        Inclusive start date based on ScheduledDateTime_Local.

    end : str, optional
        Exclusive end date based on ScheduledDateTime_Local.

    engine : optional
        SQLAlchemy engine.

    Returns
    -------
    pandas.DataFrame
        Historical passenger flight records.
    """

    columns = [
        "FlightID",
        "ScheduledDateTime_Local",
        "ScheduledDateTime_Zulu",
        "ScheduledDateTime_DateID_Local",
        "ScheduledDateTime_TimeID_Local",
        "ActualDateTime_Local",
        "ArrDeptureCode",
        "FlightNumber",
        "TicketedOperator",
        "AirlineCode_ICAO",
        "AirlineCode_IATA",
        "Airline_Description",
        "FlightTypeCode_ACCORD",
        "FlightTypeDescription",
        "AircraftTypeCode_ICAO",
        "AircraftTypeCode_IATA",
        "AircraftTypeDescription",
        "AirportCode_IATA",
        "AirportCode_ICAO",
        "AirportDescription",
        "AirportCountryCode",
        "CountryName",
        "AirportEU_NonEU",
        "ACLSchedule_AircraftMaxPax",
        "Domestic_International",
        "Sector",
        "ScheduleSeason",
        "FlightStatus",
        "FlightStatusDescription",
        "FlightCancelled",
        "Passengers",
        "Children",
        "Infants",
        "LatestForecast_Pax",
        "PublishedForecast_Pax",
        "LatestForecast_LoadFactor",
        "PublishedForecast_LoadFactor",
        "IsPassengerFlight",
        "Pax_MostConfident",
        "MaxPax_MostConfident",
        "SeatLoadFactor",
        "PaxForecastError",
        "AbsolutePaxForecastError",
        "TransitPax",
        "Checkin_Total_Pax",
        "PenetrationRate_CheckIn",
        "FirstCheckInAction_Local",
        "LastCheckInAction_Local",
        "UniquePax_Boarding",
        "UniqueBookings_Boarding",
        "FirstBoardingScan_Local",
        "LastBoardingScan_Local",
        "BoardingScanTime_Mins",
        "BoardingPassScansSecurity",
        "BoardingPassScansCUPPS",
        "AOS_Source",
        "UniqueID",
        "SlotDateTime_Local",
    ]

    where = [
        "IsPassengerFlight = :is_passenger_flight",
    ]

    params = {
        "is_passenger_flight": 1,
    }

    df = query(
        table="EAL.FlightPerformance",
        columns=columns,
        where=where,
        params=params,
        date_column="ScheduledDateTime_Local",
        start=start,
        end=end,
        order_by="ScheduledDateTime_Local, FlightID",
        engine=engine,
    )

    return df

# ============================================================
# 2. BASIC CLEANING AND DERIVED FIELDS
# ============================================================

def clean_bookings(bookings, config):
    """
    Clean and enrich the bookings dataframe.

    Adds date/time features and status flags used across the analysis.

    Parameters:
        bookings: pandas.DataFrame
            Raw dataframe from AirportX.v_Bookings.

        config: dict
            Configuration dictionary.

    Returns:
        pandas.DataFrame:
            Cleaned bookings dataframe with derived fields.
    """

    df = bookings.copy()

    # Convert datetime columns
    df["createdAt"] = pd.to_datetime(df["createdAt"])
    df["updatedAt"] = pd.to_datetime(df["updatedAt"])
    df["cancelledAt"] = pd.to_datetime(df["cancelledAt"])
    df["entryDate"] = pd.to_datetime(df["entryDate"])
    df["exitDate"] = pd.to_datetime(df["exitDate"])

    # Status flags
    df["is_valid_booking"] = df["status"].eq(config["valid_booking_status"])
    df["is_cancelled"] = df["status"].eq(config["cancelled_status"])
    df["is_unknown_status"] = df["status"].isin(config["unknown_statuses"])

    # Date and hour features
    df["booking_created_date"] = df["createdAt"].dt.date
    df["booking_created_hour"] = df["createdAt"].dt.hour
    df["planned_entry_date"] = df["entryDate"].dt.date
    df["planned_entry_hour"] = df["entryDate"].dt.hour
    df["planned_exit_date"] = df["exitDate"].dt.date
    df["planned_exit_hour"] = df["exitDate"].dt.hour

    # Recalculate lead time and duration to validate stored values
    df["lead_time_days_calc"] = (df["entryDate"] - df["createdAt"]).dt.total_seconds() / 86400
    df["planned_duration_days_calc"] = (df["exitDate"] - df["entryDate"]).dt.total_seconds() / 86400

    #  Calendar features
    df["entry_weekday"] = df["entryDate"].dt.day_name()
    df["entry_weekday_num"] = df["entryDate"].dt.weekday
    df["entry_month"] = df["entryDate"].dt.month
    df["entry_year"] = df["entryDate"].dt.year
    df["entry_week"] = df["entryDate"].dt.isocalendar().week
    df["exit_weekday"] = df["exitDate"].dt.day_name()
    df["exit_month"] = df["exitDate"].dt.month

    # Duration bands
    df["planned_duration_band"] = pd.cut(
        df["planned_duration_days_calc"],
        bins=config["duration_bins_days"],
        labels=config["duration_labels"],
        right=True,
        include_lowest=True
    )

    # --------------------------------------------------------
    # Price metrics
    # --------------------------------------------------------
    # bookingTotal is the total value of the booking.
    # Because longer stays naturally cost more, price-per-day metrics are
    # needed to understand whether pricing affects demand behaviour rather
    # than just reflecting duration.

    df["booking_total_per_quantity"] = (
        df["bookingTotal"]
        / df["productQuantity"].replace(0, np.nan)
    )

    df["booking_total_per_planned_day"] = (
        df["bookingTotal"]
        / df["planned_duration_days_calc"].replace(0, np.nan)
    )

    df["booking_total_per_quantity_per_day"] = (
        df["booking_total_per_quantity"]
        / df["planned_duration_days_calc"].replace(0, np.nan)
    )

    df["product_price_per_planned_day"] = (
        df["productPrice"]
        / df["planned_duration_days_calc"].replace(0, np.nan)
    )

    return df

def create_daily_price_summary(bookings_clean):
    """
    Create daily pricing metrics by planned entry date.

    Purpose:
        Allow price to be analysed as a daily demand driver alongside
        passenger volumes, booking volumes, entries, exits and penetration.

    Notes:
        This uses valid bookings only because the purpose is to understand
        the price environment associated with realised booking demand.
    """

    valid = bookings_clean[
        bookings_clean["is_valid_booking"]
    ].copy()

    daily_price_summary = (
        valid
        .groupby("planned_entry_date", dropna=False)
        .agg(
            valid_bookings=("bookingId", "nunique"),

            avg_booking_total=("bookingTotal", "mean"),
            median_booking_total=("bookingTotal", "median"),

            avg_product_price=("productPrice", "mean"),
            median_product_price=("productPrice", "median"),

            avg_price_per_day=("booking_total_per_planned_day", "mean"),
            median_price_per_day=("booking_total_per_planned_day", "median"),

            avg_price_per_quantity=("booking_total_per_quantity", "mean"),
            median_price_per_quantity=("booking_total_per_quantity", "median"),

            avg_price_per_quantity_per_day=(
                "booking_total_per_quantity_per_day",
                "mean",
            ),
            median_price_per_quantity_per_day=(
                "booking_total_per_quantity_per_day",
                "median",
            ),

            avg_product_price_per_day=("product_price_per_planned_day", "mean"),
            median_product_price_per_day=("product_price_per_planned_day", "median"),

            avg_lead_time_days=("lead_time_days_calc", "mean"),
            median_lead_time_days=("lead_time_days_calc", "median"),

            avg_duration_days=("planned_duration_days_calc", "mean"),
            median_duration_days=("planned_duration_days_calc", "median"),
        )
        .reset_index()
        .rename(columns={"planned_entry_date": "date"})
    )

    daily_price_summary["date"] = pd.to_datetime(
        daily_price_summary["date"],
        errors="coerce",
    )

    return daily_price_summary


def clean_operations(operations, config):
    """
    Clean and enrich the operational entry/exit dataframe.

    Adds operational entry and exit timestamps:
        - actual entry timestamp = CheckInEnded
        - actual exit timestamp = ActualCheckedOutDate

    Parameters:
        operations: pandas.DataFrame
            Raw dataframe from FastPark.v_EntryAndExits.

        config: dict
            Configuration dictionary.

    Returns:
        pandas.DataFrame:
            Cleaned operations dataframe with derived fields.
    """

    df = operations.copy()

    # Convert datetime columns
    df["CheckInStarted"] = pd.to_datetime(df["CheckInStarted"])
    df["CheckInEnded"] = pd.to_datetime(df["CheckInEnded"])
    df["ExpectedArrivalDate"] = pd.to_datetime(df["ExpectedArrivalDate"])
    df["ExpectedReturnDate"] = pd.to_datetime(df["ExpectedReturnDate"])
    df["ActualCheckedOutDate"] = pd.to_datetime(df["ActualCheckedOutDate"])
    df["ChromaDateTime_Local"] = pd.to_datetime(df["ChromaDateTime_Local"])

    # Define actual entry and exit timestamps
    df["actual_entry_ts"] = df[config["actual_entry_timestamp_col"]]
    df["actual_exit_ts"] = df[config["actual_exit_timestamp_col"]]

    # Operational date/hour fields
    df["actual_entry_date"] = df["actual_entry_ts"].dt.date
    df["actual_entry_hour"] = df["actual_entry_ts"].dt.hour
    df["actual_exit_date"] = df["actual_exit_ts"].dt.date
    df["actual_exit_hour"] = df["actual_exit_ts"].dt.hour

    # Expected return date/hour
    df["expected_return_date"] = df["ExpectedReturnDate"].dt.date
    df["expected_return_hour"] = df["ExpectedReturnDate"].dt.hour

    # Check-in process duration
    # If CheckInDurationSecs is populated, use it.
    # Otherwise calculate it.
    df["checkin_duration_secs_calc"] = (
        df["CheckInEnded"] - df["CheckInStarted"]
    ).dt.total_seconds()

    # Flags
    df["has_actual_checkin"] = df["actual_entry_ts"].notna()
    df["has_actual_checkout"] = df["actual_exit_ts"].notna()

    return df

def clean_flights(flights, config):
    """
    Clean and enrich the flight performance dataframe.

    Creates date/hour fields and prepares passenger metrics.

    Parameters:
        flights: pandas.DataFrame
            Raw dataframe from EAL.FlightPerformance.

        config: dict
            Configuration dictionary.

    Returns:
        pandas.DataFrame:
            Cleaned flights dataframe.
    """

    df = flights.copy()

    # Convert datetime fields
    df["ScheduledDateTime_Local"] = pd.to_datetime(df["ScheduledDateTime_Local"])
    df["ActualDateTime_Local"] = pd.to_datetime(df["ActualDateTime_Local"])

    # Scheduled date/hour
    df["flight_date"] = df["ScheduledDateTime_Local"].dt.date
    df["flight_hour"] = df["ScheduledDateTime_Local"].dt.hour
    df["flight_weekday"] = df["ScheduledDateTime_Local"].dt.day_name()
    df["flight_month"] = df["ScheduledDateTime_Local"].dt.month
    df["flight_year"] = df["ScheduledDateTime_Local"].dt.year

    # Passenger column to use for historical analysis
    df["analysis_pax"] = df[config["historical_pax_col"]]

    # Direction flags
    df["is_departure"] = df["ArrDeptureCode"].eq("D")
    df["is_arrival"] = df["ArrDeptureCode"].eq("A")

    # Domestic / international flags
    df["is_domestic"] = df["Domestic_International"].eq("Domestic")
    df["is_international"] = df["Domestic_International"].eq("International")

    return df

# ============================================================
# 3. DATA RECONCILIATION
# ============================================================

def reconcile_bookings_to_operations(bookings_clean, operations_clean):
    """
    Join bookings to operational entry/exit records.

    Join key:
        FastPark.v_EntryAndExits.BookingReference
            =
        AirportX.v_Bookings.bookingId

    This creates the master booking-operation fact table.

    Parameters:
        bookings_clean: pandas.DataFrame
            Cleaned bookings dataframe.

        operations_clean: pandas.DataFrame
            Cleaned operations dataframe.

    Returns:
        pandas.DataFrame:
            Master dataframe with one row per booking/operation record where possible.

    """

    ops = operations_clean.copy()

    ops["has_checkout"] = (
        ops["ActualCheckedOutDate"]
        .notna()
        .astype(int)
    )

    operations_latest = (
        ops
        .sort_values(
            [
                "BookingReference",
                "has_checkout",
                "ActualCheckedOutDate"
            ]
        )
        .drop_duplicates(
            subset=["BookingReference"],
            keep="last"
        )
    )
    
    master = bookings_clean.merge(
        operations_latest,
        how="left",
        left_on="bookingId",
        right_on="BookingReference",
        suffixes=("_booking", "_ops")
    )

    return master

config = get_analysis_config()
dsn = 'AzureConnection'
user = 'jamie_douglas'


sql_connection = get_engine(dsn=dsn, username=user)

bookings_raw = get_fastpark_bookings(
        start=config["analysis_start_date"],
        end=config["analysis_end_date"],
        statuses=["B", "CX", "F"],
        asset_name=config["asset_name"],
        engine=sql_connection,
    )



operations_raw = get_fastpark_entry_exits(
        start=config["analysis_start_date"],
        end=config["analysis_end_date"],
        engine=sql_connection,
    )


flights_raw = get_historical_flight_performance(
        start=config["analysis_start_date"],
        end=config["analysis_end_date"],
        engine=sql_connection,
    )


    # ----------------------------
    # Clean raw data
    # ----------------------------


bookings_clean = clean_bookings(bookings_raw, config)
operations_clean = clean_operations(operations_raw, config)
flights_clean = clean_flights(flights_raw, config)
daily_price_summary = create_daily_price_summary(bookings_clean)


master = reconcile_bookings_to_operations(bookings_clean, operations_clean)


# ==========================
# 0-1 DAY INVESTIGATION
# ==========================

df = master.copy()

# Match flags
df["has_operation_match"] = df["BookingReference"].notna()

# Work out actual check-in field if present
actual_checkin_col = None

for col in [
    "ActualCheckedInDate",
    "actual_entry_ts",
    "actualEntryDate",
]:
    if col in df.columns:
        actual_checkin_col = col
        break

if actual_checkin_col:
    df["has_actual_checkin"] = df[actual_checkin_col].notna()
else:
    df["has_actual_checkin"] = False

# Lead time
df["entryDate"] = pd.to_datetime(df["entryDate"])
df["createdAt"] = pd.to_datetime(df["createdAt"])

df["lead_time_days"] = (
    df["entryDate"] - df["createdAt"]
).dt.total_seconds() / 86400

# Filter
short = df[
    (df["is_valid_booking"])
    & (df["planned_duration_band"] == "0-1 days")
].copy()

print("="*80)
print("0-1 DAY SUMMARY")
print("="*80)

print(f"Bookings: {len(short):,}")
print(f"Matched: {short['has_operation_match'].sum():,}")
print(f"Unmatched: {(~short['has_operation_match']).sum():,}")

print()

# -------------------------------------
# Matched vs Unmatched
# -------------------------------------

summary = (
    short
    .groupby("has_operation_match")
    .agg(
        bookings=("bookingId", "nunique"),
        avg_lead_time=("lead_time_days", "mean"),
        median_lead_time=("lead_time_days", "median"),
        avg_booking_total=("bookingTotal", "mean"),
        median_booking_total=("bookingTotal", "median"),
    )
)

print("="*80)
print("MATCHED VS UNMATCHED")
print("="*80)
print(summary)

# -------------------------------------
# Lead Time Bands
# -------------------------------------

short["lead_time_band"] = pd.cut(
    short["lead_time_days"],
    bins=[-999, 0, 1, 3, 7, 14, 28, 56, 999],
    labels=[
        "<=0",
        "1 day",
        "2-3 days",
        "4-7 days",
        "8-14 days",
        "15-28 days",
        "29-56 days",
        "57+ days",
    ]
)

leadtime = (
    short
    .groupby("lead_time_band", observed=False)
    .agg(
        bookings=("bookingId", "nunique"),
        matches=("has_operation_match", "sum")
    )
    .reset_index()
)

leadtime["match_rate"] = (
    leadtime["matches"]
    / leadtime["bookings"]
)

print("\n")
print("="*80)
print("0-1 DAY MATCH RATE BY LEAD TIME")
print("="*80)
print(leadtime)

# -------------------------------------
# Monthly trend
# -------------------------------------

short["entry_month"] = (
    pd.to_datetime(short["entryDate"])
    .dt.to_period("M")
)

monthly = (
    short
    .groupby("entry_month")
    .agg(
        bookings=("bookingId", "nunique"),
        matches=("has_operation_match", "sum")
    )
    .reset_index()
)

monthly["match_rate"] = (
    monthly["matches"]
    / monthly["bookings"]
)

print("\n")
print("="*80)
print("0-1 DAY MATCH RATE BY MONTH")
print("="*80)
print(monthly.tail(24))

# -------------------------------------
# Product check
# -------------------------------------

for product_col in [
    "productName",
    "productCode",
    "productGroup",
]:
    if product_col in short.columns:

        product = (
            short
            .groupby(product_col)
            .agg(
                bookings=("bookingId", "nunique"),
                matches=("has_operation_match", "sum")
            )
            .reset_index()
        )

        product["match_rate"] = (
            product["matches"]
            / product["bookings"]
        )

        product = product.sort_values(
            "bookings",
            ascending=False
        )

        print("\n")
        print("="*80)
        print(f"0-1 DAY BY {product_col}")
        print("="*80)
        print(product.head(20))

        break