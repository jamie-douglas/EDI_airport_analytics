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
        "analysis_end_date": "2026-07-30", 

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

def create_reconciliation_summary(bookings_clean, operations_clean, master):
    """
    Create a reconciliation summary showing how well the booking and
    operational tables match.

    Parameters:
        bookings_clean: pandas.DataFrame
            Cleaned bookings dataframe.

        operations_clean: pandas.DataFrame
            Cleaned operations dataframe.

        master: pandas.DataFrame
            Joined booking-operation dataframe.

    Returns:
        pandas.DataFrame:
            Summary table with counts and match rates.

    Output should include:
        - total booking records
        - total operational records
        - valid bookings
        - cancelled bookings
        - unknown F status bookings
        - duplicate bookingId count
        - duplicate BookingReference count
        - bookings matched to operations
        - bookings missing operations
        - operations missing bookings
        - bookings with check-in
        - bookings with checkout
        - bookings with check-in but no checkout
        - bookings with checkout but no check-in

    """

    total_bookings = len(bookings_clean)
    total_operations = len(operations_clean)

    valid_bookings = bookings_clean["is_valid_booking"].sum()
    cancelled_bookings = bookings_clean["is_cancelled"].sum()
    unknown_status_bookings = bookings_clean["is_unknown_status"].sum()

    duplicate_booking_ids = bookings_clean["bookingId"].duplicated().sum()

    unmatched_valid_bookings = (
        master.loc[
            master["BookingReference"].isna()
            & master["is_valid_booking"],
        ].shape[0]
    )

    unmatched_cancelled_bookings = (
        master.loc[
            master["BookingReference"].isna()
            & master["is_cancelled"],
        ].shape[0]
    )

    unmatched_unknown_status_bookings = (
        master.loc[
            master["BookingReference"].isna()
            & master["is_unknown_status"],
        ].shape[0]
    )

    matched_bookings = master["BookingReference"].notna().sum()
    unmatched_bookings = master["BookingReference"].isna().sum()

    operations_without_booking_count = (
        ~operations_clean["BookingReference"].isin(bookings_clean["bookingId"])
    ).sum()

    bookings_with_checkin = master["actual_entry_ts"].notna().sum()
    bookings_with_checkout = master["actual_exit_ts"].notna().sum()

    bookings_with_checkin_no_checkout = (
        master["actual_entry_ts"].notna()
        & master["actual_exit_ts"].isna()
    ).sum()

    bookings_with_checkout_no_checkin = (
        master["actual_entry_ts"].isna()
        & master["actual_exit_ts"].notna()
    ).sum()

    summary = pd.DataFrame(
        [
            {
                "metric": "total_bookings",
                "value": total_bookings,
                "description": "Total FastPark booking rows loaded from AirportX.v_Bookings.",
            },
            {
                "metric": "total_operations",
                "value": total_operations,
                "description": "Total operational rows loaded from FastPark.v_EntryAndExits.",
            },
            {
                "metric": "valid_bookings_B",
                "value": valid_bookings,
                "description": "Bookings with status B.",
            },
            {
                "metric": "cancelled_bookings_CX",
                "value": cancelled_bookings,
                "description": "Bookings with status CX.",
            },
            {
                "metric": "unknown_status_bookings_F",
                "value": unknown_status_bookings,
                "description": "Bookings with status F.",
            },
            {
                "metric": "duplicate_booking_ids",
                "value": duplicate_booking_ids,
                "description": "Duplicate bookingId rows in bookings table.",
            },
            {
                "metric": "matched_bookings",
                "value": matched_bookings,
                "description": "Booking rows matched to operational records.",
            },
            {
                "metric": "unmatched_bookings",
                "value": unmatched_bookings,
                "description": "Booking rows with no matching operational record.",
            },
            {
                "metric": "unmatched_valid_bookings_B",
                "value": unmatched_valid_bookings,
                "description": "Valid bookings with no matching operational record.",
            },
            {
                "metric": "unmatched_cancelled_bookings_CX",
                "value": unmatched_cancelled_bookings,
                "description": "Cancelled bookings with no matching operational record.",
            },
            {
                "metric": "unmatched_unknown_status_bookings_F",
                "value": unmatched_unknown_status_bookings,
                "description": "Unknown status bookings with no matching operational record.",
            },
            {
                "metric": "operations_without_booking",
                "value": operations_without_booking_count,
                "description": "Operational rows where BookingReference was not found in bookingId.",
            },
            {
                "metric": "bookings_with_checkin",
                "value": bookings_with_checkin,
                "description": "Rows with actual_entry_ts populated.",
            },
            {
                "metric": "bookings_with_checkout",
                "value": bookings_with_checkout,
                "description": "Rows with actual_exit_ts populated.",
            },
            {
                "metric": "bookings_with_checkin_no_checkout",
                "value": bookings_with_checkin_no_checkout,
                "description": "Rows with check-in but no checkout.",
            },
            {
                "metric": "bookings_with_checkout_no_checkin",
                "value": bookings_with_checkout_no_checkin,
                "description": "Rows with checkout but no check-in.",
            },
        ]
    )

    summary["pct_of_bookings"] = np.where(
        total_bookings > 0,
        summary["value"] / total_bookings,
        np.nan,
    )

    return summary

# ============================================================
# 4. STATUS, CANCELLATION AND NO-SHOW ANALYSIS
# ============================================================

def analyse_booking_statuses(bookings_clean):
    """
    Analyse booking status distribution.

    This is needed because the known statuses are:
        B  = valid booking, assumed booked
        CX = cancelled
        F  = unknown and should be analysed separately

    Parameters:
        bookings_clean: pandas.DataFrame
            Cleaned bookings dataframe.

    Returns:
        pandas.DataFrame:
            Status summary by count and percentage.
    """
    
    status_summary = (
        bookings_clean
        .groupby("status")
        .agg(
            bookings=("bookingId", "count"),
            unique_bookings=("bookingId", "nunique"),
            avg_lead_time=("lead_time_days_calc", "mean"),
            avg_duration=("planned_duration_days_calc", "mean"),
            avg_booking_total=("bookingTotal", "mean")
        )
        .reset_index()
    )
    
    status_summary["booking_share"] = (
        status_summary["bookings"] / status_summary["bookings"].sum()
    )

    return status_summary


def analyse_cancellations(bookings_clean):
    """
    Analyse cancellation behaviour.

    Purpose:
        Understand how many bookings cancel, when they cancel,
        and whether cancellation behaviour differs by segment.

    Parameters:
        bookings_clean: pandas.DataFrame
            Cleaned bookings dataframe.

    Returns:
        dict:
            Dictionary of cancellation analysis tables, for example:
                - cancellation_summary
                - cancellation_by_lead_time
                - cancellation_by_weekday
                - cancellation_by_month
                - cancellation_by_channel
                - cancellation_by_duration_band
                - cancellation_by_airline

    """

    df = bookings_clean.copy()

    df["cancel_days_before_entry"] = (
        df["entryDate"] - df["cancelledAt"]
    ).dt.total_seconds() / 86400

    df["created_to_cancel_days"] = (
        df["cancelledAt"] - df["createdAt"]
    ).dt.total_seconds() / 86400

    df["lead_time_band"] = pd.cut(
        df["lead_time_days_calc"],
        bins=[-np.inf, 0, 1, 3, 7, 14, 28, 56, np.inf],
        labels=[
            "same day or negative",
            "1 day",
            "2-3 days",
            "4-7 days",
            "8-14 days",
            "15-28 days",
            "29-56 days",
            "57+ days",
        ],
    )

    def cancellation_rate_by(group_cols):
        out = (
            df
            .groupby(group_cols, dropna=False, observed = False)
            .agg(
                bookings=("bookingId", "count"),
                cancelled=("is_cancelled", "sum"),
                avg_cancel_days_before_entry=("cancel_days_before_entry", "mean"),
            )
            .reset_index()
        )

        out["cancellation_rate"] = (
            out["cancelled"] / out["bookings"].replace(0, np.nan)
        )

        return out

    cancellation_summary = (
        df
        .groupby("status", dropna=False)
        .agg(
            bookings=("bookingId", "count"),
            unique_bookings=("bookingId", "nunique"),
            cancelled=("is_cancelled", "sum"),
            avg_cancel_days_before_entry=("cancel_days_before_entry", "mean"),
            avg_created_to_cancel_days=("created_to_cancel_days", "mean"),
        )
        .reset_index()
    )

    cancellation_summary["cancelled_share"] = (
        cancellation_summary["cancelled"]
        / cancellation_summary["bookings"].replace(0, np.nan)
    )

    results = {
        "cancellation_summary": cancellation_summary,
        "cancellation_by_lead_time": cancellation_rate_by(["lead_time_band"]),
        "cancellation_by_duration_band": cancellation_rate_by(["planned_duration_band"]),
        "cancellation_by_duration_and_lead_time": cancellation_rate_by(["planned_duration_band", "lead_time_band"]),
        "cancellation_by_airline": cancellation_rate_by(["outboundAirline"]),

        # prove to be not useful in initial testing, uncomment if a forecasting use is identified
        #"cancellation_by_weekday": cancellation_rate_by(["entry_weekday"]),
        #"cancellation_by_month": cancellation_rate_by(["entry_month"]),
    }

    return results


def analyse_no_shows(master, analysis_cutoff_date=None):
    """
    Analyse no-show behaviour.

    Definition:
        A no-show is a valid booking that does not have an actual check-in.

    Proposed logic:
        status = B
        AND actual_entry_ts is null

    Important:
        Only count no-shows for historical entry dates that are already in the past.
        Otherwise future bookings will incorrectly appear as no-shows.

    Parameters:
        master: pandas.DataFrame
            Master booking-operation dataframe.

    Returns:
        dict:
            Dictionary of no-show analysis outputs:
                - no_show_summary
                - no_show_by_lead_time
                - no_show_by_weekday
                - no_show_by_month
                - no_show_by_channel
                - no_show_by_duration_band
                - no_show_by_airline

    """

    df = master.copy()

    if analysis_cutoff_date is None:
        analysis_cutoff_date = pd.Timestamp.today().normalize()
    else:
        analysis_cutoff_date = pd.to_datetime(analysis_cutoff_date)

    df["entryDate"] = pd.to_datetime(df["entryDate"], errors="coerce")

    df["is_no_show"] = (
        df["is_valid_booking"]
        & df["actual_entry_ts"].isna()
        & (df["entryDate"] < analysis_cutoff_date)
    )

    df["lead_time_band"] = pd.cut(
        df["lead_time_days_calc"],
        bins=[-np.inf, 0, 1, 3, 7, 14, 28, 56, np.inf],
        labels=[
            "same day or negative",
            "1 day",
            "2-3 days",
            "4-7 days",
            "8-14 days",
            "15-28 days",
            "29-56 days",
            "57+ days",
        ],
    )

    eligible = df[
        df["is_valid_booking"]
        & (df["entryDate"] < analysis_cutoff_date)
    ].copy()

    def no_show_rate_by(group_cols):
        out = (
            eligible
            .groupby(group_cols, dropna=False, observed=False)
            .agg(
                valid_bookings=("bookingId", "count"),
                no_shows=("is_no_show", "sum"),
            )
            .reset_index()
        )

        out["no_show_rate"] = (
            out["no_shows"] / out["valid_bookings"].replace(0, np.nan)
        )

        return out

    no_show_summary = pd.DataFrame(
        [
            {
                "valid_historical_bookings": len(eligible),
                "no_shows": eligible["is_no_show"].sum(),
                "no_show_rate": (
                    eligible["is_no_show"].sum() / len(eligible)
                    if len(eligible) > 0
                    else np.nan
                ),
            }
        ]
    )

    # --------------------------------------------------------
    # Temporary validation check for short-duration no-shows
    # --------------------------------------------------------
    # This diagnostic helps confirm whether high no-show rates in
    # short-duration bookings are caused by genuine no-shows or by
    # missing operational matches.

    duration_match_check = (
        eligible
        .groupby("planned_duration_band", observed=False)
        .agg(
            bookings=("bookingId", "count"),
            no_shows=("is_no_show", "sum"),
            operation_matches=("BookingReference", lambda x: x.notna().sum()),
            actual_checkins=("actual_entry_ts", lambda x: x.notna().sum()),
        )
        .reset_index()
    )

    duration_match_check["no_show_rate"] = (
        duration_match_check["no_shows"]
        / duration_match_check["bookings"].replace(0, np.nan)
    )

    duration_match_check["operation_match_rate"] = (
        duration_match_check["operation_matches"]
        / duration_match_check["bookings"].replace(0, np.nan)
    )

    duration_match_check["actual_checkin_rate"] = (
        duration_match_check["actual_checkins"]
        / duration_match_check["bookings"].replace(0, np.nan)
    )

    print("Duration match check:")
    print(duration_match_check)

    results = {
        "no_show_summary": no_show_summary,
        "no_show_by_lead_time": no_show_rate_by(["lead_time_band"]),
        "no_show_by_duration_band": no_show_rate_by(["planned_duration_band"]),
        "no_show_by_duration_and_lead_time": no_show_rate_by(["planned_duration_band", "lead_time_band"]),

        #Temporary validation table for investigating high no-show rates in short-duration bookings.
        "duration_match_check": duration_match_check,

        #supporting analysis. uncomment if required for future modelling work. 
        #"no_show_by_airline": no_show_rate_by(["outboundAirline"]),
        #"no_show_by_weekday": no_show_rate_by(["entry_weekday"]),
        #"no_show_by_month": no_show_rate_by(["entry_month"]),
    }

    return results

# ============================================================
# 5. ACTUAL ENTRY AND EXIT DEMAND
# ============================================================

def create_daily_fastpark_actuals(master):
    """
    Create actual daily entry, exit and movement volumes.

    Entry definition:
        actual entry timestamp = CheckInEnded

    Exit definition:
        actual exit timestamp = ActualCheckedOutDate

    Parameters:
        master: pandas.DataFrame
            Master booking-operation dataframe.

    Returns:
        pandas.DataFrame:
            Daily actuals table with:
                - date
                - entries
                - exits
                - movements
                - net_flow
                - estimated occupancy movement
    """

    # Entries by actual entry date
    daily_entries = (
        master
        .dropna(subset=["actual_entry_ts"])
        .groupby("actual_entry_date")
        .agg(entries=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"actual_entry_date": "date"})
    )

    # Exits by actual exit date
    daily_exits = (
        master
        .dropna(subset=["actual_exit_ts"])
        .groupby("actual_exit_date")
        .agg(exits=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"actual_exit_date": "date"})
    )

    # Combine
    daily = daily_entries.merge(daily_exits, on="date", how="outer").fillna(0)

    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date")

    daily["movements"] = daily["entries"] + daily["exits"]
    daily["net_flow"] = daily["entries"] - daily["exits"]

    daily["relative_occupancy"] = daily["net_flow"].cumsum()

    return daily

def create_estimated_occupancy_series(daily_fastpark_actuals, config):
    """
    Reconstruct an estimated historic cars-on-site series by anchoring
    daily entries and exits to a known current occupancy position.

    Purpose:
        Estimate historical occupancy and capacity pressure when true
        historic occupancy snapshots are not available.

    Logic:
        Forward relationship:
            cars_on_site_today
            =
            cars_on_site_yesterday
            + entries_today
            - exits_today

        Backward relationship:
            cars_on_site_yesterday
            =
            cars_on_site_today
            - entries_today
            + exits_today

    Important:
        This is an estimated occupancy series. It assumes:
            - the anchor cars-on-site value is accurate
            - entries and exits are complete
            - no external manual adjustments are missing
            - the anchor date is aligned with the actuals data
    """

    current_cars_on_site = config.get("current_cars_on_site")
    total_capacity = config.get("total_fastpark_capacity")
    anchor_date = config.get("occupancy_anchor_date")

    if current_cars_on_site is None or total_capacity is None:
        return pd.DataFrame()

    if total_capacity == 0:
        return pd.DataFrame()

    df = daily_fastpark_actuals.copy()

    df["date"] = pd.to_datetime(
        df["date"],
        errors="coerce",
    )

    df = df.dropna(subset=["date"]).copy()

    if df.empty:
        return pd.DataFrame()

    df = df.sort_values("date").copy()

    for col in ["entries", "exits", "movements", "net_flow"]:
        if col not in df.columns:
            df[col] = 0

        df[col] = df[col].fillna(0)

    if anchor_date is None:
        anchor_date = df["date"].max()
    else:
        anchor_date = pd.to_datetime(anchor_date)

    min_date = df["date"].min()
    max_date = max(df["date"].max(), anchor_date)

    full_dates = pd.DataFrame(
        {
            "date": pd.date_range(
                start=min_date,
                end=max_date,
                freq="D",
            )
        }
    )

    daily = (
        full_dates
        .merge(
            df[
                [
                    "date",
                    "entries",
                    "exits",
                    "movements",
                    "net_flow",
                ]
            ],
            on="date",
            how="left",
        )
        .fillna(
            {
                "entries": 0,
                "exits": 0,
                "movements": 0,
                "net_flow": 0,
            }
        )
    )

    daily = daily.sort_values("date").reset_index(drop=True)

    available_anchor_dates = daily[
        daily["date"].le(anchor_date)
    ]

    if available_anchor_dates.empty:
        selected_anchor_date = daily["date"].max()
    else:
        selected_anchor_date = available_anchor_dates["date"].max()

    if selected_anchor_date != anchor_date:
        print(
            "Occupancy anchor date was not available in daily actuals. "
            f"Requested {anchor_date.date()}, using "
            f"{selected_anchor_date.date()} instead."
        )

    anchor_idx = daily.index[
        daily["date"].eq(selected_anchor_date)
    ][0]

    daily["estimated_cars_on_site"] = np.nan

    daily.loc[
        anchor_idx,
        "estimated_cars_on_site",
    ] = current_cars_on_site

    # Reconstruct backwards from the anchor date.
    for pos in range(anchor_idx - 1, -1, -1):

        next_pos = pos + 1

        next_entries = daily.loc[
            next_pos,
            "entries",
        ]

        next_exits = daily.loc[
            next_pos,
            "exits",
        ]

        next_cars_on_site = daily.loc[
            next_pos,
            "estimated_cars_on_site",
        ]

        daily.loc[
            pos,
            "estimated_cars_on_site",
        ] = (
            next_cars_on_site
            - next_entries
            + next_exits
        )

    # Reconstruct forwards if there are dates after the anchor.
    for pos in range(anchor_idx + 1, len(daily)):

        current_entries = daily.loc[
            pos,
            "entries",
        ]

        current_exits = daily.loc[
            pos,
            "exits",
        ]

        previous_cars_on_site = daily.loc[
            pos - 1,
            "estimated_cars_on_site",
        ]

        daily.loc[
            pos,
            "estimated_cars_on_site",
        ] = (
            previous_cars_on_site
            + current_entries
            - current_exits
        )

    daily["total_fastpark_capacity"] = total_capacity

    daily["estimated_available_spaces"] = (
        daily["total_fastpark_capacity"]
        - daily["estimated_cars_on_site"]
    )

    daily["estimated_occupancy_pct"] = (
        daily["estimated_cars_on_site"]
        / daily["total_fastpark_capacity"].replace(0, np.nan)
    )

    daily["estimated_occupancy_pct_clipped"] = (
        daily["estimated_occupancy_pct"]
        .clip(lower=0, upper=1)
    )

    daily["estimated_occupancy_pct_display"] = (
        daily["estimated_occupancy_pct"]
        * 100
    )

    daily["occupancy_anchor_date"] = selected_anchor_date
    daily["occupancy_anchor_cars_on_site"] = current_cars_on_site

    daily["estimated_occupancy_band"] = pd.cut(
        daily["estimated_occupancy_pct"],
        bins=[
            -np.inf,
            0.50,
            0.60,
            0.70,
            0.80,
            0.90,
            1.00,
            np.inf,
        ],
        labels=[
            "<50%",
            "50-60%",
            "60-70%",
            "70-80%",
            "80-90%",
            "90-100%",
            ">100%",
        ],
    )

    return daily

def create_hourly_fastpark_actuals(master):
    """
    Create actual hourly entry, exit and movement volumes.

    This is essential because the operational requirement is hourly FTE,
    not just daily demand.

    Parameters:
        master: pandas.DataFrame
            Master booking-operation dataframe.

    Returns:
        pandas.DataFrame:
            Hourly actuals table with:
                - datetime_hour
                - date
                - hour
                - entries
                - exits
                - movements
                - net_flow
    """

    # Create hourly bins:
    
    entries_hourly = (
        master
        .dropna(subset=["actual_entry_ts"])
        .assign(datetime_hour=lambda x: x["actual_entry_ts"].dt.floor("h"))
        .groupby("datetime_hour")
        .agg(entries=("bookingId", "nunique"))
        .reset_index()
    )
    
    exits_hourly = (
        master
        .dropna(subset=["actual_exit_ts"])
        .assign(datetime_hour=lambda x: x["actual_exit_ts"].dt.floor("h"))
        .groupby("datetime_hour")
        .agg(exits=("bookingId", "nunique"))
        .reset_index()
    )
    
    hourly = entries_hourly.merge(exits_hourly, on="datetime_hour", how="outer").fillna(0)
    hourly["movements"] = hourly["entries"] + hourly["exits"]
    hourly["net_flow"] = hourly["entries"] - hourly["exits"]
    hourly["date"] = hourly["datetime_hour"].dt.date
    hourly["hour"] = hourly["datetime_hour"].dt.hour
    hourly["weekday"] = hourly["datetime_hour"].dt.day_name()
    hourly["month"] = hourly["datetime_hour"].dt.month

    return hourly


def create_hourly_profiles(hourly_actuals):
    """
    Create hourly show-up / workload profiles.

    Purpose:
        If the model forecasts 100 entries on a Friday,
        this helps determine how those entries should be spread by hour.

    Parameters:
        hourly_actuals: pandas.DataFrame
            Hourly actuals table.

    Returns:
        dict:
            Tables containing hourly entry, exit and movement profiles by:
                - overall
                - weekday
                - month
                - weekday and month
    """

    # Overall hourly entry profile
    entry_profile_overall = (
        hourly_actuals
        .groupby("hour")
        .agg(entries=("entries", "sum"))
        .reset_index()
    )
    entry_profile_overall["entry_hour_share"] = (
        entry_profile_overall["entries"] / entry_profile_overall["entries"].sum()
    )

    exit_profile_overall = (
        hourly_actuals
        .groupby("hour")
        .agg(exits=("exits", "sum"))
        .reset_index()
    )
    exit_profile_overall["exit_hour_share"] = (
        exit_profile_overall["exits"] / exit_profile_overall["exits"].sum()
    )

    movement_profile_overall = (
        hourly_actuals
        .groupby("hour")
        .agg(movements=("movements", "sum"))
        .reset_index()
    )
    movement_profile_overall["movement_hour_share"] = (
        movement_profile_overall["movements"] / movement_profile_overall["movements"].sum()
    )

    # Weekday-hour profile
    entry_profile_weekday = (
        hourly_actuals
        .groupby(["weekday", "hour"])
        .agg(entries=("entries", "sum"))
        .reset_index()
    )
    entry_profile_weekday["entry_hour_share"] = (
        entry_profile_weekday["entries"]
        / entry_profile_weekday.groupby("weekday")["entries"].transform("sum")
    )

    exit_profile_weekday = (
        hourly_actuals
        .groupby(["weekday", "hour"])
        .agg(exits=("exits", "sum"))
        .reset_index()
    )
    exit_profile_weekday["exit_hour_share"] = (
        exit_profile_weekday["exits"]
        / exit_profile_weekday.groupby("weekday")["exits"].transform("sum")
    )

    movement_profile_weekday = (
        hourly_actuals
        .groupby(["weekday", "hour"])
        .agg(movements=("movements", "sum"))
        .reset_index()
    )
    movement_profile_weekday["movement_hour_share"] = (
        movement_profile_weekday["movements"]
        / movement_profile_weekday.groupby("weekday")["movements"].transform("sum")
    )

    entry_profile_by_month = (
        hourly_actuals
        .groupby(["month", "hour"])
        .agg(entries=("entries", "sum"))
        .reset_index()
    )
    entry_profile_by_month["entry_hour_share"] = (
        entry_profile_by_month["entries"]
        / entry_profile_by_month.groupby("month")["entries"].transform("sum") 
    )

    exit_profile_by_month = (
        hourly_actuals
        .groupby(["month", "hour"])
        .agg(exits=("exits", "sum"))
        .reset_index()
    )  
    exit_profile_by_month["exit_hour_share"] = (
        exit_profile_by_month["exits"]
        / exit_profile_by_month.groupby("month")["exits"].transform("sum")
    )   

    movement_profile_by_month = (
        hourly_actuals
        .groupby(["month", "hour"])
        .agg(movements=("movements", "sum"))
        .reset_index()
    )
    movement_profile_by_month["movement_hour_share"] = (
        movement_profile_by_month["movements"]
        / movement_profile_by_month.groupby("month")["movements"].transform("sum")
    )

    results = {
        "entry_profile_overall": entry_profile_overall,
        "exit_profile_overall": exit_profile_overall,
        "movement_profile_overall": movement_profile_overall,
        "entry_profile_by_weekday": entry_profile_weekday,
        "exit_profile_by_weekday": exit_profile_weekday,
        "movement_profile_by_weekday": movement_profile_weekday,


        #Supporting analysis. Uncomment if required for future modelling work.
        # "entry_profile_by_month": entry_profile_by_month,
        # "exit_profile_by_month": exit_profile_by_month,
        # "movement_profile_by_month": movement_profile_by_month,
    }

    return results

# ============================================================
# 6. FLIGHT / PASSENGER CONTEXT
# ============================================================

def create_daily_passenger_summary(flights_clean, config):
    """
    Aggregate flight performance data to daily passenger context.

    Purpose:
        Create the passenger base used to analyse fastpark penetration.

    Parameters:
        flights_clean: pandas.DataFrame
            Cleaned flight performance dataframe.

        config: dict
            Configuration dictionary.

    Returns:
        pandas.DataFrame:
            Daily passenger summary with:
                - date
                - departing_pax
                - arriving_pax
                - total_pax
                - departing_flights
                - arriving_flights
                - domestic_departing_pax
                - international_departing_pax
                - domestic_arriving_pax
                - international_arriving_pax
                - airline mix fields
                - country mix fields
    """


    daily_departures = (
        flights_clean[flights_clean["is_departure"]]
        .groupby("flight_date")
        .agg(
            departing_pax=("analysis_pax", "sum"),
            departing_flights=("FlightID", "nunique")
        )
        .reset_index()
        .rename(columns={"flight_date": "date"})
    )
    
    daily_arrivals = (
        flights_clean[flights_clean["is_arrival"]]
        .groupby("flight_date")
        .agg(
            arriving_pax=("analysis_pax", "sum"),
            arriving_flights=("FlightID", "nunique")
        )
        .reset_index()
        .rename(columns={"flight_date": "date"})
    )
    
    daily_pax = daily_departures.merge(daily_arrivals, on="date", how="outer").fillna(0)
    daily_pax["total_pax"] = daily_pax["departing_pax"] + daily_pax["arriving_pax"]

    daily_domestic_departing = (
        flights_clean[flights_clean["is_departure"] & flights_clean["is_domestic"]]
        .groupby("flight_date")
        .agg(domestic_departing_pax=("analysis_pax", "sum"))
        .reset_index()
        .rename(columns={"flight_date": "date"})
    )

    daily_international_departing = (
        flights_clean[flights_clean["is_departure"] & flights_clean["is_international"]]
        .groupby("flight_date")
        .agg(international_departing_pax=("analysis_pax", "sum"))
        .reset_index()
        .rename(columns={"flight_date": "date"})
    )

    daily_domestic_arriving = (
        flights_clean[flights_clean["is_arrival"] & flights_clean["is_domestic"]]
        .groupby("flight_date")
        .agg(domestic_arriving_pax=("analysis_pax", "sum"))
        .reset_index()
        .rename(columns={"flight_date": "date"})
    )

    daily_international_arriving = (
        flights_clean[flights_clean["is_arrival"] & flights_clean["is_international"]]
        .groupby("flight_date")
        .agg(international_arriving_pax=("analysis_pax", "sum"))
        .reset_index()
        .rename(columns={"flight_date": "date"})
    )


    daily_pax = daily_pax.merge(daily_domestic_departing, on="date", how="left").fillna(0)
    daily_pax = daily_pax.merge(daily_international_departing, on="date", how="left").fillna(0)
    daily_pax = daily_pax.merge(daily_domestic_arriving, on="date", how="left").fillna(0)
    daily_pax = daily_pax.merge(daily_international_arriving, on="date", how="left").fillna(0)


    ##ADD IN DAILY AIRLINE MIX, COUNTRY MIX ??HOW?

    return daily_pax


def create_hourly_passenger_summary(flights_clean, config):
    """
    Aggregate flight performance data to hourly passenger context.

    This may be useful, but hourly fast park entry may not align perfectly
    to scheduled departure hour because customers arrive before flights.

    Parameters:
        flights_clean: pandas.DataFrame
            Cleaned flight performance dataframe.

        config: dict
            Configuration dictionary.

    Returns:
        pandas.DataFrame:
            Hourly passenger summary with departure and arrival pax by hour.
    """

    hourly_departures = (
        flights_clean[flights_clean["is_departure"]]
        .groupby(["flight_date", "flight_hour"])
        .agg(
            departing_pax=("analysis_pax", "sum"),
            departing_flights=("FlightID", "nunique"),
        )
        .reset_index()
        .rename(columns={"flight_date": "date", "flight_hour": "hour"})
    )

    hourly_arrivals = (
        flights_clean[flights_clean["is_arrival"]]
        .groupby(["flight_date", "flight_hour"])
        .agg(
            arriving_pax=("analysis_pax", "sum"),
            arriving_flights=("FlightID", "nunique"),
        )
        .reset_index()
        .rename(columns={"flight_date": "date", "flight_hour": "hour"})
    )

    hourly_pax = hourly_departures.merge(
        hourly_arrivals,
        on=["date", "hour"],
        how="outer",
    ).fillna(0)

    hourly_pax["date"] = pd.to_datetime(hourly_pax["date"])
    hourly_pax["hour"] = hourly_pax["hour"].astype(int)

    hourly_pax["total_pax"] = (
        hourly_pax["departing_pax"] + hourly_pax["arriving_pax"]
    )

    hourly_pax = hourly_pax.sort_values(["date", "hour"])

    return hourly_pax

def create_airline_country_mix_features(flights_clean):
    """
    Create daily airline, country and domestic/international mix features.

    Purpose:
        Test whether fastpark penetration changes depending on the mix of passengers,
        not just the total number of passengers.

    Parameters:
        flights_clean: pandas.DataFrame
            Cleaned flight performance dataframe.

    Returns:
        dict:
            Mix feature tables:
                - daily_airline_mix
                - daily_country_mix
                - daily_domestic_international_mix
    """

    df = flights_clean.copy()
    departures = df[df["is_departure"]].copy()

    # --------------------------------------------------------
    # Airline mix
    # --------------------------------------------------------
    daily_airline_mix = (
        departures
        .groupby(["flight_date", "Airline_Description"], dropna=False)
        .agg(airline_departing_pax=("analysis_pax", "sum"))
        .reset_index()
    )

    daily_airline_mix["total_departing_pax"] = (
        daily_airline_mix
        .groupby("flight_date")["airline_departing_pax"]
        .transform("sum")
    )

    daily_airline_mix["airline_departing_pax_share"] = (
        daily_airline_mix["airline_departing_pax"]
        / daily_airline_mix["total_departing_pax"].replace(0, np.nan)
    )

    daily_airline_mix = daily_airline_mix.rename(columns={"flight_date": "date"})
    daily_airline_mix["date"] = pd.to_datetime(daily_airline_mix["date"])

    # --------------------------------------------------------
    # Country mix
    # --------------------------------------------------------
    daily_country_mix = (
        departures
        .groupby(["flight_date", "CountryName"], dropna=False)
        .agg(country_departing_pax=("analysis_pax", "sum"))
        .reset_index()
    )

    daily_country_mix["total_departing_pax"] = (
        daily_country_mix
        .groupby("flight_date")["country_departing_pax"]
        .transform("sum")
    )

    daily_country_mix["country_departing_pax_share"] = (
        daily_country_mix["country_departing_pax"]
        / daily_country_mix["total_departing_pax"].replace(0, np.nan)
    )

    daily_country_mix = daily_country_mix.rename(columns={"flight_date": "date"})
    daily_country_mix["date"] = pd.to_datetime(daily_country_mix["date"])

    # --------------------------------------------------------
    # Domestic / international mix
    # --------------------------------------------------------
    daily_dom_int = (
        departures
        .groupby(["flight_date", "Domestic_International"], dropna=False)
        .agg(departing_pax=("analysis_pax", "sum"))
        .reset_index()
    )

    daily_dom_int["total_departing_pax"] = (
        daily_dom_int
        .groupby("flight_date")["departing_pax"]
        .transform("sum")
    )

    daily_dom_int["departing_pax_share"] = (
        daily_dom_int["departing_pax"]
        / daily_dom_int["total_departing_pax"].replace(0, np.nan)
    )

    daily_domestic_international_mix = daily_dom_int.pivot_table(
        index="flight_date",
        columns="Domestic_International",
        values=["departing_pax", "departing_pax_share"],
        aggfunc="sum",
    )

    daily_domestic_international_mix.columns = [
        "_".join([str(x) for x in col if str(x) != ""])
        for col in daily_domestic_international_mix.columns
    ]

    daily_domestic_international_mix = (
        daily_domestic_international_mix
        .reset_index()
        .rename(columns={"flight_date": "date"})
    )

    daily_domestic_international_mix["date"] = pd.to_datetime(
        daily_domestic_international_mix["date"]
    )

    results = {
        "daily_airline_mix": daily_airline_mix,
        "daily_country_mix": daily_country_mix,
        "daily_domestic_international_mix": daily_domestic_international_mix,
    }

    return results


# ============================================================
# 7. COMBINE FASTPARK ACTUALS WITH PASSENGER CONTEXT
# ============================================================

def create_daily_driver_dataset(
    daily_fastpark_actuals,
    daily_passenger_summary,
    daily_price_summary=None,
    daily_occupancy_summary=None,
):
    """
    Create a daily modelling / analysis dataset by joining fastpark actuals
    to airport passenger context.

    Purpose:
        Analyse what explains actual entries and exits.

    Parameters:
        daily_fastpark_actuals: pandas.DataFrame
            Daily entries, exits and movements.

        daily_passenger_summary: pandas.DataFrame
            Daily passenger context.

    Returns:
        pandas.DataFrame:
            Daily driver dataset with penetration measures.
    """

    fastpark = daily_fastpark_actuals.copy()
    pax = daily_passenger_summary.copy()

    fastpark["date"] = pd.to_datetime(fastpark["date"])
    pax["date"] = pd.to_datetime(pax["date"])

    daily = fastpark.merge(
        pax,
        on="date",
        how="left"
    )

    # --------------------------------------------------------
    # Optional daily price context
    # --------------------------------------------------------
    if daily_price_summary is not None:

        price = daily_price_summary.copy()
        price["date"] = pd.to_datetime(price["date"], errors="coerce")

        daily = daily.merge(
            price,
            on="date",
            how="left",
        )

        # --------------------------------------------------------
    # Optional estimated occupancy context
    # --------------------------------------------------------
    if daily_occupancy_summary is not None:

        occupancy = daily_occupancy_summary.copy()
        occupancy["date"] = pd.to_datetime(
            occupancy["date"],
            errors="coerce",
        )

        occupancy_cols = [
            "date",
            "estimated_cars_on_site",
            "total_fastpark_capacity",
            "estimated_available_spaces",
            "estimated_occupancy_pct",
            "estimated_occupancy_pct_clipped",
            "estimated_occupancy_pct_display",
            "estimated_occupancy_band",
        ]

        occupancy_cols = [
            col for col in occupancy_cols
            if col in occupancy.columns
        ]

        daily = daily.merge(
            occupancy[occupancy_cols],
            on="date",
            how="left",
        )

    # --------------------------------------------------------
    # Penetration metrics
    # --------------------------------------------------------
    daily["entry_penetration"] = (
        daily["entries"] / daily["departing_pax"].replace(0, np.nan)
    )

    daily["exit_penetration"] = (
        daily["exits"] / daily["arriving_pax"].replace(0, np.nan)
    )

    # --------------------------------------------------------
    # Calendar features
    # --------------------------------------------------------
    daily["date"] = pd.to_datetime(daily["date"])
    daily["weekday"] = daily["date"].dt.day_name()
    daily["weekday_num"] = daily["date"].dt.weekday

    daily["month"] = daily["date"].dt.month
    daily["month_name"] = daily["date"].dt.month_name()

    daily["year"] = daily["date"].dt.year
    daily["year_month"] = daily["date"].dt.to_period("M").astype(str)

    daily["week"] = daily["date"].dt.isocalendar().week

    # --------------------------------------------------------
    # Passenger mix shares
    # --------------------------------------------------------
    daily["domestic_departing_share"] = (
        daily["domestic_departing_pax"]
        / daily["departing_pax"].replace(0, np.nan)
    )

    daily["international_departing_share"] = (
        daily["international_departing_pax"]
        / daily["departing_pax"].replace(0, np.nan)
    )

    daily["domestic_arriving_share"] = (
        daily["domestic_arriving_pax"]
        / daily["arriving_pax"].replace(0, np.nan)
    )

    daily["international_arriving_share"] = (
        daily["international_arriving_pax"]
        / daily["arriving_pax"].replace(0, np.nan)
    )

    return daily

def analyse_mix_features(daily_driver_dataset):
    """
    Summarise domestic/international passenger mix and its relationship
    with FastPark entries, exits and penetration rates.

    Departure mix is most relevant to entries.
    Arrival mix is most relevant to exits.
    """

    df = daily_driver_dataset.copy()

    mix_rows = []

    mix_cols = [
        "domestic_departing_share",
        "international_departing_share",
        "domestic_arriving_share",
        "international_arriving_share",
    ]

    for col in mix_cols:

        if col not in df.columns:
            continue

        valid = df[
            [
                col,
                "entries",
                "exits",
                "entry_penetration",
                "exit_penetration",
            ]
        ].replace([np.inf, -np.inf], np.nan)

        mix_rows.append(
            {
                "mix_variable": col,
                "average_share": valid[col].mean(),
                "minimum_share": valid[col].min(),
                "maximum_share": valid[col].max(),

                "corr_entries": valid[col].corr(valid["entries"]),
                "corr_exits": valid[col].corr(valid["exits"]),

                "corr_entry_penetration": valid[col].corr(
                    valid["entry_penetration"]
                ),
                "corr_exit_penetration": valid[col].corr(
                    valid["exit_penetration"]
                ),
            }
        )

    return pd.DataFrame(mix_rows)

def analyse_estimated_occupancy(daily_driver_dataset):
    """
    Analyse the relationship between estimated occupancy, demand and price.

    Purpose:
        Understand whether higher capacity pressure is associated with:
            - higher prices
            - higher or lower bookings
            - higher or lower entries/exits
            - different penetration rates
            - different lead times or durations
    """

    df = daily_driver_dataset.copy()

    if "estimated_occupancy_band" not in df.columns:
        return {
            "occupancy_band_summary": pd.DataFrame(),
            "occupancy_correlation_summary": pd.DataFrame(),
        }

    occupancy_band_summary = (
        df
        .groupby("estimated_occupancy_band", dropna=False, observed=False)
        .agg(
            observations=("date", "count"),

            avg_estimated_cars_on_site=("estimated_cars_on_site", "mean"),
            avg_estimated_available_spaces=("estimated_available_spaces", "mean"),
            avg_estimated_occupancy_pct=("estimated_occupancy_pct", "mean"),

            avg_valid_bookings=("valid_bookings", "mean"),
            avg_entries=("entries", "mean"),
            avg_exits=("exits", "mean"),
            avg_movements=("movements", "mean"),
            avg_net_flow=("net_flow", "mean"),

            avg_departing_pax=("departing_pax", "mean"),
            avg_arriving_pax=("arriving_pax", "mean"),
            avg_total_pax=("total_pax", "mean"),

            avg_entry_penetration=("entry_penetration", "mean"),
            avg_exit_penetration=("exit_penetration", "mean"),

            avg_booking_total=("avg_booking_total", "mean"),
            avg_price_per_day=("avg_price_per_day", "mean"),
            median_price_per_day=("median_price_per_day", "mean"),
            avg_product_price_per_day=("avg_product_price_per_day", "mean"),

            avg_lead_time_days=("avg_lead_time_days", "mean"),
            avg_duration_days=("avg_duration_days", "mean"),
        )
        .reset_index()
    )

    occupancy_corr_cols = [
        "estimated_cars_on_site",
        "estimated_available_spaces",
        "estimated_occupancy_pct",
        "estimated_occupancy_pct_clipped",

        "valid_bookings",
        "entries",
        "exits",
        "movements",
        "net_flow",

        "departing_pax",
        "arriving_pax",
        "total_pax",

        "entry_penetration",
        "exit_penetration",

        "avg_booking_total",
        "median_booking_total",
        "avg_price_per_day",
        "median_price_per_day",
        "avg_product_price_per_day",
        "median_product_price_per_day",

        "avg_lead_time_days",
        "median_lead_time_days",
        "avg_duration_days",
        "median_duration_days",
    ]

    occupancy_corr_cols = [
        col for col in occupancy_corr_cols
        if col in df.columns
    ]

    occupancy_correlation_summary = (
        df[occupancy_corr_cols]
        .replace([np.inf, -np.inf], np.nan)
        .corr()
    )

    return {
        "occupancy_band_summary": occupancy_band_summary,
        "occupancy_correlation_summary": occupancy_correlation_summary,
    }

# ============================================================
# 7B. HOURLY FASTPARK VS PASSENGER OFFSET ANALYSIS
# ============================================================

def create_hourly_alignment_dataset(hourly_fastpark_actuals, hourly_passenger_summary):
    """
    Join hourly FastPark actuals to hourly passenger summaries.

    Purpose:
        Create a base hourly dataset for comparing:
            - FastPark entries vs future departing passengers
            - FastPark exits vs previous/recent arriving passengers

    Why this matters:
        FastPark entry time does not necessarily equal flight departure hour.
        Customers may check in several hours before departure.

        FastPark exit time does not necessarily equal flight arrival hour.
        Customers may collect cars after arrival, baggage collection, and walking time.

    Parameters
    ----------
    hourly_fastpark_actuals : pandas.DataFrame
        Expected columns:
            - datetime_hour
            - entries
            - exits
            - movements

    hourly_passenger_summary : pandas.DataFrame
        Expected columns:
            - date
            - hour
            - departing_pax
            - arriving_pax
            - departing_flights
            - arriving_flights

    Returns
    -------
    pandas.DataFrame
        Hourly alignment dataset.
    """

    fastpark = hourly_fastpark_actuals.copy()
    pax = hourly_passenger_summary.copy()

    fastpark["datetime_hour"] = pd.to_datetime(
        fastpark["datetime_hour"]
    )

    pax["date"] = pd.to_datetime(
        pax["date"]
    )

    pax["datetime_hour"] = (
        pax["date"]
        + pd.to_timedelta(pax["hour"], unit="h")
    )

    start_hour = min(
        fastpark["datetime_hour"].min(),
        pax["datetime_hour"].min(),
    )

    end_hour = max(
        fastpark["datetime_hour"].max(),
        pax["datetime_hour"].max(),
    )

    full_hours = pd.DataFrame(
        {
            "datetime_hour": pd.date_range(
                start=start_hour,
                end=end_hour,
                freq="h",
            )
        }
    )

    alignment = (
        full_hours
        .merge(
            fastpark,
            on="datetime_hour",
            how="left",
        )
    )

    alignment = (
        alignment
        .merge(
            pax[
                [
                    "datetime_hour",
                    "departing_pax",
                    "arriving_pax",
                    "departing_flights",
                    "arriving_flights",
                ]
            ],
            on="datetime_hour",
            how="left",
        )
    )

    fill_cols = [
        "entries",
        "exits",
        "movements",
        "net_flow",
        "departing_pax",
        "arriving_pax",
        "departing_flights",
        "arriving_flights",
    ]

    for col in fill_cols:
        alignment[col] = alignment[col].fillna(0)

    alignment = alignment.sort_values(
        "datetime_hour"
    )

    alignment["date"] = alignment["datetime_hour"].dt.date
    alignment["hour"] = alignment["datetime_hour"].dt.hour
    alignment["weekday"] = alignment["datetime_hour"].dt.day_name()
    alignment["month"] = alignment["datetime_hour"].dt.month

    return alignment


def analyse_hourly_passenger_offsets(
    hourly_fastpark_actuals,
    hourly_passenger_summary,
    max_departure_lead_hours=8,
    max_arrival_lag_hours=8,
):
    """
    Analyse which passenger-hour offset best aligns with FastPark demand.

    Entry logic:
        FastPark entries at hour t are compared to departing passengers at:
            t + 0, t + 1, ..., t + max_departure_lead_hours

        Example:
            If entry correlation is strongest at +3 hours, this suggests
            FastPark customers tend to complete kiosk check-in around 3 hours
            before scheduled departure hour.

    Exit logic:
        FastPark exits at hour t are compared to arriving passengers at:
            t, t - 1, ..., t - max_arrival_lag_hours

        Example:
            If exit correlation is strongest at -1 hour, this suggests
            FastPark checkout tends to happen about 1 hour after arrival hour.

    Parameters
    ----------
    hourly_fastpark_actuals : pandas.DataFrame
        Hourly FastPark actuals.

    hourly_passenger_summary : pandas.DataFrame
        Hourly passenger summaries.

    max_departure_lead_hours : int
        Maximum number of hours after FastPark entry to test for departures.

    max_arrival_lag_hours : int
        Maximum number of hours before FastPark exit to test for arrivals.

    Returns
    -------
    dict
        Offset analysis outputs:
            - alignment_dataset
            - entry_departure_offset_summary
            - exit_arrival_offset_summary
            - best_entry_departure_offset
            - best_exit_arrival_offset
    """

    alignment = create_hourly_alignment_dataset(
        hourly_fastpark_actuals=hourly_fastpark_actuals,
        hourly_passenger_summary=hourly_passenger_summary,
    )

    alignment = alignment.sort_values("datetime_hour").copy()

    entry_rows = []

    for lead in range(0, max_departure_lead_hours + 1):
        shifted_col = f"departing_pax_plus_{lead}h"

        # Future departing pax relative to FastPark entry time.
        alignment[shifted_col] = alignment["departing_pax"].shift(-lead)

        temp = alignment[["entries", shifted_col]].dropna()

        if len(temp) > 1:
            corr = temp["entries"].corr(temp[shifted_col])
        else:
            corr = np.nan

        entry_rows.append(
            {
                "comparison": "entries_vs_future_departing_pax",
                "offset_hours": lead,
                "interpretation": f"FastPark entries compared with departures {lead} hours later",
                "correlation": corr,
                "observations": len(temp),
            }
        )

    exit_rows = []

    for lag in range(0, max_arrival_lag_hours + 1):
        shifted_col = f"arriving_pax_minus_{lag}h"

        # Previous arriving pax relative to FastPark exit time.
        alignment[shifted_col] = alignment["arriving_pax"].shift(lag)

        temp = alignment[["exits", shifted_col]].dropna()

        if len(temp) > 1:
            corr = temp["exits"].corr(temp[shifted_col])
        else:
            corr = np.nan

        exit_rows.append(
            {
                "comparison": "exits_vs_previous_arriving_pax",
                "offset_hours": lag,
                "interpretation": f"FastPark exits compared with arrivals {lag} hours earlier",
                "correlation": corr,
                "observations": len(temp),
            }
        )

    entry_offset_summary = pd.DataFrame(entry_rows)
    exit_offset_summary = pd.DataFrame(exit_rows)

    best_entry_offset = (
        entry_offset_summary
        .sort_values("correlation", ascending=False)
        .head(1)
    )

    best_exit_offset = (
        exit_offset_summary
        .sort_values("correlation", ascending=False)
        .head(1)
    )

    return {
        "alignment_dataset": alignment,
        "entry_departure_offset_summary": entry_offset_summary,
        "exit_arrival_offset_summary": exit_offset_summary,
        "best_entry_departure_offset": best_entry_offset,
        "best_exit_arrival_offset": best_exit_offset,
    }

# ============================================================
# 8. ACTUAL DEMAND DRIVER ANALYSIS
# ============================================================

def analyse_actual_demand_drivers(daily_driver_dataset):
    """
    Analyse relationships between FastPark demand and airport passenger variables.

    Purpose:
        Identify what genuinely affects actual entries and exits.

    Returns:
        dict:
            Driver analysis outputs:
                - demand_driver_summary
                - correlation_summary
                - weekday_summary
                - monthly_summary
                - month_by_year_summary
                - passenger_volume_band_summary
                - exploratory passenger-band penetration evaluation
    """

    df = daily_driver_dataset.copy()

    df["date"] = pd.to_datetime(df["date"])

    if "month_name" not in df.columns:
        df["month_name"] = df["date"].dt.month_name()

    if "year_month" not in df.columns:
        df["year_month"] = df["date"].dt.to_period("M").astype(str)

    # --------------------------------------------------------
    # Correlation summary
    # --------------------------------------------------------
    corr_cols = [
        "entries",
        "exits",
        "movements",
        "net_flow",
        "relative_occupancy",

        "departing_pax",
        "arriving_pax",
        "total_pax",
        "departing_flights",
        "arriving_flights",

        "domestic_departing_pax",
        "international_departing_pax",
        "domestic_arriving_pax",
        "international_arriving_pax",
        "domestic_departing_share",
        "international_departing_share",
        "domestic_arriving_share",
        "international_arriving_share",

        "entry_penetration",
        "exit_penetration",

        # Estimated capacity / occupancy drivers
        "estimated_cars_on_site",
        "estimated_available_spaces",
        "estimated_occupancy_pct",
        "estimated_occupancy_pct_clipped",
        "estimated_occupancy_pct_display",

        # Daily booking / price drivers
        "valid_bookings",

        "avg_booking_total",
        "median_booking_total",

        "avg_product_price",
        "median_product_price",

        "avg_price_per_day",
        "median_price_per_day",

        "avg_price_per_quantity",
        "median_price_per_quantity",

        "avg_price_per_quantity_per_day",
        "median_price_per_quantity_per_day",

        "avg_product_price_per_day",
        "median_product_price_per_day",

        "avg_lead_time_days",
        "median_lead_time_days",

        "avg_duration_days",
        "median_duration_days",
    ]

    corr_cols = [col for col in corr_cols if col in df.columns]

    correlation_summary = df[corr_cols].corr()

    # --------------------------------------------------------
    # Focused price correlation summary
    # --------------------------------------------------------
    price_driver_cols = [
        "valid_bookings",

        "avg_booking_total",
        "median_booking_total",

        "avg_product_price",
        "median_product_price",

        "avg_price_per_day",
        "median_price_per_day",

        "avg_price_per_quantity",
        "median_price_per_quantity",

        "avg_price_per_quantity_per_day",
        "median_price_per_quantity_per_day",

        "avg_product_price_per_day",
        "median_product_price_per_day",

        "avg_lead_time_days",
        "median_lead_time_days",

        "avg_duration_days",
        "median_duration_days",
    ]

    demand_outcome_cols = [
        "valid_bookings",
        "entries",
        "exits",
        "movements",
        "net_flow",
        "relative_occupancy",
        "entry_penetration",
        "exit_penetration",
        "departing_pax",
        "arriving_pax",
        "total_pax",
    ]

    price_driver_cols = [
        col for col in price_driver_cols
        if col in correlation_summary.index
    ]

    demand_outcome_cols = [
        col for col in demand_outcome_cols
        if col in correlation_summary.columns
    ]

    if price_driver_cols and demand_outcome_cols:
        price_correlation_summary = (
            correlation_summary
            .loc[price_driver_cols, demand_outcome_cols]
            .reset_index()
            .rename(columns={"index": "price_or_booking_metric"})
        )
    else:
        price_correlation_summary = pd.DataFrame()

    # --------------------------------------------------------
    # Weekly / weekday summary
    # --------------------------------------------------------
    weekday_summary = (
        df
        .groupby(["weekday_num", "weekday"], dropna=False)
        .agg(
            observations=("date", "count"),
            avg_entries=("entries", "mean"),
            avg_exits=("exits", "mean"),
            avg_movements=("movements", "mean"),
            avg_net_flow=("net_flow", "mean"),
            avg_departing_pax=("departing_pax", "mean"),
            avg_arriving_pax=("arriving_pax", "mean"),
            avg_total_pax=("total_pax", "mean"),
            avg_entry_penetration=("entry_penetration", "mean"),
            avg_exit_penetration=("exit_penetration", "mean"),
            avg_estimated_cars_on_site=("estimated_cars_on_site", "mean"),
            avg_estimated_available_spaces=("estimated_available_spaces", "mean"),
            avg_estimated_occupancy_pct=("estimated_occupancy_pct", "mean"),
            avg_valid_bookings=("valid_bookings", "mean"),
            avg_booking_total=("avg_booking_total", "mean"),
            avg_price_per_day=("avg_price_per_day", "mean"),
            median_price_per_day=("median_price_per_day", "mean"),
            avg_product_price_per_day=("avg_product_price_per_day", "mean"),
            avg_lead_time_days=("avg_lead_time_days", "mean"),
            avg_duration_days=("avg_duration_days", "mean"),
        )

        .reset_index()
        .sort_values("weekday_num")
    )

    # --------------------------------------------------------
    # Monthly summary across all years
    # --------------------------------------------------------
    monthly_summary = (
        df
        .groupby(["month", "month_name"], dropna=False)
        .agg(
            observations=("date", "count"),
            avg_entries=("entries", "mean"),
            avg_exits=("exits", "mean"),
            avg_movements=("movements", "mean"),
            avg_net_flow=("net_flow", "mean"),
            avg_departing_pax=("departing_pax", "mean"),
            avg_arriving_pax=("arriving_pax", "mean"),
            avg_total_pax=("total_pax", "mean"),
            avg_entry_penetration=("entry_penetration", "mean"),
            avg_exit_penetration=("exit_penetration", "mean"),
            avg_estimated_cars_on_site=("estimated_cars_on_site", "mean"),
            avg_estimated_available_spaces=("estimated_available_spaces", "mean"),
            avg_estimated_occupancy_pct=("estimated_occupancy_pct", "mean"),
            avg_valid_bookings=("valid_bookings", "mean"),
            avg_booking_total=("avg_booking_total", "mean"),
            avg_price_per_day=("avg_price_per_day", "mean"),
            median_price_per_day=("median_price_per_day", "mean"),
            avg_product_price_per_day=("avg_product_price_per_day", "mean"),
            avg_lead_time_days=("avg_lead_time_days", "mean"),
            avg_duration_days=("avg_duration_days", "mean"),
        )
        .reset_index()
        .sort_values("month")
    )

    # --------------------------------------------------------
    # Month by year summary
    # --------------------------------------------------------
    month_by_year_summary = (
        df
        .groupby(["year", "month", "month_name", "year_month"], dropna=False)
        .agg(
            observations=("date", "count"),
            total_entries=("entries", "sum"),
            total_exits=("exits", "sum"),
            avg_entries=("entries", "mean"),
            avg_exits=("exits", "mean"),
            avg_movements=("movements", "mean"),
            avg_net_flow=("net_flow", "mean"),
            avg_departing_pax=("departing_pax", "mean"),
            avg_arriving_pax=("arriving_pax", "mean"),
            avg_total_pax=("total_pax", "mean"),
            avg_entry_penetration=("entry_penetration", "mean"),
            avg_exit_penetration=("exit_penetration", "mean"),
            avg_estimated_cars_on_site=("estimated_cars_on_site", "mean"),
            avg_estimated_available_spaces=("estimated_available_spaces", "mean"),
            avg_estimated_occupancy_pct=("estimated_occupancy_pct", "mean"),
            avg_valid_bookings=("valid_bookings", "mean"),
            avg_booking_total=("avg_booking_total", "mean"),
            avg_price_per_day=("avg_price_per_day", "mean"),
            median_price_per_day=("median_price_per_day", "mean"),
            avg_product_price_per_day=("avg_product_price_per_day", "mean"),
            avg_lead_time_days=("avg_lead_time_days", "mean"),
            avg_duration_days=("avg_duration_days", "mean"),
        )
        .reset_index()
        .sort_values(["year", "month"])
    )

    # --------------------------------------------------------
    # Passenger volume bands
    # --------------------------------------------------------
    df["departing_pax_band"] = pd.qcut(
        df["departing_pax"],
        q=5,
        duplicates="drop"
    )

    df["arriving_pax_band"] = pd.qcut(
        df["arriving_pax"],
        q=5,
        duplicates="drop"
    )

    departing_band_summary = (
        df
        .groupby("departing_pax_band", observed=False)
        .agg(
            observations=("date", "count"),
            avg_pax=("departing_pax", "mean"),
            min_pax=("departing_pax", "min"),
            max_pax=("departing_pax", "max"),
            avg_entries=("entries", "mean"),
            avg_exits=("exits", "mean"),
            avg_entry_penetration=("entry_penetration", "mean"),
            avg_exit_penetration=("exit_penetration", "mean"),
        )
        .reset_index()
        .rename(columns={"departing_pax_band": "passenger_volume_band"})
    )

    departing_band_summary["band_type"] = "departing_pax_band"

    arriving_band_summary = (
        df
        .groupby("arriving_pax_band", observed=False)
        .agg(
            observations=("date", "count"),
            avg_pax=("arriving_pax", "mean"),
            min_pax=("arriving_pax", "min"),
            max_pax=("arriving_pax", "max"),
            avg_entries=("entries", "mean"),
            avg_exits=("exits", "mean"),
            avg_entry_penetration=("entry_penetration", "mean"),
            avg_exit_penetration=("exit_penetration", "mean"),
        )
        .reset_index()
        .rename(columns={"arriving_pax_band": "passenger_volume_band"})
    )

    arriving_band_summary["band_type"] = "arriving_pax_band"

    passenger_volume_band_summary = pd.concat(
        [
            departing_band_summary,
            arriving_band_summary,
        ],
        ignore_index=True,
    )

    passenger_volume_band_summary = passenger_volume_band_summary[
        [
            "band_type",
            "passenger_volume_band",
            "observations",
            "avg_pax",
            "min_pax",
            "max_pax",
            "avg_entries",
            "avg_exits",
            "avg_entry_penetration",
            "avg_exit_penetration",
        ]
    ]

    # --------------------------------------------------------
    # Passenger-band adjusted penetration forecast evaluation
    # Exploratory only: this indicates whether banded penetration is useful.
    # It should not be treated as a final leakage-safe backtest yet.
    # --------------------------------------------------------
    entry_band_rates = (
        df
        .groupby("departing_pax_band", observed=False)
        .agg(
            band_avg_entry_penetration=("entry_penetration", "mean")
        )
        .reset_index()
    )

    exit_band_rates = (
        df
        .groupby("arriving_pax_band", observed=False)
        .agg(
            band_avg_exit_penetration=("exit_penetration", "mean")
        )
        .reset_index()
    )

    passenger_band_eval = df[
        [
            "date",
            "weekday",
            "month",
            "month_name",
            "year",
            "year_month",
            "departing_pax",
            "arriving_pax",
            "entries",
            "exits",
            "departing_pax_band",
            "arriving_pax_band",
        ]
    ].copy()

    passenger_band_eval = passenger_band_eval.merge(
        entry_band_rates,
        on="departing_pax_band",
        how="left",
    )

    passenger_band_eval = passenger_band_eval.merge(
        exit_band_rates,
        on="arriving_pax_band",
        how="left",
    )

    passenger_band_eval["forecast_entries_pax_band"] = (
        passenger_band_eval["departing_pax"]
        * passenger_band_eval["band_avg_entry_penetration"]
    )

    passenger_band_eval["forecast_exits_pax_band"] = (
        passenger_band_eval["arriving_pax"]
        * passenger_band_eval["band_avg_exit_penetration"]
    )

    passenger_band_eval["entry_error"] = (
        passenger_band_eval["forecast_entries_pax_band"]
        - passenger_band_eval["entries"]
    )

    passenger_band_eval["exit_error"] = (
        passenger_band_eval["forecast_exits_pax_band"]
        - passenger_band_eval["exits"]
    )

    passenger_band_eval["entry_absolute_error"] = (
        passenger_band_eval["entry_error"].abs()
    )

    passenger_band_eval["exit_absolute_error"] = (
        passenger_band_eval["exit_error"].abs()
    )

    passenger_band_eval["entry_percentage_error"] = (
        passenger_band_eval["entry_error"]
        / passenger_band_eval["entries"].replace(0, np.nan)
    )

    passenger_band_eval["exit_percentage_error"] = (
        passenger_band_eval["exit_error"]
        / passenger_band_eval["exits"].replace(0, np.nan)
    )

    passenger_band_eval["entry_absolute_percentage_error"] = (
        passenger_band_eval["entry_percentage_error"].abs()
    )

    passenger_band_eval["exit_absolute_percentage_error"] = (
        passenger_band_eval["exit_percentage_error"].abs()
    )

    passenger_band_eval["entry_squared_error"] = (
        passenger_band_eval["entry_error"] ** 2
    )

    passenger_band_eval["exit_squared_error"] = (
        passenger_band_eval["exit_error"] ** 2
    )

    passenger_band_performance_summary = pd.DataFrame(
        [
            {
                "method": "departing_pax_band_adjusted_entry_penetration",
                "target": "entries",
                "mae": passenger_band_eval["entry_absolute_error"].mean(),
                "bias": passenger_band_eval["entry_error"].mean(),
                "mape": passenger_band_eval["entry_absolute_percentage_error"].mean(),
                "rmse": passenger_band_eval["entry_squared_error"].mean() ** 0.5,
                "records": passenger_band_eval["entries"].count(),
            },
            {
                "method": "arriving_pax_band_adjusted_exit_penetration",
                "target": "exits",
                "mae": passenger_band_eval["exit_absolute_error"].mean(),
                "bias": passenger_band_eval["exit_error"].mean(),
                "mape": passenger_band_eval["exit_absolute_percentage_error"].mean(),
                "rmse": passenger_band_eval["exit_squared_error"].mean() ** 0.5,
                "records": passenger_band_eval["exits"].count(),
            },
        ]
    )

    # --------------------------------------------------------
    # Domestic / international mix, summarised only
    # --------------------------------------------------------
    domestic_mix_summary = {}

    for share_col in [
        "domestic_departing_share",
        "international_departing_share",
        "domestic_arriving_share",
        "international_arriving_share",
    ]:
        if share_col in df.columns:
            domestic_mix_summary[share_col] = {
                "avg": df[share_col].mean(),
                "min": df[share_col].min(),
                "max": df[share_col].max(),
                "corr_with_entries": df[share_col].corr(df["entries"]),
                "corr_with_exits": df[share_col].corr(df["exits"]),
            }

    # --------------------------------------------------------
    # Demand driver summary
    # --------------------------------------------------------
    summary_rows = []

    # Entry correlations using external drivers only
    external_entry_drivers = [
        "departing_pax",
        "arriving_pax",
        "total_pax",
        "departing_flights",
        "arriving_flights",
        "domestic_departing_pax",
        "international_departing_pax",
        "domestic_departing_share",
        "international_departing_share",
    ]

    external_entry_drivers = [
        col for col in external_entry_drivers
        if col in correlation_summary.index
    ]

    if "entries" in correlation_summary.columns and external_entry_drivers:

        entry_corr = (
            correlation_summary.loc[external_entry_drivers, "entries"]
            .dropna()
            .sort_values(key=lambda x: x.abs(), ascending=False)
        )

        if not entry_corr.empty:
            summary_rows.append(
                {
                    "section": "Correlation",
                    "metric": "Strongest external entry driver",
                    "value": entry_corr.index[0],
                    "supporting_value": entry_corr.iloc[0],
                    "interpretation": "Highest absolute correlation with FastPark entries using external passenger or flight drivers only.",
                }
            )

    # Exit correlations using external drivers only
    external_exit_drivers = [
        "departing_pax",
        "arriving_pax",
        "total_pax",
        "departing_flights",
        "arriving_flights",
        "domestic_arriving_pax",
        "international_arriving_pax",
        "domestic_arriving_share",
        "international_arriving_share",
    ]

    external_exit_drivers = [
        col for col in external_exit_drivers
        if col in correlation_summary.index
    ]

    if "exits" in correlation_summary.columns and external_exit_drivers:

        exit_corr = (
            correlation_summary.loc[external_exit_drivers, "exits"]
            .dropna()
            .sort_values(key=lambda x: x.abs(), ascending=False)
        )

        if not exit_corr.empty:
            summary_rows.append(
                {
                    "section": "Correlation",
                    "metric": "Strongest external exit driver",
                    "value": exit_corr.index[0],
                    "supporting_value": exit_corr.iloc[0],
                    "interpretation": "Highest absolute correlation with FastPark exits using external passenger or flight drivers only.",
                }
            )

    # --------------------------------------------------------
    # Price correlations with demand outcomes
    # --------------------------------------------------------
    price_metrics = [
        "avg_booking_total",
        "median_booking_total",
        "avg_product_price",
        "median_product_price",
        "avg_price_per_day",
        "median_price_per_day",
        "avg_price_per_quantity",
        "median_price_per_quantity",
        "avg_price_per_quantity_per_day",
        "median_price_per_quantity_per_day",
        "avg_product_price_per_day",
        "median_product_price_per_day",
    ]

    price_metrics = [
        col for col in price_metrics
        if col in correlation_summary.index
    ]

    price_targets = [
        ("valid_bookings", "bookings"),
        ("entries", "entries"),
        ("exits", "exits"),
        ("movements", "movements"),
        ("entry_penetration", "entry penetration"),
        ("exit_penetration", "exit penetration"),
    ]

    for target_col, target_label in price_targets:

        if target_col not in correlation_summary.columns:
            continue

        if not price_metrics:
            continue

        target_price_corr = (
            correlation_summary
            .loc[price_metrics, target_col]
            .dropna()
            .sort_values(
                key=lambda x: x.abs(),
                ascending=False,
            )
        )

        if target_price_corr.empty:
            continue

        summary_rows.append(
            {
                "section": "Pricing",
                "metric": f"Strongest price relationship with {target_label}",
                "value": target_price_corr.index[0],
                "supporting_value": target_price_corr.iloc[0],
                "interpretation": (
                    f"Price metric with the strongest absolute correlation "
                    f"to {target_label}."
                ),
            }
        )

    # Weekday effects
    if not weekday_summary.empty:
        max_entry_day = weekday_summary.sort_values(
            "avg_entries",
            ascending=False
        ).iloc[0]

        min_entry_day = weekday_summary.sort_values(
            "avg_entries",
            ascending=True
        ).iloc[0]

        max_exit_day = weekday_summary.sort_values(
            "avg_exits",
            ascending=False
        ).iloc[0]

        min_exit_day = weekday_summary.sort_values(
            "avg_exits",
            ascending=True
        ).iloc[0]

        max_movement_day = weekday_summary.sort_values(
            "avg_movements",
            ascending=False
        ).iloc[0]

        summary_rows.append(
            {
                "section": "Weekday",
                "metric": "Highest average entry weekday",
                "value": max_entry_day["weekday"],
                "supporting_value": max_entry_day["avg_entries"],
                "interpretation": "Weekday with the highest average daily entries.",
            }
        )

        summary_rows.append(
            {
                "section": "Weekday",
                "metric": "Lowest average entry weekday",
                "value": min_entry_day["weekday"],
                "supporting_value": min_entry_day["avg_entries"],
                "interpretation": "Weekday with the lowest average daily entries.",
            }
        )

        summary_rows.append(
            {
                "section": "Weekday",
                "metric": "Highest average exit weekday",
                "value": max_exit_day["weekday"],
                "supporting_value": max_exit_day["avg_exits"],
                "interpretation": "Weekday with the highest average daily exits.",
            }
        )

        summary_rows.append(
            {
                "section": "Weekday",
                "metric": "Lowest average exit weekday",
                "value": min_exit_day["weekday"],
                "supporting_value": min_exit_day["avg_exits"],
                "interpretation": "Weekday with the lowest average daily exits.",
            }
        )

        summary_rows.append(
            {
                "section": "Weekday",
                "metric": "Highest average movement weekday",
                "value": max_movement_day["weekday"],
                "supporting_value": max_movement_day["avg_movements"],
                "interpretation": "Weekday with the highest average combined entries and exits.",
            }
        )


    # Monthly effects
    if not monthly_summary.empty:
        max_entry_month = monthly_summary.sort_values(
            "avg_entries",
            ascending=False
        ).iloc[0]

        min_entry_month = monthly_summary.sort_values(
            "avg_entries",
            ascending=True
        ).iloc[0]

        max_exit_month = monthly_summary.sort_values(
            "avg_exits",
            ascending=False
        ).iloc[0]

        min_exit_month = monthly_summary.sort_values(
            "avg_exits",
            ascending=True
        ).iloc[0]

        max_movement_month = monthly_summary.sort_values(
            "avg_movements",
            ascending=False
        ).iloc[0]

        summary_rows.append(
            {
                "section": "Month",
                "metric": "Highest average entry month",
                "value": max_entry_month["month_name"],
                "supporting_value": max_entry_month["avg_entries"],
                "interpretation": "Month with the highest average daily entries.",
            }
        )

        summary_rows.append(
            {
                "section": "Month",
                "metric": "Lowest average entry month",
                "value": min_entry_month["month_name"],
                "supporting_value": min_entry_month["avg_entries"],
                "interpretation": "Month with the lowest average daily entries.",
            }
        )

        summary_rows.append(
            {
                "section": "Month",
                "metric": "Highest average exit month",
                "value": max_exit_month["month_name"],
                "supporting_value": max_exit_month["avg_exits"],
                "interpretation": "Month with the highest average daily exits.",
            }
        )

        summary_rows.append(
            {
                "section": "Month",
                "metric": "Lowest average exit month",
                "value": min_exit_month["month_name"],
                "supporting_value": min_exit_month["avg_exits"],
                "interpretation": "Month with the lowest average daily exits.",
            }
        )

        summary_rows.append(
            {
                "section": "Month",
                "metric": "Highest average movement month",
                "value": max_movement_month["month_name"],
                "supporting_value": max_movement_month["avg_movements"],
                "interpretation": "Month with the highest average combined entries and exits.",
            }
        )

    # Passenger volume band effect
    departing_bands = passenger_volume_band_summary[
        passenger_volume_band_summary["band_type"].eq("departing_pax_band")
    ].copy()

    if not departing_bands.empty:
        low_band = departing_bands.sort_values("avg_pax").head(1).iloc[0]
        high_band = departing_bands.sort_values("avg_pax").tail(1).iloc[0]

        summary_rows.append(
            {
                "section": "Passenger Bands",
                "metric": "Low pax band entry penetration",
                "value": str(low_band["passenger_volume_band"]),
                "supporting_value": low_band["avg_entry_penetration"],
                "interpretation": "Average entry penetration in the lowest departing passenger volume band.",
            }
        )

        summary_rows.append(
            {
                "section": "Passenger Bands",
                "metric": "High pax band entry penetration",
                "value": str(high_band["passenger_volume_band"]),
                "supporting_value": high_band["avg_entry_penetration"],
                "interpretation": "Average entry penetration in the highest departing passenger volume band.",
            }
        )

    arriving_bands = passenger_volume_band_summary[
        passenger_volume_band_summary["band_type"].eq("arriving_pax_band")
    ].copy()

    if not arriving_bands.empty:
        low_arrival_band = arriving_bands.sort_values("avg_pax").head(1).iloc[0]
        high_arrival_band = arriving_bands.sort_values("avg_pax").tail(1).iloc[0]

        summary_rows.append(
            {
                "section": "Passenger Bands",
                "metric": "Low pax band exit penetration",
                "value": str(low_arrival_band["passenger_volume_band"]),
                "supporting_value": low_arrival_band["avg_exit_penetration"],
                "interpretation": "Average exit penetration in the lowest arriving passenger volume band.",
            }
        )

        summary_rows.append(
            {
                "section": "Passenger Bands",
                "metric": "High pax band exit penetration",
                "value": str(high_arrival_band["passenger_volume_band"]),
                "supporting_value": high_arrival_band["avg_exit_penetration"],
                "interpretation": "Average exit penetration in the highest arriving passenger volume band.",
            }
        )

    # Passenger band forecast performance
    for _, row in passenger_band_performance_summary.iterrows():
        summary_rows.append(
            {
                "section": "Passenger Band Forecast",
                "metric": row["method"],
                "value": row["target"],
                "supporting_value": row["mae"],
                "interpretation": "Exploratory MAE using passenger-volume-band adjusted penetration.",
            }
        )

    # Domestic / international mix
    for share_col, values in domestic_mix_summary.items():

        if "departing" in share_col:
            summary_rows.append(
                {
                    "section": "Passenger Mix",
                    "metric": f"{share_col} corr with entries",
                    "value": share_col,
                    "supporting_value": values["corr_with_entries"],
                    "interpretation": "Correlation between departure passenger mix share and FastPark entries.",
                }
            )

        if "arriving" in share_col:
            summary_rows.append(
                {
                    "section": "Passenger Mix",
                    "metric": f"{share_col} corr with exits",
                    "value": share_col,
                    "supporting_value": values["corr_with_exits"],
                    "interpretation": "Correlation between arrival passenger mix share and FastPark exits.",
                }
            )

    demand_driver_summary = pd.DataFrame(summary_rows)

    results = {
        "demand_driver_summary": demand_driver_summary,
        "correlation_summary": correlation_summary,
        "price_correlation_summary": price_correlation_summary,
        "weekday_summary": weekday_summary,
        "monthly_summary": monthly_summary,
        "month_by_year_summary": month_by_year_summary,
        "passenger_volume_band_summary": passenger_volume_band_summary,
    }

    return results


# ============================================================
# 9. BOOKING CURVE ANALYSIS
# ============================================================

def create_booking_curve_dataset(bookings_clean, master, config):
    """
    Create booking curve dataset.

    Purpose:
        For each planned entry date, calculate how many bookings were already
        known at each lead time checkpoint.

    Example:
        For entry date 2026-08-20:
            - how many bookings existed 56 days before?
            - how many existed 28 days before?
            - how many existed 14 days before?
            - how many existed 7 days before?
            - how many final valid bookings existed?
            - how many actual operational entries occurred?

    Parameters
    ----------
    bookings_clean : pandas.DataFrame
        Cleaned booking data.

    master : pandas.DataFrame
        Master booking-operation table.

    config : dict
        Configuration dictionary.

    Returns
    -------
    pandas.DataFrame
        Booking curve table.
    """

    valid_bookings = bookings_clean[bookings_clean["is_valid_booking"]].copy()

    valid_bookings["planned_entry_date"] = pd.to_datetime(
        valid_bookings["planned_entry_date"]
    )

    valid_bookings["createdAt"] = pd.to_datetime(
        valid_bookings["createdAt"],
        errors="coerce",
    )

    final_valid_bookings_by_entry_date = (
        valid_bookings
        .groupby("planned_entry_date")
        .agg(final_valid_bookings=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"planned_entry_date": "entry_date"})
    )

    final_actual_entries_by_entry_date = (
        master
        .dropna(subset=["actual_entry_ts"])
        .copy()
    )

    final_actual_entries_by_entry_date["planned_entry_date"] = pd.to_datetime(
        final_actual_entries_by_entry_date["planned_entry_date"]
    )

    final_actual_entries_by_entry_date = (
        final_actual_entries_by_entry_date
        .groupby("planned_entry_date")
        .agg(final_actual_entries=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"planned_entry_date": "entry_date"})
    )

    unique_entry_dates = (
        valid_bookings["planned_entry_date"]
        .dropna()
        .sort_values()
        .unique()
    )

    rows = []

    for entry_date in unique_entry_dates:
        entry_date_ts = pd.Timestamp(entry_date)

        final_valid = final_valid_bookings_by_entry_date.loc[
            final_valid_bookings_by_entry_date["entry_date"].eq(entry_date_ts),
            "final_valid_bookings",
        ]

        final_actual = final_actual_entries_by_entry_date.loc[
            final_actual_entries_by_entry_date["entry_date"].eq(entry_date_ts),
            "final_actual_entries",
        ]

        final_valid_count = int(final_valid.iloc[0]) if len(final_valid) > 0 else 0
        final_actual_count = int(final_actual.iloc[0]) if len(final_actual) > 0 else 0

        for lead_time in config["lead_time_checkpoints"]:
            cutoff_ts = entry_date_ts - pd.Timedelta(days=lead_time)

            bookings_known = valid_bookings[
                (valid_bookings["planned_entry_date"].eq(entry_date_ts))
                & (valid_bookings["createdAt"] <= cutoff_ts)
            ]["bookingId"].nunique()

            rows.append(
                {
                    "entry_date": entry_date_ts,
                    "lead_time_checkpoint": lead_time,
                    "cutoff_timestamp": cutoff_ts,
                    "bookings_known": bookings_known,
                    "final_valid_bookings": final_valid_count,
                    "final_actual_entries": final_actual_count,
                    "completion_vs_valid_bookings": (
                        bookings_known / final_valid_count
                        if final_valid_count > 0
                        else np.nan
                    ),
                    "completion_vs_actual_entries": (
                        bookings_known / final_actual_count
                        if final_actual_count > 0
                        else np.nan
                    ),
                    "weekday": entry_date_ts.day_name(),
                    "weekday_num": entry_date_ts.weekday(),
                    "month": entry_date_ts.month,
                    "year": entry_date_ts.year,
                    "week": entry_date_ts.isocalendar().week,
                }
            )

    booking_curve = pd.DataFrame(rows)

    return booking_curve

def create_booking_entry_curve_by_duration_dataset(bookings_clean, master, config):
    """
    Create an entry booking curve split by planned duration band.

    Purpose:
        The standard entry booking curve shows how much final entry demand is
        visible at each lead time. This duration version tests whether bookings
        with different planned stay lengths become visible at different points
        before the entry date.

    Example:
        Short-stay bookings may be made later than long-stay bookings.
        If true, the uplift applied to known bookings should differ by planned
        duration band.
    """

    valid_bookings = bookings_clean[bookings_clean["is_valid_booking"]].copy()

    valid_bookings["planned_entry_date"] = pd.to_datetime(
        valid_bookings["planned_entry_date"]
    )

    valid_bookings["createdAt"] = pd.to_datetime(
        valid_bookings["createdAt"],
        errors="coerce",
    )

    valid_bookings["planned_duration_band"] = (
        valid_bookings["planned_duration_band"]
        .astype(str)
    )

    actual_entries = master.dropna(subset=["actual_entry_ts"]).copy()

    actual_entries["planned_entry_date"] = pd.to_datetime(
        actual_entries["planned_entry_date"]
    )

    actual_entries["planned_duration_band"] = (
        actual_entries["planned_duration_band"]
        .astype(str)
    )

    final_valid = (
        valid_bookings
        .groupby(["planned_entry_date", "planned_duration_band"])
        .agg(final_valid_bookings=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"planned_entry_date": "entry_date"})
    )

    final_actual = (
        actual_entries
        .groupby(["planned_entry_date", "planned_duration_band"])
        .agg(final_actual_entries=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"planned_entry_date": "entry_date"})
    )

    segments = (
        valid_bookings[
            ["planned_entry_date", "planned_duration_band"]
        ]
        .dropna()
        .drop_duplicates()
        .sort_values(["planned_entry_date", "planned_duration_band"])
    )

    rows = []

    for _, segment in segments.iterrows():

        entry_date = pd.Timestamp(segment["planned_entry_date"])
        duration_band = segment["planned_duration_band"]

        final_valid_count = final_valid.loc[
            final_valid["entry_date"].eq(entry_date)
            & final_valid["planned_duration_band"].eq(duration_band),
            "final_valid_bookings",
        ]

        final_actual_count = final_actual.loc[
            final_actual["entry_date"].eq(entry_date)
            & final_actual["planned_duration_band"].eq(duration_band),
            "final_actual_entries",
        ]

        final_valid_count = (
            int(final_valid_count.iloc[0])
            if len(final_valid_count) > 0
            else 0
        )

        final_actual_count = (
            int(final_actual_count.iloc[0])
            if len(final_actual_count) > 0
            else 0
        )

        for lead_time in config["lead_time_checkpoints"]:

            cutoff_ts = entry_date - pd.Timedelta(days=lead_time)

            bookings_known = valid_bookings[
                valid_bookings["planned_entry_date"].eq(entry_date)
                & valid_bookings["planned_duration_band"].eq(duration_band)
                & (valid_bookings["createdAt"] <= cutoff_ts)
            ]["bookingId"].nunique()

            rows.append(
                {
                    "entry_date": entry_date,
                    "planned_duration_band": duration_band,
                    "lead_time_checkpoint": lead_time,
                    "cutoff_timestamp": cutoff_ts,
                    "bookings_known": bookings_known,
                    "final_valid_bookings": final_valid_count,
                    "final_actual_entries": final_actual_count,
                    "completion_vs_valid_bookings": (
                        bookings_known / final_valid_count
                        if final_valid_count > 0
                        else np.nan
                    ),
                    "completion_vs_actual_entries": (
                        bookings_known / final_actual_count
                        if final_actual_count > 0
                        else np.nan
                    ),
                    "weekday": entry_date.day_name(),
                    "weekday_num": entry_date.weekday(),
                    "month": entry_date.month,
                    "year": entry_date.year,
                    "week": entry_date.isocalendar().week,
                }
            )

    return pd.DataFrame(rows)

def create_booking_exit_curve_dataset(bookings_clean, master, config):
    """
    Create an exit booking visibility curve.

    Purpose:
        The entry booking curve measures how much entry demand is visible before
        the planned entry date. This function creates the equivalent curve for
        planned exits.

        This is important for rostering because future exit demand may be known
        from bookings before the operational exit date occurs.

    Definition:
        For each planned exit date and lead-time checkpoint, count how many
        valid bookings were already created by the cutoff date.
    """

    valid_bookings = bookings_clean[bookings_clean["is_valid_booking"]].copy()

    valid_bookings["createdAt"] = pd.to_datetime(
        valid_bookings["createdAt"],
        errors="coerce",
    )

    valid_bookings["planned_exit_date"] = pd.to_datetime(
        valid_bookings["planned_exit_date"]
    )

    actual_exits = master.dropna(subset=["actual_exit_ts"]).copy()

    actual_exits["planned_exit_date"] = pd.to_datetime(
        actual_exits["planned_exit_date"]
    )

    final_valid_by_exit_date = (
        valid_bookings
        .groupby("planned_exit_date")
        .agg(final_valid_bookings=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"planned_exit_date": "exit_date"})
    )

    final_actual_by_exit_date = (
        actual_exits
        .groupby("planned_exit_date")
        .agg(final_actual_exits=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"planned_exit_date": "exit_date"})
    )

    unique_exit_dates = (
        valid_bookings["planned_exit_date"]
        .dropna()
        .sort_values()
        .unique()
    )

    rows = []

    for exit_date in unique_exit_dates:

        exit_date_ts = pd.Timestamp(exit_date)

        final_valid = final_valid_by_exit_date.loc[
            final_valid_by_exit_date["exit_date"].eq(exit_date_ts),
            "final_valid_bookings",
        ]

        final_actual = final_actual_by_exit_date.loc[
            final_actual_by_exit_date["exit_date"].eq(exit_date_ts),
            "final_actual_exits",
        ]

        final_valid_count = int(final_valid.iloc[0]) if len(final_valid) > 0 else 0
        final_actual_count = int(final_actual.iloc[0]) if len(final_actual) > 0 else 0

        for lead_time in config["lead_time_checkpoints"]:

            cutoff_ts = exit_date_ts - pd.Timedelta(days=lead_time)

            bookings_known = valid_bookings[
                valid_bookings["planned_exit_date"].eq(exit_date_ts)
                & (valid_bookings["createdAt"] <= cutoff_ts)
            ]["bookingId"].nunique()

            rows.append(
                {
                    "exit_date": exit_date_ts,
                    "lead_time_checkpoint": lead_time,
                    "cutoff_timestamp": cutoff_ts,
                    "bookings_known": bookings_known,
                    "final_valid_bookings": final_valid_count,
                    "final_actual_exits": final_actual_count,
                    "completion_vs_valid_bookings": (
                        bookings_known / final_valid_count
                        if final_valid_count > 0
                        else np.nan
                    ),
                    "completion_vs_actual_exits": (
                        bookings_known / final_actual_count
                        if final_actual_count > 0
                        else np.nan
                    ),
                    "weekday": exit_date_ts.day_name(),
                    "weekday_num": exit_date_ts.weekday(),
                    "month": exit_date_ts.month,
                    "year": exit_date_ts.year,
                    "week": exit_date_ts.isocalendar().week,
                }
            )

    return pd.DataFrame(rows)

def create_booking_exit_curve_by_duration_dataset(bookings_clean, master, config):
    """
    Create an exit booking visibility curve split by planned duration band.

    Purpose:
        This table measures how much future exit demand is visible at different
        lead times, separated by planned stay length.

        This is useful because short-duration bookings can create exit demand
        with limited forward visibility, while long-duration bookings may create
        exit demand that is visible much earlier.
    """

    valid_bookings = bookings_clean[bookings_clean["is_valid_booking"]].copy()

    valid_bookings["createdAt"] = pd.to_datetime(
        valid_bookings["createdAt"],
        errors="coerce",
    )

    valid_bookings["planned_exit_date"] = pd.to_datetime(
        valid_bookings["planned_exit_date"]
    )

    valid_bookings["planned_duration_band"] = (
        valid_bookings["planned_duration_band"]
        .astype(str)
    )

    actual_exits = master.dropna(subset=["actual_exit_ts"]).copy()

    actual_exits["planned_exit_date"] = pd.to_datetime(
        actual_exits["planned_exit_date"]
    )

    actual_exits["planned_duration_band"] = (
        actual_exits["planned_duration_band"]
        .astype(str)
    )

    final_valid = (
        valid_bookings
        .groupby(["planned_exit_date", "planned_duration_band"])
        .agg(final_valid_bookings=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"planned_exit_date": "exit_date"})
    )

    final_actual = (
        actual_exits
        .groupby(["planned_exit_date", "planned_duration_band"])
        .agg(final_actual_exits=("bookingId", "nunique"))
        .reset_index()
        .rename(columns={"planned_exit_date": "exit_date"})
    )

    segments = (
        valid_bookings[
            ["planned_exit_date", "planned_duration_band"]
        ]
        .dropna()
        .drop_duplicates()
        .sort_values(["planned_exit_date", "planned_duration_band"])
    )

    rows = []

    for _, segment in segments.iterrows():

        exit_date = pd.Timestamp(segment["planned_exit_date"])
        duration_band = segment["planned_duration_band"]

        final_valid_count = final_valid.loc[
            final_valid["exit_date"].eq(exit_date)
            & final_valid["planned_duration_band"].eq(duration_band),
            "final_valid_bookings",
        ]

        final_actual_count = final_actual.loc[
            final_actual["exit_date"].eq(exit_date)
            & final_actual["planned_duration_band"].eq(duration_band),
            "final_actual_exits",
        ]

        final_valid_count = (
            int(final_valid_count.iloc[0])
            if len(final_valid_count) > 0
            else 0
        )

        final_actual_count = (
            int(final_actual_count.iloc[0])
            if len(final_actual_count) > 0
            else 0
        )

        for lead_time in config["lead_time_checkpoints"]:

            cutoff_ts = exit_date - pd.Timedelta(days=lead_time)

            bookings_known = valid_bookings[
                valid_bookings["planned_exit_date"].eq(exit_date)
                & valid_bookings["planned_duration_band"].eq(duration_band)
                & (valid_bookings["createdAt"] <= cutoff_ts)
            ]["bookingId"].nunique()

            rows.append(
                {
                    "exit_date": exit_date,
                    "planned_duration_band": duration_band,
                    "lead_time_checkpoint": lead_time,
                    "cutoff_timestamp": cutoff_ts,
                    "bookings_known": bookings_known,
                    "final_valid_bookings": final_valid_count,
                    "final_actual_exits": final_actual_count,
                    "completion_vs_valid_bookings": (
                        bookings_known / final_valid_count
                        if final_valid_count > 0
                        else np.nan
                    ),
                    "completion_vs_actual_exits": (
                        bookings_known / final_actual_count
                        if final_actual_count > 0
                        else np.nan
                    ),
                    "weekday": exit_date.day_name(),
                    "weekday_num": exit_date.weekday(),
                    "month": exit_date.month,
                    "year": exit_date.year,
                    "week": exit_date.isocalendar().week,
                }
            )

    return pd.DataFrame(rows)

def summarise_booking_curve(
    curve_df,
    date_col,
    actual_col,
    group_cols=None,
):
    """
    Summarise a booking visibility curve.

    Purpose:
        Convert detailed entry or exit visibility data into a compact summary
        by lead time and optional grouping fields.

    Parameters
    ----------
    curve_df : pandas.DataFrame
        Detailed booking curve dataset.

    date_col : str
        Date column used to count observations.
        Examples:
            - entry_date
            - exit_date

    actual_col : str
        Final actual demand column.
        Examples:
            - final_actual_entries
            - final_actual_exits

    group_cols : list, optional
        Additional grouping columns.
        Examples:
            - ["weekday_num", "weekday"]
            - ["month"]
            - ["planned_duration_band"]

    Returns
    -------
    pandas.DataFrame
        Aggregated booking curve summary.
    """

    if curve_df is None or curve_df.empty:
        return pd.DataFrame()

    if group_cols is None:
        group_cols = []

    df = curve_df.copy()

    grouping = group_cols + ["lead_time_checkpoint"]

    completion_actual_col = (
        "completion_vs_actual_entries"
        if "completion_vs_actual_entries" in df.columns
        else "completion_vs_actual_exits"
    )

    summary = (
        df
        .groupby(grouping, dropna=False, observed=False)
        .agg(
            avg_bookings_known=("bookings_known", "mean"),
            avg_final_valid_bookings=("final_valid_bookings", "mean"),
            avg_final_actual=(actual_col, "mean"),
            avg_completion_vs_valid=("completion_vs_valid_bookings", "mean"),
            avg_completion_vs_actual=(completion_actual_col, "mean"),
            observations=(date_col, "nunique"),
        )
        .reset_index()
        .sort_values(grouping, ascending=[True] * len(grouping))
    )

    return summary

def analyse_booking_curve_segments(booking_curve):
    """
    Summarise the entry booking curve by lead time, weekday and month.

    Purpose:
        Entry booking curves show how much final entry demand is visible at
        each lead time before the planned entry date.
    """

    entry_curve_summary = summarise_booking_curve(
        curve_df=booking_curve,
        date_col="entry_date",
        actual_col="final_actual_entries",
        group_cols=[],
    )

    entry_curve_weekday = summarise_booking_curve(
        curve_df=booking_curve,
        date_col="entry_date",
        actual_col="final_actual_entries",
        group_cols=["weekday_num", "weekday"],
    )

    entry_curve_month = summarise_booking_curve(
        curve_df=booking_curve,
        date_col="entry_date",
        actual_col="final_actual_entries",
        group_cols=["month"],
    )

    return {
        "entry_curve_summary": entry_curve_summary,
        "entry_curve_weekday": entry_curve_weekday,
        "entry_curve_month": entry_curve_month,
    }

def analyse_booking_visibility_curves(
    booking_entry_curve,
    booking_entry_curve_duration,
    booking_exit_curve,
    booking_exit_curve_duration,
):
    """
    Summarise entry and exit booking visibility curves.

    Purpose:
        This creates the core booking visibility outputs used to understand:
            - how much entry demand is known before entry date
            - how much entry demand is known by planned duration
            - how much exit demand is known before exit date
            - how much exit demand is known by planned duration

        The exit curves are especially important for roster planning because
        short-duration bookings can create future exits with limited visibility.
    """

    entry_curve_summary = summarise_booking_curve(
        curve_df=booking_entry_curve,
        date_col="entry_date",
        actual_col="final_actual_entries",
        group_cols=[],
    )

    entry_curve_duration = summarise_booking_curve(
        curve_df=booking_entry_curve_duration,
        date_col="entry_date",
        actual_col="final_actual_entries",
        group_cols=["planned_duration_band"],
    )

    exit_curve_summary = summarise_booking_curve(
        curve_df=booking_exit_curve,
        date_col="exit_date",
        actual_col="final_actual_exits",
        group_cols=[],
    )

    exit_curve_duration = summarise_booking_curve(
        curve_df=booking_exit_curve_duration,
        date_col="exit_date",
        actual_col="final_actual_exits",
        group_cols=["planned_duration_band"],
    )

    return {
        "entry_curve_summary": entry_curve_summary,
        "entry_curve_duration": entry_curve_duration,
        "exit_curve_summary": exit_curve_summary,
        "exit_curve_duration": exit_curve_duration,
    }

def analyse_price_premium_booking_curve(
    bookings_clean,
    booking_entry_curve_duration,
    config,
):
    """
    Analyse whether price relative to normal explains booking-curve uplift.

    Purpose:
        This analysis tests whether days with higher or lower price-per-day
        than normal for the same horizon, month and planned duration band
        tend to finish above or below the normal booking-curve expectation.

    Important:
        This is an analysis section, not a final forecast model.

        The analysis uses the price paid by bookings created within each
        lead-time checkpoint window as a historical proxy for the price
        environment at that horizon.

        Example:
            For the 7-day horizon, the price window is bookings created
            between 7 and 10 days before planned entry, because 10 days is
            the next earlier checkpoint.

    Outputs:
        dict:
            - price_premium_detail
            - price_premium_band_summary
            - price_premium_horizon_summary
            - price_premium_duration_summary
    """

    if booking_entry_curve_duration is None or booking_entry_curve_duration.empty:
        return {
            "price_premium_detail": pd.DataFrame(),
            "price_premium_band_summary": pd.DataFrame(),
            "price_premium_horizon_summary": pd.DataFrame(),
            "price_premium_duration_summary": pd.DataFrame(),
        }

    # --------------------------------------------------------
    # Prepare booking curve data
    # --------------------------------------------------------
    curve = booking_entry_curve_duration.copy()

    curve["entry_date"] = pd.to_datetime(
        curve["entry_date"],
        errors="coerce",
    )

    curve["planned_duration_band"] = (
        curve["planned_duration_band"]
        .astype(str)
    )

    curve["month"] = curve["entry_date"].dt.month
    curve["year"] = curve["entry_date"].dt.year
    curve["weekday"] = curve["entry_date"].dt.day_name()

    # --------------------------------------------------------
    # Prepare valid booking price data
    # --------------------------------------------------------
    valid = bookings_clean[
        bookings_clean["is_valid_booking"]
    ].copy()

    valid["planned_entry_date"] = pd.to_datetime(
        valid["planned_entry_date"],
        errors="coerce",
    )

    valid["createdAt"] = pd.to_datetime(
        valid["createdAt"],
        errors="coerce",
    )

    valid["planned_duration_band"] = (
        valid["planned_duration_band"]
        .astype(str)
    )

    # Recalculate price fields defensively in case the function is reused.
    if "booking_total_per_quantity" not in valid.columns:
        valid["booking_total_per_quantity"] = (
            valid["bookingTotal"]
            / valid["productQuantity"].replace(0, np.nan)
        )

    if "booking_total_per_planned_day" not in valid.columns:
        valid["booking_total_per_planned_day"] = (
            valid["bookingTotal"]
            / valid["planned_duration_days_calc"].replace(0, np.nan)
        )

    if "booking_total_per_quantity_per_day" not in valid.columns:
        valid["booking_total_per_quantity_per_day"] = (
            valid["booking_total_per_quantity"]
            / valid["planned_duration_days_calc"].replace(0, np.nan)
        )

    if "product_price_per_planned_day" not in valid.columns:
        valid["product_price_per_planned_day"] = (
            valid["productPrice"]
            / valid["planned_duration_days_calc"].replace(0, np.nan)
        )

    # Lead time observed at booking creation.
    valid["lead_time_days_calc"] = (
        valid["entryDate"] - valid["createdAt"]
    ).dt.total_seconds() / 86400

    checkpoints = sorted(config["lead_time_checkpoints"])

    def assign_price_checkpoint(lead_time_days):
        """
        Assign each booking to the lead-time checkpoint window in which it
        was created.

        Example:
            If checkpoints include 7 and 10:
                bookings created 7 to less than 10 days before entry
                are assigned to checkpoint 7.
        """

        if pd.isna(lead_time_days):
            return np.nan

        if lead_time_days <= checkpoints[0]:
            return checkpoints[0]

        for i, checkpoint in enumerate(checkpoints):

            if i == len(checkpoints) - 1:
                if lead_time_days >= checkpoint:
                    return checkpoint

            next_checkpoint = checkpoints[i + 1]

            if (
                lead_time_days >= checkpoint
                and lead_time_days < next_checkpoint
            ):
                return checkpoint

        return np.nan

    valid["lead_time_checkpoint"] = valid["lead_time_days_calc"].apply(
        assign_price_checkpoint
    )

    valid = valid.dropna(
        subset=[
            "planned_entry_date",
            "planned_duration_band",
            "lead_time_checkpoint",
        ]
    ).copy()

    valid["lead_time_checkpoint"] = (
        valid["lead_time_checkpoint"]
        .astype(int)
    )

    # --------------------------------------------------------
    # Price observed in each booking horizon window
    # --------------------------------------------------------
    price_window_summary = (
        valid
        .groupby(
            [
                "planned_entry_date",
                "planned_duration_band",
                "lead_time_checkpoint",
            ],
            dropna=False,
            observed=False,
        )
        .agg(
            bookings_created_in_price_window=("bookingId", "nunique"),

            avg_booking_total_at_horizon=("bookingTotal", "mean"),
            median_booking_total_at_horizon=("bookingTotal", "median"),

            avg_price_per_day_at_horizon=(
                "booking_total_per_planned_day",
                "mean",
            ),
            median_price_per_day_at_horizon=(
                "booking_total_per_planned_day",
                "median",
            ),

            avg_price_per_quantity_per_day_at_horizon=(
                "booking_total_per_quantity_per_day",
                "mean",
            ),
            median_price_per_quantity_per_day_at_horizon=(
                "booking_total_per_quantity_per_day",
                "median",
            ),

            avg_product_price_per_day_at_horizon=(
                "product_price_per_planned_day",
                "mean",
            ),
            median_product_price_per_day_at_horizon=(
                "product_price_per_planned_day",
                "median",
            ),

            avg_lead_time_days_in_price_window=(
                "lead_time_days_calc",
                "mean",
            ),
            min_lead_time_days_in_price_window=(
                "lead_time_days_calc",
                "min",
            ),
            max_lead_time_days_in_price_window=(
                "lead_time_days_calc",
                "max",
            ),
        )
        .reset_index()
        .rename(columns={"planned_entry_date": "entry_date"})
    )

    price_window_summary["entry_date"] = pd.to_datetime(
        price_window_summary["entry_date"],
        errors="coerce",
    )

    # --------------------------------------------------------
    # Merge booking curve and price window data
    # --------------------------------------------------------
    detail = curve.merge(
        price_window_summary,
        on=[
            "entry_date",
            "planned_duration_band",
            "lead_time_checkpoint",
        ],
        how="left",
    )

    # --------------------------------------------------------
    # Historical normal price and completion baselines
    # Same horizon, same month, same duration band.
    # --------------------------------------------------------
    baseline = (
        detail
        .groupby(
            [
                "lead_time_checkpoint",
                "month",
                "planned_duration_band",
            ],
            dropna=False,
            observed=False,
        )
        .agg(
            normal_median_price_per_day=(
                "median_price_per_day_at_horizon",
                "median",
            ),
            normal_avg_price_per_day=(
                "avg_price_per_day_at_horizon",
                "mean",
            ),
            normal_median_price_per_quantity_per_day=(
                "median_price_per_quantity_per_day_at_horizon",
                "median",
            ),
            normal_avg_price_per_quantity_per_day=(
                "avg_price_per_quantity_per_day_at_horizon",
                "mean",
            ),
            normal_completion_vs_actual=(
                "completion_vs_actual_entries",
                "mean",
            ),
            normal_completion_vs_valid=(
                "completion_vs_valid_bookings",
                "mean",
            ),
            normal_final_actual_entries=(
                "final_actual_entries",
                "mean",
            ),
            normal_final_valid_bookings=(
                "final_valid_bookings",
                "mean",
            ),
            baseline_observations=("entry_date", "nunique"),
        )
        .reset_index()
    )

    detail = detail.merge(
        baseline,
        on=[
            "lead_time_checkpoint",
            "month",
            "planned_duration_band",
        ],
        how="left",
    )

    # --------------------------------------------------------
    # Price premium calculations
    # --------------------------------------------------------
    detail["price_premium_vs_normal_median_price_per_day"] = (
        detail["median_price_per_day_at_horizon"]
        / detail["normal_median_price_per_day"].replace(0, np.nan)
        - 1
    )

    detail["price_premium_vs_normal_avg_price_per_day"] = (
        detail["avg_price_per_day_at_horizon"]
        / detail["normal_avg_price_per_day"].replace(0, np.nan)
        - 1
    )

    detail["price_premium_vs_normal_median_price_per_quantity_per_day"] = (
        detail["median_price_per_quantity_per_day_at_horizon"]
        / detail["normal_median_price_per_quantity_per_day"].replace(0, np.nan)
        - 1
    )

    detail["price_premium_vs_normal_avg_price_per_quantity_per_day"] = (
        detail["avg_price_per_quantity_per_day_at_horizon"]
        / detail["normal_avg_price_per_quantity_per_day"].replace(0, np.nan)
        - 1
    )

    # Main premium used for summaries.
    detail["price_premium"] = (
        detail["price_premium_vs_normal_median_price_per_quantity_per_day"]
    )

    # --------------------------------------------------------
    # Booking curve expectation and uplift
    # --------------------------------------------------------
    detail["expected_final_actual_from_curve"] = (
        detail["bookings_known"]
        / detail["normal_completion_vs_actual"].replace(0, np.nan)
    )

    detail["expected_final_valid_from_curve"] = (
        detail["bookings_known"]
        / detail["normal_completion_vs_valid"].replace(0, np.nan)
    )

    detail["actual_uplift_vs_curve_expected"] = (
        detail["final_actual_entries"]
        / detail["expected_final_actual_from_curve"].replace(0, np.nan)
        - 1
    )

    detail["valid_booking_uplift_vs_curve_expected"] = (
        detail["final_valid_bookings"]
        / detail["expected_final_valid_from_curve"].replace(0, np.nan)
        - 1
    )

    detail["actual_uplift_vs_normal_final_actual"] = (
        detail["final_actual_entries"]
        / detail["normal_final_actual_entries"].replace(0, np.nan)
        - 1
    )

    detail["valid_booking_uplift_vs_normal_final_valid"] = (
        detail["final_valid_bookings"]
        / detail["normal_final_valid_bookings"].replace(0, np.nan)
        - 1
    )

    detail["curve_error_entries"] = (
        detail["expected_final_actual_from_curve"]
        - detail["final_actual_entries"]
    )

    detail["curve_absolute_error_entries"] = (
        detail["curve_error_entries"].abs()
    )

    detail["curve_percentage_error_entries"] = (
        detail["curve_error_entries"]
        / detail["final_actual_entries"].replace(0, np.nan)
    )

    detail["curve_absolute_percentage_error_entries"] = (
        detail["curve_percentage_error_entries"].abs()
    )

    # --------------------------------------------------------
    # Price premium bands
    # --------------------------------------------------------
    premium_bins = [
        -np.inf,
        -0.20,
        -0.10,
        -0.05,
        0.05,
        0.10,
        0.20,
        np.inf,
    ]

    premium_labels = [
        "< -20%",
        "-20% to -10%",
        "-10% to -5%",
        "-5% to +5%",
        "+5% to +10%",
        "+10% to +20%",
        "> +20%",
    ]

    detail["price_premium_band"] = pd.cut(
        detail["price_premium"],
        bins=premium_bins,
        labels=premium_labels,
    )

    # --------------------------------------------------------
    # Summary by horizon and premium band
    # --------------------------------------------------------
    price_premium_band_summary = (
        detail
        .groupby(
            [
                "lead_time_checkpoint",
                "price_premium_band",
            ],
            dropna=False,
            observed=False,
        )
        .agg(
            observations=("entry_date", "nunique"),
            avg_price_premium=("price_premium", "mean"),
            median_price_premium=("price_premium", "median"),

            avg_bookings_known=("bookings_known", "mean"),
            avg_final_actual_entries=("final_actual_entries", "mean"),
            avg_expected_final_actual_from_curve=(
                "expected_final_actual_from_curve",
                "mean",
            ),

            avg_actual_uplift_vs_curve_expected=(
                "actual_uplift_vs_curve_expected",
                "mean",
            ),
            median_actual_uplift_vs_curve_expected=(
                "actual_uplift_vs_curve_expected",
                "median",
            ),

            avg_valid_booking_uplift_vs_curve_expected=(
                "valid_booking_uplift_vs_curve_expected",
                "mean",
            ),
            median_valid_booking_uplift_vs_curve_expected=(
                "valid_booking_uplift_vs_curve_expected",
                "median",
            ),

            curve_mae=("curve_absolute_error_entries", "mean"),
            curve_mape=("curve_absolute_percentage_error_entries", "mean"),

            avg_median_price_per_day_at_horizon=(
                "median_price_per_day_at_horizon",
                "mean",
            ),
            avg_normal_median_price_per_day=(
                "normal_median_price_per_day",
                "mean",
            ),
        )
        .reset_index()
        .sort_values(
            [
                "lead_time_checkpoint",
                "price_premium_band",
            ]
        )
    )

    # --------------------------------------------------------
    # Summary by horizon
    # --------------------------------------------------------
    horizon_rows = []

    for horizon, horizon_df in detail.groupby("lead_time_checkpoint"):

        valid_corr = horizon_df[
            [
                "price_premium",
                "actual_uplift_vs_curve_expected",
                "valid_booking_uplift_vs_curve_expected",
            ]
        ].replace([np.inf, -np.inf], np.nan).dropna()

        if len(valid_corr) > 1:
            corr_actual = valid_corr["price_premium"].corr(
                valid_corr["actual_uplift_vs_curve_expected"]
            )

            corr_valid = valid_corr["price_premium"].corr(
                valid_corr["valid_booking_uplift_vs_curve_expected"]
            )
        else:
            corr_actual = np.nan
            corr_valid = np.nan

        horizon_rows.append(
            {
                "lead_time_checkpoint": horizon,
                "observations": horizon_df["entry_date"].nunique(),
                "avg_price_premium": horizon_df["price_premium"].mean(),
                "median_price_premium": horizon_df["price_premium"].median(),
                "corr_price_premium_with_actual_uplift": corr_actual,
                "corr_price_premium_with_valid_booking_uplift": corr_valid,
                "avg_actual_uplift_vs_curve_expected": (
                    horizon_df["actual_uplift_vs_curve_expected"].mean()
                ),
                "median_actual_uplift_vs_curve_expected": (
                    horizon_df["actual_uplift_vs_curve_expected"].median()
                ),
                "curve_mae": horizon_df["curve_absolute_error_entries"].mean(),
                "curve_mape": (
                    horizon_df["curve_absolute_percentage_error_entries"].mean()
                ),
            }
        )

    price_premium_horizon_summary = pd.DataFrame(horizon_rows)

    # --------------------------------------------------------
    # Summary by horizon and duration band
    # --------------------------------------------------------
    price_premium_duration_summary = (
        detail
        .groupby(
            [
                "lead_time_checkpoint",
                "planned_duration_band",
            ],
            dropna=False,
            observed=False,
        )
        .agg(
            observations=("entry_date", "nunique"),
            avg_price_premium=("price_premium", "mean"),
            median_price_premium=("price_premium", "median"),
            avg_actual_uplift_vs_curve_expected=(
                "actual_uplift_vs_curve_expected",
                "mean",
            ),
            median_actual_uplift_vs_curve_expected=(
                "actual_uplift_vs_curve_expected",
                "median",
            ),
            avg_valid_booking_uplift_vs_curve_expected=(
                "valid_booking_uplift_vs_curve_expected",
                "mean",
            ),
            curve_mae=("curve_absolute_error_entries", "mean"),
            curve_mape=("curve_absolute_percentage_error_entries", "mean"),
            avg_median_price_per_day_at_horizon=(
                "median_price_per_day_at_horizon",
                "mean",
            ),
            avg_normal_median_price_per_day=(
                "normal_median_price_per_day",
                "mean",
            ),
        )
        .reset_index()
        .sort_values(
            [
                "lead_time_checkpoint",
                "planned_duration_band",
            ]
        )
    )

    return {
        "price_premium_detail": detail,
        "price_premium_band_summary": price_premium_band_summary,
        "price_premium_horizon_summary": price_premium_horizon_summary,
        "price_premium_duration_summary": price_premium_duration_summary,
    }

# ============================================================
# 10. STAY DURATION, RETURN BEHAVIOUR AND EXIT FORECAST BACKTESTING
# ============================================================

def create_duration_analysis_dataset(master, config):
    """
    Create dataset for planned duration, actual duration and return deviation.

    Purpose:
        This is the foundation for testing whether FastPark exits are better
        explained by:
            - advised / expected return dates
            - actual stay durations
            - entry cohorts
            - historical arriving passenger penetration

    Parameters
    ----------
    master : pandas.DataFrame
        Master booking-operation table.

    config : dict
        Configuration dictionary.

    Returns
    -------
    pandas.DataFrame
        Duration analysis dataset with:
            - planned duration
            - actual duration
            - deviation versus booking exit date
            - deviation versus expected return date
            - early / late return flags
            - duration bands
    """

    df = master.copy()

    # Ensure datetime columns are datetime.
    datetime_cols = [
        "entryDate",
        "exitDate",
        "ExpectedReturnDate",
        "actual_entry_ts",
        "actual_exit_ts",
    ]

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Planned duration from booking.
    df["planned_duration_days"] = (
        df["exitDate"] - df["entryDate"]
    ).dt.total_seconds() / 86400

    # Actual car-on-site duration from operational actuals.
    df["actual_duration_days"] = (
        df["actual_exit_ts"] - df["actual_entry_ts"]
    ).dt.total_seconds() / 86400

    # Deviation versus booking exit date.
    df["exit_deviation_vs_booking_minutes"] = (
        df["actual_exit_ts"] - df["exitDate"]
    ).dt.total_seconds() / 60

    # Deviation versus operational expected return date.
    df["exit_deviation_vs_expected_return_minutes"] = (
        df["actual_exit_ts"] - df["ExpectedReturnDate"]
    ).dt.total_seconds() / 60

    # Thresholds for operationally meaningful deviations.
    threshold_mins = config["early_late_threshold_hours"] * 60
    major_threshold_mins = config["major_deviation_threshold_hours"] * 60

    df["returned_early_flag"] = (
        df["exit_deviation_vs_expected_return_minutes"] < -threshold_mins
    )

    df["returned_late_flag"] = (
        df["exit_deviation_vs_expected_return_minutes"] > threshold_mins
    )

    df["major_return_deviation_flag"] = (
        df["exit_deviation_vs_expected_return_minutes"].abs() > major_threshold_mins
    )

    # Different date return flag.
    df["returned_on_different_date_flag"] = (
        df["actual_exit_ts"].dt.date != df["ExpectedReturnDate"].dt.date
    )

    # Actual duration band.
    df["actual_duration_band"] = pd.cut(
        df["actual_duration_days"],
        bins=config["duration_bins_days"],
        labels=config["duration_labels"],
        right=True,
        include_lowest=True,
    )

    # Planned duration band.
    df["planned_duration_band_recalc"] = pd.cut(
        df["planned_duration_days"],
        bins=config["duration_bins_days"],
        labels=config["duration_labels"],
        right=True,
        include_lowest=True,
    )

    return df


def analyse_duration_patterns(duration_df):
    """
    Analyse planned stay-duration patterns.

    Purpose:
        Planned booking duration is available for future bookings, so it is
        the most useful duration field for forecasting future occupancy and
        exit demand.

        Actual duration is used only as a validation check to confirm whether
        planned duration is a reliable forecasting input.

    Returns
    -------
    dict
        Duration summary tables:
            - planned_duration_distribution
            - planned_duration_by_weekday
            - planned_duration_by_month
            - planned_duration_by_airline
            - planned_vs_actual_summary
    """

    df = duration_df.copy()

    valid_planned = df.dropna(subset=["planned_duration_days"]).copy()

    # --------------------------------------------------------
    # Planned duration distribution
    # --------------------------------------------------------
    # This shows the share of bookings by planned stay-length band.
    # This is the main duration table for forecasting because planned
    # duration is known for future bookings.
    planned_duration_distribution = (
        valid_planned
        .groupby("planned_duration_band_recalc", observed=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_planned_duration_days=("planned_duration_days", "mean"),
            median_planned_duration_days=("planned_duration_days", "median"),
        )
        .reset_index()
        .rename(columns={"planned_duration_band_recalc": "planned_duration_band"})
    )

    planned_duration_distribution["share"] = (
        planned_duration_distribution["bookings"]
        / planned_duration_distribution["bookings"].sum()
    )

    # --------------------------------------------------------
    # Planned duration by entry weekday
    # --------------------------------------------------------
    # This is kept as a supporting view. It can be commented out later if
    # weekday duration differences are not material enough for the forecast.
    planned_duration_by_weekday = (
        valid_planned
        .groupby("entry_weekday", dropna=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_planned_duration_days=("planned_duration_days", "mean"),
            median_planned_duration_days=("planned_duration_days", "median"),
        )
        .reset_index()
    )

    # --------------------------------------------------------
    # Planned duration by entry month
    # --------------------------------------------------------
    # This shows seasonal differences in planned stay length.
    # It is useful for occupancy and future exit forecasting.
    planned_duration_by_month = (
        valid_planned
        .groupby("entry_month", dropna=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_planned_duration_days=("planned_duration_days", "mean"),
            median_planned_duration_days=("planned_duration_days", "median"),
        )
        .reset_index()
    )

    # --------------------------------------------------------
    # Planned duration by outbound airline
    # --------------------------------------------------------
    # Airline mix can affect duration because different carriers and markets
    # may represent different trip types. This is kept as an exploratory
    # forecasting feature.
    planned_duration_by_airline = (
        valid_planned
        .groupby("outboundAirline", dropna=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_planned_duration_days=("planned_duration_days", "mean"),
            median_planned_duration_days=("planned_duration_days", "median"),
        )
        .reset_index()
        .sort_values("bookings", ascending=False)
    )

    # --------------------------------------------------------
    # Planned vs actual validation
    # --------------------------------------------------------
    # This validates whether planned duration is a reliable proxy for actual
    # duration. It should not be treated as a future forecasting input because
    # actual duration is not known in advance.
    valid_actual = df.dropna(
        subset=["planned_duration_days", "actual_duration_days"]
    ).copy()

    planned_vs_actual_summary = pd.DataFrame(
        [
            {
                "records": valid_actual["bookingId"].nunique(),
                "avg_planned_duration_days": valid_actual["planned_duration_days"].mean(),
                "avg_actual_duration_days": valid_actual["actual_duration_days"].mean(),
                "avg_actual_minus_planned_days": (
                    valid_actual["actual_duration_days"]
                    - valid_actual["planned_duration_days"]
                ).mean(),
                "median_actual_minus_planned_days": (
                    valid_actual["actual_duration_days"]
                    - valid_actual["planned_duration_days"]
                ).median(),
            }
        ]
    )

    return {
        "planned_duration_distribution": planned_duration_distribution,
        "planned_duration_by_weekday": planned_duration_by_weekday,
        "planned_duration_by_month": planned_duration_by_month,
        "planned_duration_by_airline": planned_duration_by_airline,
        "planned_vs_actual_summary": planned_vs_actual_summary,
    }


def analyse_return_deviation(duration_df):
    """
    Analyse how planned / expected returns differ from actual returns.

    Purpose:
        Understand whether customers generally return early, late, or on time,
        and whether expected return datetime needs adjustment before being used
        as an exit forecast.

    Parameters
    ----------
    duration_df : pandas.DataFrame
        Duration analysis dataset.

    Returns
    -------
    dict
        Return deviation summaries.
    """

    df = duration_df.copy()

    valid = df.dropna(
        subset=["ExpectedReturnDate", "actual_exit_ts"]
    ).copy()

    overall_deviation_summary = (
        valid["exit_deviation_vs_expected_return_minutes"]
        .describe()
        .reset_index()
        .rename(columns={"index": "metric", "exit_deviation_vs_expected_return_minutes": "value"})
    )

    deviation_by_expected_return_hour = (
        valid
        .groupby("expected_return_hour", dropna=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_deviation_minutes=("exit_deviation_vs_expected_return_minutes", "mean"),
            median_deviation_minutes=("exit_deviation_vs_expected_return_minutes", "median"),
            early_returns=("returned_early_flag", "sum"),
            late_returns=("returned_late_flag", "sum"),
            major_deviations=("major_return_deviation_flag", "sum"),
            different_day_returns=("returned_on_different_date_flag", "sum"),
        )
        .reset_index()
    )

    deviation_by_weekday = (
        valid
        .groupby("exit_weekday", dropna=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_deviation_minutes=("exit_deviation_vs_expected_return_minutes", "mean"),
            median_deviation_minutes=("exit_deviation_vs_expected_return_minutes", "median"),
            early_returns=("returned_early_flag", "sum"),
            late_returns=("returned_late_flag", "sum"),
            different_day_returns=("returned_on_different_date_flag", "sum"),
        )
        .reset_index()
    )

    deviation_by_month = (
        valid
        .groupby("exit_month", dropna=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_deviation_minutes=("exit_deviation_vs_expected_return_minutes", "mean"),
            median_deviation_minutes=("exit_deviation_vs_expected_return_minutes", "median"),
            early_returns=("returned_early_flag", "sum"),
            late_returns=("returned_late_flag", "sum"),
            different_day_returns=("returned_on_different_date_flag", "sum"),
        )
        .reset_index()
    )

    deviation_by_duration_band = (
        valid
        .groupby("planned_duration_band_recalc", dropna=False, observed=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_deviation_minutes=("exit_deviation_vs_expected_return_minutes", "mean"),
            median_deviation_minutes=("exit_deviation_vs_expected_return_minutes", "median"),
            early_returns=("returned_early_flag", "sum"),
            late_returns=("returned_late_flag", "sum"),
            major_deviations=("major_return_deviation_flag", "sum"),
            different_day_returns=("returned_on_different_date_flag", "sum"),
        )
        .reset_index()
        .rename(columns={"planned_duration_band_recalc": "planned_duration_band"})
    )

    early_return_summary = (
        valid
        .groupby("returned_early_flag", dropna=False)
        .agg(bookings=("bookingId", "nunique"))
        .reset_index()
    )

    early_return_summary["share"] = (
        early_return_summary["bookings"]
        / early_return_summary["bookings"].sum()
    )

    late_return_summary = (
        valid
        .groupby("returned_late_flag", dropna=False)
        .agg(bookings=("bookingId", "nunique"))
        .reset_index()
    )

    late_return_summary["share"] = (
        late_return_summary["bookings"]
        / late_return_summary["bookings"].sum()
    )

    different_day_return_summary = (
        valid
        .groupby("returned_on_different_date_flag", dropna=False)
        .agg(bookings=("bookingId", "nunique"))
        .reset_index()
    )

    different_day_return_summary["share"] = (
        different_day_return_summary["bookings"]
        / different_day_return_summary["bookings"].sum()
    )

    return {
        "overall_deviation_summary": overall_deviation_summary,
        "deviation_by_expected_return_hour": deviation_by_expected_return_hour,
        "deviation_by_month": deviation_by_month,
        "deviation_by_duration_band": deviation_by_duration_band,
        "early_return_summary": early_return_summary,
        "late_return_summary": late_return_summary,
        "different_day_return_summary": different_day_return_summary,

        #Supporting analysis. uncomment if required for future modelling
        #"deviation_by_weekday": deviation_by_weekday,
    }


def create_known_booked_exit_profile(master):
    """
    Create known booked exit profile using expected / advised return datetime.

    Logic:
        Known booked exits for hour t are valid bookings whose ExpectedReturnDate
        falls in hour t. If ExpectedReturnDate is missing, fall back to booking exitDate.

    Parameters
    ----------
    master : pandas.DataFrame
        Master booking-operation table.

    Returns
    -------
    pandas.DataFrame
        Known booked exit profile by expected return hour.
    """

    df = master.copy()
    valid = df[df["is_valid_booking"]].copy()

    valid["ExpectedReturnDate"] = pd.to_datetime(
        valid["ExpectedReturnDate"],
        errors="coerce",
    )

    valid["exitDate"] = pd.to_datetime(
        valid["exitDate"],
        errors="coerce",
    )

    valid["expected_return_ts_final"] = valid["ExpectedReturnDate"].fillna(
        valid["exitDate"]
    )

    known_exits = (
        valid
        .dropna(subset=["expected_return_ts_final"])
        .assign(expected_return_hour_ts=lambda x: x["expected_return_ts_final"].dt.floor("h"))
        .groupby("expected_return_hour_ts")
        .agg(known_booked_exits=("bookingId", "nunique"))
        .reset_index()
    )

    return known_exits


# ============================================================
# 11. TENDENCY / ROLLING PENETRATION ANALYSIS
# ============================================================

def calculate_rolling_penetration(
    daily_driver_dataset,
    window_weeks,
    target_col,
    pax_col,
    target_name,
):
    """
    Calculate rolling penetration over a selected number of weeks.

    Purpose:
        Create historical tendency measures for either entries or exits.

    Examples:
        Entry tendency:
            entries / departing_pax

        Exit tendency:
            exits / arriving_pax

    Important:
        Uses shifted rolling sums so the current day is not included in
        its own tendency estimate.
    """

    df = daily_driver_dataset.sort_values("date").copy()
    window_days = window_weeks * 7

    target_sum_col = f"rolling_{window_weeks}wk_{target_col}"
    pax_sum_col = f"rolling_{window_weeks}wk_{pax_col}"
    pen_col = f"rolling_{window_weeks}wk_{target_name}_penetration"

    df[target_sum_col] = (
        df[target_col]
        .shift(1)
        .rolling(window_days)
        .sum()
    )

    df[pax_sum_col] = (
        df[pax_col]
        .shift(1)
        .rolling(window_days)
        .sum()
    )

    df[pen_col] = (
        df[target_sum_col]
        / df[pax_sum_col].replace(0, np.nan)
    )

    return df


def calculate_same_weekday_penetration(
    daily_driver_dataset,
    n_occurrences,
    target_col,
    pax_col,
    target_name,
):
    """
    Calculate same-weekday ratio-of-sums penetration.

    Example:
        To forecast a Friday, use the previous n Fridays:

            sum(previous n Friday target_col)
            /
            sum(previous n Friday pax_col)

    This avoids mixing weekday behaviour.
    """

    df = daily_driver_dataset.sort_values("date").copy()

    pen_col = f"same_weekday_last_{n_occurrences}_{target_name}_penetration"
    target_sum_col = f"same_weekday_last_{n_occurrences}_{target_col}"
    pax_sum_col = f"same_weekday_last_{n_occurrences}_{pax_col}"

    df[pen_col] = np.nan
    df[target_sum_col] = np.nan
    df[pax_sum_col] = np.nan

    for weekday in df["weekday"].dropna().unique():

        mask = df["weekday"].eq(weekday)
        sub = df.loc[mask].copy().sort_values("date")

        rolling_target = (
            sub[target_col]
            .shift(1)
            .rolling(n_occurrences)
            .sum()
        )

        rolling_pax = (
            sub[pax_col]
            .shift(1)
            .rolling(n_occurrences)
            .sum()
        )

        df.loc[mask, target_sum_col] = rolling_target
        df.loc[mask, pax_sum_col] = rolling_pax

        df.loc[mask, pen_col] = (
            rolling_target
            / rolling_pax.replace(0, np.nan)
        )

    return df


def calculate_same_weekday_average_penetration(
    daily_driver_dataset,
    n_occurrences,
    penetration_col,
    target_name,
):
    """
    Calculate same-weekday rolling average of daily penetration.

    Example:
        To estimate a Friday entry penetration, use the average of the
        previous n Friday entry penetration values.

    This differs from ratio-of-sums because each historical day receives
    equal weight regardless of passenger volume.
    """

    df = daily_driver_dataset.sort_values("date").copy()

    pen_col = f"same_weekday_avg_last_{n_occurrences}_{target_name}_penetration"

    df[pen_col] = np.nan

    for weekday in df["weekday"].dropna().unique():

        mask = df["weekday"].eq(weekday)
        sub = df.loc[mask].copy().sort_values("date")

        rolling_avg_pen = (
            sub[penetration_col]
            .shift(1)
            .rolling(n_occurrences)
            .mean()
        )

        df.loc[mask, pen_col] = rolling_avg_pen

    return df


def calculate_weighted_same_weekday_penetration(
    daily_driver_dataset,
    n_occurrences,
    penetration_col,
    target_name,
):
    """
    Calculate weighted same-weekday rolling penetration.

    Purpose:
        Test whether recent same weekdays should matter more than older
        same weekdays.

    Example for n=4:
        oldest same weekday      = weight 1
        second oldest            = weight 2
        second most recent       = weight 3
        most recent              = weight 4
    """

    df = daily_driver_dataset.sort_values("date").copy()

    pen_col = f"same_weekday_weighted_last_{n_occurrences}_{target_name}_penetration"

    df[pen_col] = np.nan

    weights = np.arange(1, n_occurrences + 1)
    weights = weights / weights.sum()

    def weighted_average(values):
        if np.isnan(values).any():
            return np.nan
        return np.dot(values, weights)

    for weekday in df["weekday"].dropna().unique():

        mask = df["weekday"].eq(weekday)
        sub = df.loc[mask].copy().sort_values("date")

        weighted_pen = (
            sub[penetration_col]
            .shift(1)
            .rolling(n_occurrences)
            .apply(weighted_average, raw=True)
        )

        df.loc[mask, pen_col] = weighted_pen

    return df


def run_tendency_window_backtest(daily_driver_dataset, config):
    """
    Back-test entry and exit penetration tendency methods.

    Entry tendency:
        forecast_entries = departing_pax * historical_entry_penetration

    Exit tendency:
        forecast_exits = arriving_pax * historical_exit_penetration

    Methods tested:
        1. Optional general rolling ratio-of-sums penetration
        2. Same-weekday ratio-of-sums penetration
        3. Same-weekday average daily penetration
        4. Weighted same-weekday daily penetration
    """

    results = []

    base = daily_driver_dataset.copy()
    base["date"] = pd.to_datetime(base["date"])

    target_specs = [
        {
            "target_name": "entry",
            "target_col": "entries",
            "pax_col": "departing_pax",
            "penetration_col": "entry_penetration",
            "run_general_rolling": config.get(
                "run_general_rolling_entry_tendency",
                True,
            ),
        },
        {
            "target_name": "exit",
            "target_col": "exits",
            "pax_col": "arriving_pax",
            "penetration_col": "exit_penetration",
            "run_general_rolling": config.get(
                "run_general_rolling_exit_tendency",
                True,
            ),
        },
    ]

    def append_forecast_results(
        temp,
        pen_col,
        method_name,
        target_name,
        target_col,
        pax_col,
    ):
        temp = temp.copy()

        temp["target"] = target_name
        temp["method"] = method_name

        temp["forecast_value"] = temp[pax_col] * temp[pen_col]
        temp["actual_value"] = temp[target_col]

        temp["error"] = (
            temp["forecast_value"]
            - temp["actual_value"]
        )

        temp["absolute_error"] = temp["error"].abs()

        temp["percentage_error"] = (
            temp["error"]
            / temp["actual_value"].replace(0, np.nan)
        )

        temp["absolute_percentage_error"] = (
            temp["percentage_error"].abs()
        )

        temp["squared_error"] = temp["error"] ** 2

        results.append(
            temp[
                [
                    "date",
                    "weekday",
                    "month",
                    "target",
                    "method",
                    pax_col,
                    pen_col,
                    "forecast_value",
                    "actual_value",
                    "error",
                    "absolute_error",
                    "percentage_error",
                    "absolute_percentage_error",
                    "squared_error",
                ]
            ].rename(
                columns={
                    pax_col: "passenger_base",
                    pen_col: "penetration_used",
                }
            )
        )

    for spec in target_specs:

        target_name = spec["target_name"]
        target_col = spec["target_col"]
        pax_col = spec["pax_col"]
        penetration_col = spec["penetration_col"]

        # --------------------------------------------------------
        # 1. Optional general rolling methods
        # --------------------------------------------------------
        if spec["run_general_rolling"]:

            for window in config["tendency_windows_weeks"]:

                df_window = calculate_rolling_penetration(
                    daily_driver_dataset=base,
                    window_weeks=window,
                    target_col=target_col,
                    pax_col=pax_col,
                    target_name=target_name,
                )

                pen_col = f"rolling_{window}wk_{target_name}_penetration"

                append_forecast_results(
                    temp=df_window,
                    pen_col=pen_col,
                    method_name=f"rolling_{window}wk_{target_name}_penetration",
                    target_name=target_name,
                    target_col=target_col,
                    pax_col=pax_col,
                )

        # --------------------------------------------------------
        # 2. Same-weekday ratio-of-sums methods
        # --------------------------------------------------------
        for n in config["same_weekday_occurrences"]:

            df_same = calculate_same_weekday_penetration(
                daily_driver_dataset=base,
                n_occurrences=n,
                target_col=target_col,
                pax_col=pax_col,
                target_name=target_name,
            )

            pen_col = f"same_weekday_last_{n}_{target_name}_penetration"

            append_forecast_results(
                temp=df_same,
                pen_col=pen_col,
                method_name=f"same_weekday_last_{n}_{target_name}_penetration",
                target_name=target_name,
                target_col=target_col,
                pax_col=pax_col,
            )

        # --------------------------------------------------------
        # 3. Same-weekday rolling average methods
        # --------------------------------------------------------
        for n in config.get("same_weekday_average_occurrences", []):

            df_avg = calculate_same_weekday_average_penetration(
                daily_driver_dataset=base,
                n_occurrences=n,
                penetration_col=penetration_col,
                target_name=target_name,
            )

            pen_col = f"same_weekday_avg_last_{n}_{target_name}_penetration"

            append_forecast_results(
                temp=df_avg,
                pen_col=pen_col,
                method_name=f"same_weekday_avg_last_{n}_{target_name}_penetration",
                target_name=target_name,
                target_col=target_col,
                pax_col=pax_col,
            )

        # --------------------------------------------------------
        # 4. Weighted same-weekday methods
        # --------------------------------------------------------
        for n in config.get("same_weekday_weighted_occurrences", []):

            df_weighted = calculate_weighted_same_weekday_penetration(
                daily_driver_dataset=base,
                n_occurrences=n,
                penetration_col=penetration_col,
                target_name=target_name,
            )

            pen_col = f"same_weekday_weighted_last_{n}_{target_name}_penetration"

            append_forecast_results(
                temp=df_weighted,
                pen_col=pen_col,
                method_name=f"same_weekday_weighted_last_{n}_{target_name}_penetration",
                target_name=target_name,
                target_col=target_col,
                pax_col=pax_col,
            )

    if results:
        return pd.concat(results, ignore_index=True)

    return pd.DataFrame()


def summarise_backtest_results(backtest_results):
    """
    Summarise tendency back-test performance for entries and exits.

    Returns:
        dict:
            - overall_method_performance
            - performance_by_weekday
            - performance_by_month
    """

    if backtest_results is None or backtest_results.empty:
        return {
            "overall_method_performance": pd.DataFrame(),
            "performance_by_weekday": pd.DataFrame(),
            "performance_by_month": pd.DataFrame(),
        }

    df = backtest_results.copy()

    overall_method_performance = (
        df
        .groupby(["target", "method"])
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            rmse=("squared_error", lambda x: (x.mean()) ** 0.5),
            total_forecast=("forecast_value", "sum"),
            total_actual=("actual_value", "sum"),
            records=("forecast_value", "count"),
        )
        .reset_index()
    )

    performance_by_weekday = (
        df
        .groupby(["target", "method", "weekday"])
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            records=("forecast_value", "count"),
        )
        .reset_index()
    )

    performance_by_month = (
        df
        .groupby(["target", "method", "month"])
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            records=("forecast_value", "count"),
        )
        .reset_index()
    )

    return {
        "overall_method_performance": overall_method_performance,
        "performance_by_weekday": performance_by_weekday,
        "performance_by_month": performance_by_month,
    }



def build_output_tables(
    reconciliation_summary,
    daily_fastpark_actuals,
    hourly_fastpark_actuals,
    daily_driver_dataset,
    booking_curve,
    duration_df,
    tendency_backtest_results,
    forecast_error_summaries,
):
    """
    Package final analysis outputs into a dictionary.

    Purpose:
        Make it easy to export to Excel, Power BI, or inspect in notebook.

    Parameters:
        reconciliation_summary: pandas.DataFrame
        daily_fastpark_actuals: pandas.DataFrame
        hourly_fastpark_actuals: pandas.DataFrame
        daily_driver_dataset: pandas.DataFrame
        booking_curve: pandas.DataFrame
        duration_df: pandas.DataFrame
        tendency_backtest_results: pandas.DataFrame
        forecast_error_summaries: dict

    Returns:
        dict:
            Dictionary of named output tables.
    """

    outputs = {
        "Data Validation Summary": reconciliation_summary,
        "Daily FastPark Actuals": daily_fastpark_actuals,
        "Hourly FastPark Actuals": hourly_fastpark_actuals,
        "Demand Driver Dataset": daily_driver_dataset,
        "Booking Entry Dataset": booking_curve,
        "Tendency Backtest Results": tendency_backtest_results,
        "forecast_error_summaries": forecast_error_summaries,
    }

    return outputs


def export_outputs_to_excel(outputs, output_path):
    """
    Export all analysis outputs to a single Excel workbook.

    Handles:
        - DataFrames
        - dictionaries of DataFrames
        - nested dictionaries of DataFrames

    Parameters
    ----------
    outputs : dict
        Analysis outputs dictionary.

    output_path : str
        Output workbook path.

    Returns
    -------
    None
    """

    def flatten_outputs(obj, parent_key=""):
        """
        Recursively flatten nested dictionaries of DataFrames.

        Example:
            {
                "forecast_error_summaries": {
                    "driver_analysis": {
                        "correlation_summary": df
                    }
                }
            }

        becomes:
            forecast_error_summaries_driver_analysis_correlation_summary
        """

        flattened = {}

        SKIP_EXPORTS = {
            "entry_forecast_evaluations",
            "daily_exit_forecast_evaluations",
            "hourly_exit_forecast_evaluations",
            "alignment_dataset",
        }

        if isinstance(obj, pd.DataFrame):

            if any(
                x in parent_key
                for x in SKIP_EXPORTS
            ):
                return {}

            flattened[parent_key] = obj

        elif isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{parent_key}_{key}" if parent_key else str(key)
                flattened.update(flatten_outputs(value, new_key))

        return flattened

    def clean_sheet_name(sheet_name):
        """
        Clean Excel sheet name:
            - remove invalid Excel characters
            - trim to 31 characters
            - avoid blank sheet names
        """

        sheet_name = str(sheet_name)

        # Excel does not allow: \ / ? * [ ] :
        sheet_name = re.sub(r"[\\/*?:\[\]]", "_", sheet_name)

        # Excel sheet names cannot be blank.
        if not sheet_name.strip():
            sheet_name = "Sheet"

        return sheet_name[:31]

    # Ensure output directory exists.
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    flat_outputs = flatten_outputs(outputs)

    SHEET_NAME_MAP = {
        "forecast_error_summaries_driver_analysis_demand_driver_summary":
            "Demand Driver Summary",

        "forecast_error_summaries_driver_analysis_correlation_summary":
            "Driver Correlations",

        "forecast_error_summaries_driver_analysis_price_correlation_summary":
            "Price Driver Corr",

        "forecast_error_summaries_driver_analysis_weekday_summary":
            "Weekly Demand Drivers",

        "forecast_error_summaries_driver_analysis_monthly_summary":
            "Monthly Demand Drivers",

        "forecast_error_summaries_driver_analysis_month_by_year_summary":
            "Month Year Drivers",

        "forecast_error_summaries_driver_analysis_passenger_volume_band_summary":
            "Demand by Pax Band",


        # Passenger Mix

        "forecast_error_summaries_mix_summary":
            "Passenger Mix",

        # Estimated Occupancy Analysis
        "forecast_error_summaries_daily_occupancy_summary":
            "Daily Occupancy",

        "forecast_error_summaries_occupancy_analysis_occupancy_band_summary":
            "Occupancy Bands",

        "forecast_error_summaries_occupancy_analysis_occupancy_correlation_summary":
            "Occupancy Correlations",

        # Tendency Analysis

        "Tendency Backtest Results":
            "Tendency Results",

        "forecast_error_summaries_tendency_summary_overall_method_performance":
            "Tendency Performance",

        "forecast_error_summaries_tendency_summary_performance_by_weekday":
            "Tendency by Weekday",

        "forecast_error_summaries_tendency_summary_performance_by_month":
            "Tendency by Month",

        # Booking visibility curves
        "forecast_error_summaries_booking_visibility_curves_entry_curve_summary":
            "Booking Entry Curve",

        "forecast_error_summaries_booking_visibility_curves_entry_curve_duration":
            "Booking Entry Duration",

        "forecast_error_summaries_booking_visibility_curves_exit_curve_summary":
            "Booking Exit Curve",

        "forecast_error_summaries_booking_visibility_curves_exit_curve_duration":
            "Booking Exit Duration",

        # Price Premium Analysis
        "forecast_error_summaries_price_premium_analysis_price_premium_detail":
            "Price Premium Detail",

        "forecast_error_summaries_price_premium_analysis_price_premium_band_summary":
            "Price Premium Bands",

        "forecast_error_summaries_price_premium_analysis_price_premium_horizon_summary":
            "Price Premium Horizon",

        "forecast_error_summaries_price_premium_analysis_price_premium_duration_summary":
            "Price Premium Duration",

        # Planned duration analysis
        "forecast_error_summaries_duration_patterns_planned_duration_distribution":
            "Planned Duration Dist",

        "forecast_error_summaries_duration_patterns_planned_duration_by_weekday":
            "Planned Duration Weekday",

        "forecast_error_summaries_duration_patterns_planned_duration_by_month":
            "Planned Duration Month",

        "forecast_error_summaries_duration_patterns_planned_duration_by_airline":
            "Planned Duration Airline",

        "forecast_error_summaries_duration_patterns_planned_vs_actual_summary":
            "Duration Validation",

        "forecast_error_summaries_no_show_results_duration_match_check":
            "No Show Check",

        "forecast_error_summaries_status_summary":
            "Status Summary",

        # Cancellation Analysis

        "forecast_error_summaries_cancellation_results_cancellation_summary":
            "Cancellation Summary",

        "forecast_error_summaries_cancellation_results_cancellation_by_lead_time":
            "Cancel by Lead Time",

        "forecast_error_summaries_cancellation_results_cancellation_by_duration_band":
            "Cancel by Duration",

        "forecast_error_summaries_cancellation_results_cancellation_by_duration_and_lead_time":
            "Cancel by Duration/Lead Time",

        "forecast_error_summaries_cancellation_results_cancellation_by_airline":
            "Cancel by Airline",


        # No-Show Analysis

        "forecast_error_summaries_no_show_results_no_show_summary":
            "No Show Summary",

        "forecast_error_summaries_no_show_results_no_show_by_lead_time":
            "No Show Lead Time",

        "forecast_error_summaries_no_show_results_no_show_by_duration_band":
            "No Show Duration",

        "forecast_error_summaries_no_show_results_no_show_by_duration_and_lead_time":
            "No Show Duration/Lead Time",


        # Hourly Profiles

        "forecast_error_summaries_hourly_profiles_entry_profile_overall":
            "Hourly Entry Profile",

        "forecast_error_summaries_hourly_profiles_exit_profile_overall":
            "Hourly Exit Profile",

        "forecast_error_summaries_hourly_profiles_movement_profile_overall":
            "Hourly Move Profile",

        "forecast_error_summaries_hourly_profiles_entry_profile_by_weekday":
            "Hourly Entry Weekday",

        "forecast_error_summaries_hourly_profiles_exit_profile_by_weekday":
            "Hourly Exit Weekday",

        "forecast_error_summaries_hourly_profiles_movement_profile_by_weekday":
            "Hourly Move Weekday",

        # Hourly Offset Analysis

        "forecast_error_summaries_hourly_offset_analysis_entry_departure_offset_summary":
            "Entry Pax Offset",

        "forecast_error_summaries_hourly_offset_analysis_exit_arrival_offset_summary":
            "Exit Pax Offset",

        "forecast_error_summaries_hourly_offset_analysis_best_entry_departure_offset":
            "Best Entry Offset",

        "forecast_error_summaries_hourly_offset_analysis_best_exit_arrival_offset":
            "Best Exit Offset",


        # Return Behaviour

        "forecast_error_summaries_return_deviation_overall_deviation_summary":
            "Return Summary",

        "forecast_error_summaries_return_deviation_deviation_by_expected_return_hour":
            "Return by Hour",

        "forecast_error_summaries_return_deviation_deviation_by_month":
            "Return by Month",

        "forecast_error_summaries_return_deviation_deviation_by_duration_band":
            "Return by Duration",

        "forecast_error_summaries_return_deviation_early_return_summary":
            "Early Return Summary",

        "forecast_error_summaries_return_deviation_late_return_summary":
            "Late Return Summary",

        "forecast_error_summaries_return_deviation_different_day_return_summary":
            "Different Day Returns",

        "forecast_error_summaries_known_booked_exit_profile":
            "Known Exit Profile",
    }

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:

        used_sheet_names = set()
        sheets_written = 0

        for raw_sheet_name, df in flat_outputs.items():

            if df is None or not isinstance(df, pd.DataFrame):
                continue

            export_sheet_name = SHEET_NAME_MAP.get(raw_sheet_name, raw_sheet_name)
            safe_sheet_name = clean_sheet_name(export_sheet_name)

            counter = 1
            base_sheet_name = safe_sheet_name

            while safe_sheet_name in used_sheet_names:
                suffix = f"_{counter}"
                safe_sheet_name = base_sheet_name[: 31 - len(suffix)] + suffix
                counter += 1

            used_sheet_names.add(safe_sheet_name)

            print(
                f"Exporting: {raw_sheet_name} "
                f"({len(df):,} rows)"
            )

            df.to_excel(
                writer,
                sheet_name=safe_sheet_name,
                index=False,
            )

            print(
                f"completed: {raw_sheet_name}"
            )

            sheets_written += 1

        # If everything was empty, still create a visible workbook.
        if sheets_written == 0:
            pd.DataFrame(
                [{"message": "No DataFrame outputs were available to export."}]
            ).to_excel(
                writer,
                sheet_name="No outputs",
                index=False,
            )


# ============================================================
# 16. MAIN PIPELINE
# ============================================================

def run_fastpark_historical_analysis(sql_connection, output_path=None):
    """
    Run the full historical analysis pipeline.

    Parameters:
        sql_connection:
            Database connection object.
        output_path:
            Path to export Excel outputs. If None, outputs are not exported.

    Returns:
        dict:
            Dictionary of output tables from the full analysis.
    """
    print("FASTPARK ANALYSIS PIPELINE")

  
    # ----------------------------
    # Config
    # ----------------------------
    config = get_analysis_config()

    print(f"Window : {config['analysis_start_date']} → {config['analysis_end_date']}")

    t0 = time.perf_counter()
    # ----------------------------
    # Load raw data
    # ----------------------------

    print("[1/14] Loading FastPark Bookings…")
    bookings_raw = get_fastpark_bookings(
        start=config["analysis_start_date"],
        end=config["analysis_end_date"],
        statuses=["B", "CX", "F"],
        asset_name=config["asset_name"],
        engine=sql_connection,
    )

    t1 = step(t0, f"Loaded FastPark Bookings ({len(bookings_raw):,} rows)")

    print("[2/14] Loading FastPark Actuals…")

    operations_raw = get_fastpark_entry_exits(
        start=config["analysis_start_date"],
        end=config["analysis_end_date"],
        engine=sql_connection,
    )

    t2 = step(t1, f"Loaded FastPark Actuals ({len(operations_raw):,} rows)")

    print("[3/14] Loading Historical Flights…")

    flights_raw = get_historical_flight_performance(
        start=config["analysis_start_date"],
        end=config["analysis_end_date"],
        engine=sql_connection,
    )
    t3 = step(t2, f"Loaded Historical Flight Performance ({len(flights_raw):,} rows)")



    # ----------------------------
    # Clean raw data
    # ----------------------------

    print("[4/14] Cleaning FastPark Bookings, Actuals and Flights…")

    bookings_clean = clean_bookings(bookings_raw, config)
    operations_clean = clean_operations(operations_raw, config)
    flights_clean = clean_flights(flights_raw, config)
    daily_price_summary = create_daily_price_summary(bookings_clean)

    t4 = step(t3, "Cleaned FastPark Bookings, Actuals and Flights")

    # ----------------------------
    # Reconcile bookings and operations
    # ----------------------------

    print("[5/14] Reconciling FastPark Bookings and Actuals…")

    master = reconcile_bookings_to_operations(bookings_clean, operations_clean)


    reconciliation_summary = create_reconciliation_summary(
        bookings_clean,
        operations_clean,
        master,
    )

    t5 = step(t4, "Reconciled FastPark Bookings and Actuals")

    # ----------------------------
    # Status, cancellations and no-shows
    # ----------------------------

    print("[6/14] Analysing Booking Statuses, Cancellations and No-Shows…")

    status_summary = analyse_booking_statuses(bookings_clean)
    cancellation_results = analyse_cancellations(bookings_clean)
    no_show_results = analyse_no_shows(master)

    t6 = step(t5, "Analysed Booking Statuses, Cancellations and No-Shows ")

    # ----------------------------
    # Actual FastPark demand
    # ----------------------------

    print("[7/14] Creating Daily and Hourly FastPark Actuals…")

    daily_fastpark_actuals = create_daily_fastpark_actuals(master)

    daily_occupancy_summary = create_estimated_occupancy_series(
        daily_fastpark_actuals=daily_fastpark_actuals,
        config=config,
    )

    hourly_fastpark_actuals = create_hourly_fastpark_actuals(master)
    hourly_profiles = create_hourly_profiles(hourly_fastpark_actuals)

    t7 = step(t6, "Created Daily and Hourly FastPark Actuals")


    # ----------------------------
    # Passenger / flight context
    # ----------------------------

    print("[8/14] Creating Passenger Context Features…")
    daily_passenger_summary = create_daily_passenger_summary(flights_clean, config)
    hourly_passenger_summary = create_hourly_passenger_summary(flights_clean, config)

    # Raw daily airline/country mix tables are not exported in this version
    # passenger mix is summarised later from the daily driver dataset
    #mix_features = create_airline_country_mix_features(flights_clean)

    t8 = step(t7, "Created Passenger Context Features")

    # ----------------------------
    # Hourly FastPark vs passenger offset analysis
    # ----------------------------

    print("[9/14] Analysing Hourly FastPark vs Passenger Offsets…")

    hourly_offset_analysis = analyse_hourly_passenger_offsets(
        hourly_fastpark_actuals=hourly_fastpark_actuals,
        hourly_passenger_summary=hourly_passenger_summary,
        max_departure_lead_hours=8,
        max_arrival_lag_hours=8,
    )

    t9 = step(t8, "Analysed Hourly FastPark vs Passenger Offsets")

    # ----------------------------
    # Daily driver dataset
    # ----------------------------

    print("[10/14] Analysing Daily FastPark Drivers…")
    daily_driver_dataset = create_daily_driver_dataset(
        daily_fastpark_actuals,
        daily_passenger_summary,
        daily_price_summary=daily_price_summary,
        daily_occupancy_summary=daily_occupancy_summary,
    )

    driver_analysis = analyse_actual_demand_drivers(daily_driver_dataset)
    mix_summary = analyse_mix_features(daily_driver_dataset)
    occupancy_analysis = analyse_estimated_occupancy(daily_driver_dataset)

    t10 = step(t9, "Analysed Daily FastPark Drivers")

    # ----------------------------
    # Booking curve
    # ----------------------------

    print("[11/14] Analysing Booking and Exit Visibility Curves…")

    booking_curve = create_booking_curve_dataset(
        bookings_clean=bookings_clean,
        master=master,
        config=config,
    )

    booking_entry_curve_duration = create_booking_entry_curve_by_duration_dataset(
        bookings_clean=bookings_clean,
        master=master,
        config=config,
    )

    booking_exit_curve = create_booking_exit_curve_dataset(
        bookings_clean=bookings_clean,
        master=master,
        config=config,
    )

    booking_exit_curve_duration = create_booking_exit_curve_by_duration_dataset(
        bookings_clean=bookings_clean,
        master=master,
        config=config,
    )

    booking_visibility_curves = analyse_booking_visibility_curves(
        booking_entry_curve=booking_curve,
        booking_entry_curve_duration=booking_entry_curve_duration,
        booking_exit_curve=booking_exit_curve,
        booking_exit_curve_duration=booking_exit_curve_duration,
    )

    price_premium_analysis = analyse_price_premium_booking_curve(
        bookings_clean=bookings_clean,
        booking_entry_curve_duration=booking_entry_curve_duration,
        config=config,
    )

    t11 = step(
        t10,
        "Analysed Booking, Exit Visibility and Price Premium Curves",
    )

    # ----------------------------
    # Duration and return behaviour
    # ----------------------------

    print("[12/14] Analysing Duration and Return Behaviour…")

    duration_df = create_duration_analysis_dataset(master, config)
    duration_patterns = analyse_duration_patterns(duration_df)
    return_deviation = analyse_return_deviation(duration_df)
    known_booked_exit_profile = create_known_booked_exit_profile(master)

    t12 = step(t11, "Analysed Duration and Return Behaviour")


    # ----------------------------
    # Tendency back-testing
    # ----------------------------

    print("[13/14] Back-testing Tendency Forecast Methods…")

    tendency_backtest_results = run_tendency_window_backtest(
        daily_driver_dataset,
        config,
    )

    tendency_summary = summarise_backtest_results(tendency_backtest_results)

    t13 = step(t12, "Back-tested Tendency Forecast Methods")

    # ----------------------------
    # Package outputs
    # ----------------------------

    print("[14/14] Building Outputs…")
    forecast_error_summaries = {
        "status_summary": status_summary,
        "cancellation_results": cancellation_results,
        "no_show_results": no_show_results,
        "hourly_profiles": hourly_profiles,
        "hourly_offset_analysis": hourly_offset_analysis,
        "driver_analysis": driver_analysis,
        "mix_summary": mix_summary,
        "occupancy_analysis": occupancy_analysis,
        "daily_occupancy_summary": daily_occupancy_summary,
        "booking_visibility_curves": booking_visibility_curves,
        "price_premium_analysis": price_premium_analysis,
        "duration_patterns": duration_patterns,
        "return_deviation": return_deviation,
        "known_booked_exit_profile": known_booked_exit_profile,
        "tendency_summary": tendency_summary,
    }

    outputs = build_output_tables(
        reconciliation_summary=reconciliation_summary,
        daily_fastpark_actuals=daily_fastpark_actuals,
        hourly_fastpark_actuals=hourly_fastpark_actuals,
        daily_driver_dataset=daily_driver_dataset,
        booking_curve=booking_curve,
        duration_df=duration_df,
        tendency_backtest_results=tendency_backtest_results,
        forecast_error_summaries=forecast_error_summaries,
    )

    t14 = step(t13, "Built Outputs")

    
    final_time = t14

    if output_path is not None:
        print(f"Exporting Outputs to {output_path}…")
        export_outputs_to_excel(
            outputs=outputs,
            output_path=output_path,
        )
        final_time = step(t14, f"Exported Outputs to {output_path}")

    print(
        f"Completed FastPark Historical Analysis "
        f"in {final_time - t0:.2f} seconds."
    )

    return outputs




if __name__ == "__main__":
    dsn = 'AzureConnection'
    user = 'jamie_douglas'


    sql_connection = get_engine(dsn=dsn, username=user)

    outputs = run_fastpark_historical_analysis(
        sql_connection=sql_connection,
        output_path=r"output\fastpark\reports\fastpark_historical_analysis_v3.xlsx",
    )