import sys
import pathlib
from pathlib import Path
import time
from tracemalloc import start

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
    7. Operational workload and FTE translation

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

        # Rolling tendency windows in weeks
        "tendency_windows_weeks": [2, 4, 6, 8, 13],

        # Same weekday occurrence windows
        # Example: last 4 Mondays, last 8 Mondays, etc.
        "same_weekday_occurrences": [4, 8, 12],

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

    #Price metrics
    df["booking_total_per_quantity"] = df["bookingTotal"] / df["productQuantity"].replace(0, np.nan)

    return df


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

    operations_latest = (
        operations_clean
        .sort_values(
            ["BookingReference", "ActualCheckedOutDate"]
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
    duplicate_booking_refs = operations_clean["BookingReference"].duplicated().sum()

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
                "metric": "duplicate_booking_references",
                "value": duplicate_booking_refs,
                "description": "Duplicate BookingReference rows in operational table.",
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
        "cancellation_by_weekday": cancellation_rate_by(["entry_weekday"]),
        "cancellation_by_month": cancellation_rate_by(["entry_month"]),
        "cancellation_by_channel": cancellation_rate_by(["channel"]),
        "cancellation_by_duration_band": cancellation_rate_by(["planned_duration_band"]),
        "cancellation_by_airline": cancellation_rate_by(["outboundAirline"]),
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

    results = {
        "no_show_summary": no_show_summary,
        "no_show_by_lead_time": no_show_rate_by(["lead_time_band"]),
        "no_show_by_weekday": no_show_rate_by(["entry_weekday"]),
        "no_show_by_month": no_show_rate_by(["entry_month"]),
        "no_show_by_channel": no_show_rate_by(["channel"]),
        "no_show_by_duration_band": no_show_rate_by(["planned_duration_band"]),
        "no_show_by_airline": no_show_rate_by(["outboundAirline"]),
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
        "entry_profile_by_month": entry_profile_by_month,
        "exit_profile_by_month": exit_profile_by_month,
        "movement_profile_by_month": movement_profile_by_month,
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

def create_daily_driver_dataset(daily_fastpark_actuals, daily_passenger_summary):
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

    daily_fastpark_actuals["date"] = pd.to_datetime(
    daily_fastpark_actuals["date"]
)

    daily_passenger_summary["date"] = pd.to_datetime(
        daily_passenger_summary["date"]
    )

    daily = daily_fastpark_actuals.merge(
        daily_passenger_summary,
        on="date",
        how="left"
    )

    #Calculate entry penetration using departing passengers
    daily["entry_penetration"] = daily["entries"] / daily["departing_pax"].replace(0, np.nan)

    #Calculate exit penetration using arriving passengers
    #This is only a benchmark, not necessarily the final exit forecasting logic.
    daily["exit_penetration"] = daily["exits"] / daily["arriving_pax"].replace(0, np.nan)

    #Calendar features
    daily["date"] = pd.to_datetime(daily["date"])
    daily["weekday"] = daily["date"].dt.day_name()
    daily["weekday_num"] = daily["date"].dt.weekday
    daily["month"] = daily["date"].dt.month
    daily["year"] = daily["date"].dt.year
    daily["week"] = daily["date"].dt.isocalendar().week


    return daily

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

    fastpark["datetime_hour"] = pd.to_datetime(fastpark["datetime_hour"])

    pax["date"] = pd.to_datetime(pax["date"])
    pax["datetime_hour"] = pax["date"] + pd.to_timedelta(pax["hour"], unit="h")

    cols = [
        "datetime_hour",
        "departing_pax",
        "arriving_pax",
        "departing_flights",
        "arriving_flights",
    ]

    alignment = fastpark.merge(
        pax[cols],
        on="datetime_hour",
        how="left",
    )

    for col in ["departing_pax", "arriving_pax", "departing_flights", "arriving_flights"]:
        alignment[col] = alignment[col].fillna(0)

    alignment = alignment.sort_values("datetime_hour")

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

    Parameters:
        daily_driver_dataset: pandas.DataFrame
            Daily FastPark demand joined to passenger features.

    Returns:
        dict:
            Driver analysis outputs:
                - correlation_summary
                - weekday_summary
                - monthly_summary
                - passenger_volume_band_summary
                - domestic_international_summary
    """
    df = daily_driver_dataset.copy()

    correlation_summary = daily_driver_dataset[
        [
          "entries",
          "exits",
          "movements",
          "departing_pax",
          "arriving_pax",
          "total_pax",
          "departing_flights",
          "arriving_flights"
        ]
    ].corr()
    
    weekday_summary = df.groupby("weekday").agg(
        avg_entries=("entries", "mean"),
        avg_exits=("exits", "mean"),
        avg_entry_penetration=("entry_penetration", "mean"),
        avg_exit_penetration=("exit_penetration", "mean"),
    )

    monthly_summary = df.groupby("month").agg(
        avg_entries=("entries", "mean"),
        avg_exits=("exits", "mean"),
        avg_entry_penetration=("entry_penetration", "mean"),
        avg_exit_penetration=("exit_penetration", "mean"),
    )

    df["passenger_volume_band"] = pd.qcut(
        df["departing_pax"],
        q=5,
        duplicates="drop"
    )

    passenger_volume_band_summary = (
        df
        .groupby(
            "passenger_volume_band",
            observed=False
        )
        .agg(
            avg_entries=("entries", "mean"),
            avg_entry_penetration=("entry_penetration", "mean"),
            observations=("entries", "count")
        )
        .reset_index()
    )   

    df["domestic_share"] = (
        df["domestic_departing_pax"]
        / df["departing_pax"]
    )

    df["domestic_share_band"] = pd.cut(
        df["domestic_share"],
        bins=[0,0.2,0.4,0.6,0.8,1]
    )

    domestic_international_summary = (
        df
        .groupby(
            "domestic_share_band",
            observed=False
        )
        .agg(
            avg_entry_penetration=("entry_penetration","mean"),
            avg_exit_penetration=("exit_penetration","mean")
        )
        .reset_index()
    )

    results = {
        "correlation_summary": correlation_summary,
        "weekday_summary": weekday_summary,
        "monthly_summary": monthly_summary,
        "passenger_volume_band_summary": passenger_volume_band_summary,
        "domestic_international_summary": domestic_international_summary,
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


def analyse_booking_curve_segments(booking_curve):
    """
    Analyse booking curve completion rates by segment.

    Purpose:
        Determine whether one booking uplift/completion factor is enough,
        or whether separate curves are needed by weekday/month.

    Parameters
    ----------
    booking_curve : pandas.DataFrame
        Output from create_booking_curve_dataset().

    Returns
    -------
    dict
        Segment-level booking curve summaries.
    """

    df = booking_curve.copy()

    completion_by_lead_time = (
        df
        .groupby("lead_time_checkpoint")
        .agg(
            avg_bookings_known=("bookings_known", "mean"),
            avg_final_valid_bookings=("final_valid_bookings", "mean"),
            avg_final_actual_entries=("final_actual_entries", "mean"),
            avg_completion_vs_valid=("completion_vs_valid_bookings", "mean"),
            avg_completion_vs_actual=("completion_vs_actual_entries", "mean"),
            observations=("entry_date", "nunique"),
        )
        .reset_index()
        .sort_values("lead_time_checkpoint", ascending=False)
    )

    completion_by_weekday_and_lead_time = (
        df
        .groupby(["weekday", "weekday_num", "lead_time_checkpoint"])
        .agg(
            avg_bookings_known=("bookings_known", "mean"),
            avg_final_valid_bookings=("final_valid_bookings", "mean"),
            avg_final_actual_entries=("final_actual_entries", "mean"),
            avg_completion_vs_valid=("completion_vs_valid_bookings", "mean"),
            avg_completion_vs_actual=("completion_vs_actual_entries", "mean"),
            observations=("entry_date", "nunique"),
        )
        .reset_index()
        .sort_values(["weekday_num", "lead_time_checkpoint"], ascending=[True, False])
    )

    completion_by_month_and_lead_time = (
        df
        .groupby(["month", "lead_time_checkpoint"])
        .agg(
            avg_bookings_known=("bookings_known", "mean"),
            avg_final_valid_bookings=("final_valid_bookings", "mean"),
            avg_final_actual_entries=("final_actual_entries", "mean"),
            avg_completion_vs_valid=("completion_vs_valid_bookings", "mean"),
            avg_completion_vs_actual=("completion_vs_actual_entries", "mean"),
            observations=("entry_date", "nunique"),
        )
        .reset_index()
        .sort_values(["month", "lead_time_checkpoint"], ascending=[True, False])
    )

    results = {
        "completion_by_lead_time": completion_by_lead_time,
        "completion_by_weekday_and_lead_time": completion_by_weekday_and_lead_time,
        "completion_by_month_and_lead_time": completion_by_month_and_lead_time,
    }

    return results

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
    Analyse stay duration patterns.

    Purpose:
        Understand how entries convert into future exits.

    Parameters
    ----------
    duration_df : pandas.DataFrame
        Output from create_duration_analysis_dataset().

    Returns
    -------
    dict
        Duration summary tables.
    """

    df = duration_df.copy()

    valid_duration = df.dropna(subset=["actual_duration_days"]).copy()

    duration_distribution = (
        valid_duration
        .groupby("actual_duration_band", observed=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_actual_duration_days=("actual_duration_days", "mean"),
            median_actual_duration_days=("actual_duration_days", "median"),
            avg_planned_duration_days=("planned_duration_days", "mean"),
            median_planned_duration_days=("planned_duration_days", "median"),
        )
        .reset_index()
    )

    duration_distribution["share"] = (
        duration_distribution["bookings"]
        / duration_distribution["bookings"].sum()
    )

    duration_by_weekday = (
        valid_duration
        .groupby("entry_weekday", dropna=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_actual_duration_days=("actual_duration_days", "mean"),
            median_actual_duration_days=("actual_duration_days", "median"),
            avg_planned_duration_days=("planned_duration_days", "mean"),
            median_planned_duration_days=("planned_duration_days", "median"),
        )
        .reset_index()
    )

    duration_by_month = (
        valid_duration
        .groupby("entry_month", dropna=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_actual_duration_days=("actual_duration_days", "mean"),
            median_actual_duration_days=("actual_duration_days", "median"),
            avg_planned_duration_days=("planned_duration_days", "mean"),
            median_planned_duration_days=("planned_duration_days", "median"),
        )
        .reset_index()
    )

    duration_by_airline = (
        valid_duration
        .groupby("outboundAirline", dropna=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_actual_duration_days=("actual_duration_days", "mean"),
            median_actual_duration_days=("actual_duration_days", "median"),
            avg_planned_duration_days=("planned_duration_days", "mean"),
            median_planned_duration_days=("planned_duration_days", "median"),
        )
        .reset_index()
        .sort_values("bookings", ascending=False)
    )

    duration_by_route = (
        valid_duration
        .groupby("outboundRoute", dropna=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_actual_duration_days=("actual_duration_days", "mean"),
            median_actual_duration_days=("actual_duration_days", "median"),
            avg_planned_duration_days=("planned_duration_days", "mean"),
            median_planned_duration_days=("planned_duration_days", "median"),
        )
        .reset_index()
        .sort_values("bookings", ascending=False)
    )

    duration_by_lead_time_band = (
        valid_duration
        .groupby("planned_duration_band", dropna=False, observed=False)
        .agg(
            bookings=("bookingId", "nunique"),
            avg_actual_duration_days=("actual_duration_days", "mean"),
            median_actual_duration_days=("actual_duration_days", "median"),
            avg_planned_duration_days=("planned_duration_days", "mean"),
            median_planned_duration_days=("planned_duration_days", "median"),
        )
        .reset_index()
    )

    actual_vs_planned_duration_summary = pd.DataFrame(
        [
            {
                "records": valid_duration["bookingId"].nunique(),
                "avg_planned_duration_days": valid_duration["planned_duration_days"].mean(),
                "avg_actual_duration_days": valid_duration["actual_duration_days"].mean(),
                "avg_actual_minus_planned_days": (
                    valid_duration["actual_duration_days"]
                    - valid_duration["planned_duration_days"]
                ).mean(),
                "median_actual_minus_planned_days": (
                    valid_duration["actual_duration_days"]
                    - valid_duration["planned_duration_days"]
                ).median(),
            }
        ]
    )

    return {
        "duration_distribution": duration_distribution,
        "duration_by_weekday": duration_by_weekday,
        "duration_by_month": duration_by_month,
        "duration_by_airline": duration_by_airline,
        "duration_by_route": duration_by_route,
        "duration_by_lead_time_band": duration_by_lead_time_band,
        "actual_vs_planned_duration_summary": actual_vs_planned_duration_summary,
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
        "deviation_by_weekday": deviation_by_weekday,
        "deviation_by_month": deviation_by_month,
        "early_return_summary": early_return_summary,
        "late_return_summary": late_return_summary,
        "different_day_return_summary": different_day_return_summary,
    }


def create_known_booked_exit_forecast(master):
    """
    Create known booked exit forecast using expected / advised return datetime.

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
        Known booked exit forecast by expected return hour.
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


def calculate_exit_penetration_baselines(daily_driver_dataset, config):
    """
    Create historical exit penetration baselines.

    Purpose:
        Build current-style exit forecast assumptions using arriving passengers.

    Formula:
        ForecastExits[d] = ArrivingPassengers[d] * HistoricalExitPenetration[d]

    Returns
    -------
    pandas.DataFrame
        Dataset with multiple exit penetration baseline columns.
    """

    df = daily_driver_dataset.copy()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    df["exit_penetration"] = (
        df["exits"] / df["arriving_pax"].replace(0, np.nan)
    )

    overall_exit_pen = (
        df["exits"].sum()
        / df["arriving_pax"].sum()
        if df["arriving_pax"].sum() != 0
        else np.nan
    )

    df["overall_avg_exit_penetration"] = overall_exit_pen

    weekday_exit_pen = (
        df
        .groupby("weekday", dropna=False)
        .agg(
            exits=("exits", "sum"),
            arriving_pax=("arriving_pax", "sum")
        )
        .reset_index()
    )

    weekday_exit_pen["weekday_avg_exit_penetration"] = (
        weekday_exit_pen["exits"]
        / weekday_exit_pen["arriving_pax"].replace(0, np.nan)
    )

    weekday_exit_pen = weekday_exit_pen[
        ["weekday", "weekday_avg_exit_penetration"]
    ]


    df = df.merge(weekday_exit_pen, on="weekday", how="left")

    month_exit_pen = (
        df
        .groupby("month", dropna=False, observed=False)
        .agg(
            exits=("exits", "sum"),
            arriving_pax=("arriving_pax", "sum")
        )
        .reset_index()
    )

    month_exit_pen["monthly_avg_exit_penetration"] = (
        month_exit_pen["exits"]
        / month_exit_pen["arriving_pax"].replace(0, np.nan)
    )

    month_exit_pen = month_exit_pen[
        ["month", "monthly_avg_exit_penetration"]
    ]

    df = df.merge(month_exit_pen, on="month", how="left")

    for window_weeks in config["tendency_windows_weeks"]:
        window_days = window_weeks * 7

        exits_col = f"rolling_{window_weeks}wk_exits"
        arr_pax_col = f"rolling_{window_weeks}wk_arriving_pax"
        pen_col = f"rolling_{window_weeks}wk_exit_penetration"

        df[exits_col] = df["exits"].shift(1).rolling(window_days).sum()
        df[arr_pax_col] = df["arriving_pax"].shift(1).rolling(window_days).sum()

        df[pen_col] = (
            df[exits_col] / df[arr_pax_col].replace(0, np.nan)
        )

    for n_occurrences in config["same_weekday_occurrences"]:
        pen_col = f"same_weekday_last_{n_occurrences}_exit_penetration"
        df[pen_col] = np.nan

        for weekday in df["weekday"].dropna().unique():
            mask = df["weekday"].eq(weekday)
            sub = df.loc[mask].copy().sort_values("date")

            rolling_exits = sub["exits"].shift(1).rolling(n_occurrences).sum()
            rolling_arr_pax = sub["arriving_pax"].shift(1).rolling(n_occurrences).sum()

            df.loc[mask, pen_col] = (
                rolling_exits / rolling_arr_pax.replace(0, np.nan)
            )

    return df


def backtest_exits_arriving_passenger_penetration(exit_penetration_dataset, config):
    """
    Backtest exit forecasts using arriving passenger penetration methods.

    Returns
    -------
    pandas.DataFrame
        Daily backtest results for each penetration method.
    """

    df = exit_penetration_dataset.copy()

    penetration_cols = [
        "overall_avg_exit_penetration",
        "weekday_avg_exit_penetration",
        "monthly_avg_exit_penetration",
    ]

    for window_weeks in config["tendency_windows_weeks"]:
        penetration_cols.append(f"rolling_{window_weeks}wk_exit_penetration")

    for n_occurrences in config["same_weekday_occurrences"]:
        penetration_cols.append(f"same_weekday_last_{n_occurrences}_exit_penetration")

    results = []

    for pen_col in penetration_cols:
        if pen_col not in df.columns:
            continue

        temp = df[
            [
                "date",
                "weekday",
                "month",
                "arriving_pax",
                "exits",
                pen_col,
            ]
        ].copy()

        temp["method"] = f"arriving_pax_x_{pen_col}"
        temp["forecast_exits"] = temp["arriving_pax"] * temp[pen_col]
        temp["actual_exits"] = temp["exits"]

        temp["error"] = temp["forecast_exits"] - temp["actual_exits"]
        temp["absolute_error"] = temp["error"].abs()
        temp["percentage_error"] = (
            temp["error"] / temp["actual_exits"].replace(0, np.nan)
        )
        temp["absolute_percentage_error"] = temp["percentage_error"].abs()
        temp["squared_error"] = temp["error"] ** 2

        results.append(
            temp[
                [
                    "date",
                    "weekday",
                    "month",
                    "method",
                    "forecast_exits",
                    "actual_exits",
                    "error",
                    "absolute_error",
                    "percentage_error",
                    "absolute_percentage_error",
                    "squared_error",
                ]
            ]
        )

    if results:
        return pd.concat(results, ignore_index=True)

    return pd.DataFrame()


def create_expected_return_forecast_daily(master):
    """
    Create daily exit forecast using expected / advised return date only.

    Logic:
        ForecastExits[d] = count of valid bookings with ExpectedReturnDate on d.
        If ExpectedReturnDate is missing, fall back to booking exitDate.
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

    valid["date"] = valid["expected_return_ts_final"].dt.normalize()

    daily_forecast = (
        valid
        .dropna(subset=["expected_return_ts_final"])
        .groupby("date")
        .agg(forecast_exits=("bookingId", "nunique"))
        .reset_index()
    )

    daily_forecast["date"] = pd.to_datetime(daily_forecast["date"])
    daily_forecast["method"] = "expected_return_date_only"

    return daily_forecast


def create_expected_return_forecast_hourly(master):
    """
    Create hourly exit forecast using expected / advised return datetime only.
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

    valid["datetime_hour"] = valid["expected_return_ts_final"].dt.floor("h")

    hourly_forecast = (
        valid
        .dropna(subset=["expected_return_ts_final"])
        .groupby("datetime_hour")
        .agg(forecast_exits=("bookingId", "nunique"))
        .reset_index()
    )

    hourly_forecast["method"] = "expected_return_datetime_only"

    return hourly_forecast


def create_return_deviation_profile(duration_df):
    """
    Create a return deviation profile.

    Purpose:
        Estimate how actual checkout hour differs from expected return hour.

    Returns
    -------
    pandas.DataFrame
        Profile with:
            - expected_return_hour
            - actual_exit_hour_offset
            - probability
    """

    df = duration_df.copy()

    df = df.dropna(subset=["ExpectedReturnDate", "actual_exit_ts"]).copy()

    df["ExpectedReturnDate"] = pd.to_datetime(
        df["ExpectedReturnDate"],
        errors="coerce",
    )

    df["actual_exit_ts"] = pd.to_datetime(
        df["actual_exit_ts"],
        errors="coerce",
    )

    df["expected_return_hour_ts"] = df["ExpectedReturnDate"].dt.floor("h")
    df["actual_exit_hour_ts"] = df["actual_exit_ts"].dt.floor("h")

    df["actual_exit_hour_offset"] = (
        df["actual_exit_hour_ts"] - df["expected_return_hour_ts"]
    ).dt.total_seconds() / 3600

    df["expected_return_hour"] = df["ExpectedReturnDate"].dt.hour

    profile = (
        df
        .groupby(["expected_return_hour", "actual_exit_hour_offset"])
        .agg(records=("bookingId", "nunique"))
        .reset_index()
    )

    profile["total_records_for_expected_hour"] = (
        profile
        .groupby("expected_return_hour")["records"]
        .transform("sum")
    )

    profile["probability"] = (
        profile["records"]
        / profile["total_records_for_expected_hour"].replace(0, np.nan)
    )

    return profile


def apply_return_deviation_profile_to_hourly_forecast(
    hourly_expected_return_forecast,
    return_deviation_profile,
):
    """
    Apply historical early/late return adjustment to expected return forecast.

    Purpose:
        Convert expected/advised return hour into predicted actual checkout hour.
    """

    forecast = hourly_expected_return_forecast.copy()
    profile = return_deviation_profile.copy()

    if forecast.empty or profile.empty:
        return pd.DataFrame(
            columns=["datetime_hour", "forecast_exits", "method"]
        )

    forecast["datetime_hour"] = pd.to_datetime(
        forecast["datetime_hour"],
        errors="coerce",
    )

    forecast["expected_return_hour"] = forecast["datetime_hour"].dt.hour

    expanded = forecast.merge(
        profile,
        on="expected_return_hour",
        how="left",
    )

    # If there is no deviation profile for a given hour, keep the exits in the same hour.
    expanded["actual_exit_hour_offset"] = expanded["actual_exit_hour_offset"].fillna(0)
    expanded["probability"] = expanded["probability"].fillna(1)

    expanded["adjusted_datetime_hour"] = (
        expanded["datetime_hour"]
        + pd.to_timedelta(expanded["actual_exit_hour_offset"], unit="h")
    )

    expanded["adjusted_forecast_exits"] = (
        expanded["forecast_exits"] * expanded["probability"]
    )

    adjusted = (
        expanded
        .groupby("adjusted_datetime_hour")
        .agg(forecast_exits=("adjusted_forecast_exits", "sum"))
        .reset_index()
        .rename(columns={"adjusted_datetime_hour": "datetime_hour"})
    )

    adjusted["method"] = "expected_return_datetime_plus_deviation_adjustment"

    return adjusted


def create_duration_distribution(duration_df, duration_granularity="days"):
    """
    Create historical stay-duration probability distribution.

    Purpose:
        Use historical durations to convert entries into future exits.
    """

    df = duration_df.copy()
    df = df.dropna(subset=["actual_entry_ts", "actual_exit_ts"]).copy()

    df["actual_entry_ts"] = pd.to_datetime(
        df["actual_entry_ts"],
        errors="coerce",
    )

    df["actual_exit_ts"] = pd.to_datetime(
        df["actual_exit_ts"],
        errors="coerce",
    )

    if duration_granularity == "days":
        df["duration_offset"] = (
            df["actual_exit_ts"].dt.floor("D")
            - df["actual_entry_ts"].dt.floor("D")
        ).dt.days

    elif duration_granularity == "hours":
        df["duration_offset"] = (
            df["actual_exit_ts"].dt.floor("h")
            - df["actual_entry_ts"].dt.floor("h")
        ).dt.total_seconds() / 3600

    else:
        raise ValueError("duration_granularity must be either 'days' or 'hours'")

    df = df[df["duration_offset"].notna()].copy()
    df = df[df["duration_offset"] >= 0].copy()

    distribution = (
        df
        .groupby("duration_offset")
        .agg(records=("bookingId", "nunique"))
        .reset_index()
    )

    distribution["probability"] = (
        distribution["records"]
        / distribution["records"].sum()
    )

    return distribution


def create_duration_based_exit_forecast_daily(daily_entries, duration_distribution):
    """
    Create daily exit forecast from entry cohorts and duration distribution.

    Formula:
        ForecastExits[d + k] += Entries[d] * P(Duration = k)
    """

    entries = daily_entries.copy()
    dist = duration_distribution.copy()

    entries["date"] = pd.to_datetime(entries["date"])

    rows = []

    for _, entry_row in entries.iterrows():
        entry_date = pd.Timestamp(entry_row["date"])
        entry_count = entry_row["entries"]

        for _, dur_row in dist.iterrows():
            duration_days = int(dur_row["duration_offset"])
            probability = dur_row["probability"]

            rows.append(
                {
                    "date": entry_date + pd.Timedelta(days=duration_days),
                    "forecast_exits": entry_count * probability,
                }
            )

    forecast = pd.DataFrame(rows)

    if forecast.empty:
        return pd.DataFrame(columns=["date", "forecast_exits", "method"])

    forecast = (
        forecast
        .groupby("date")
        .agg(forecast_exits=("forecast_exits", "sum"))
        .reset_index()
    )

    forecast["method"] = "entry_cohort_duration_distribution_daily"

    return forecast


def create_duration_based_exit_forecast_hourly(hourly_entries, duration_distribution):
    """
    Create hourly exit forecast from hourly entry cohorts and duration distribution.

    Formula:
        ForecastExits[t + k] += Entries[t] * P(DurationHours = k)
    """

    entries = hourly_entries.copy()
    dist = duration_distribution.copy()

    entries["datetime_hour"] = pd.to_datetime(entries["datetime_hour"])

    rows = []

    for _, entry_row in entries.iterrows():
        entry_hour = pd.Timestamp(entry_row["datetime_hour"])
        entry_count = entry_row["entries"]

        for _, dur_row in dist.iterrows():
            duration_hours = int(dur_row["duration_offset"])
            probability = dur_row["probability"]

            rows.append(
                {
                    "datetime_hour": entry_hour + pd.Timedelta(hours=duration_hours),
                    "forecast_exits": entry_count * probability,
                }
            )

    forecast = pd.DataFrame(rows)

    if forecast.empty:
        return pd.DataFrame(columns=["datetime_hour", "forecast_exits", "method"])

    forecast = (
        forecast
        .groupby("datetime_hour")
        .agg(forecast_exits=("forecast_exits", "sum"))
        .reset_index()
    )

    forecast["method"] = "entry_cohort_duration_distribution_hourly"

    return forecast


def create_hybrid_exit_forecast_daily(
    expected_return_forecast,
    duration_based_forecast,
    known_return_weight=0.75,
):
    """
    Create a simple hybrid daily exit forecast.

    Purpose:
        Combine:
            - expected/advised returns
            - duration-based implied exits
    """

    expected = expected_return_forecast.copy()
    duration = duration_based_forecast.copy()

    expected = expected.rename(
        columns={"forecast_exits": "expected_return_forecast_exits"}
    )

    duration = duration.rename(
        columns={"forecast_exits": "duration_forecast_exits"}
    )

    hybrid = expected[["date", "expected_return_forecast_exits"]].merge(
        duration[["date", "duration_forecast_exits"]],
        on="date",
        how="outer",
    ).fillna(0)

    hybrid["forecast_exits"] = (
        known_return_weight * hybrid["expected_return_forecast_exits"]
        + (1 - known_return_weight) * hybrid["duration_forecast_exits"]
    )

    hybrid["method"] = (
        f"hybrid_expected_return_{known_return_weight}"
        f"_duration_{1 - known_return_weight}"
    )

    return hybrid


def create_hybrid_exit_forecast_hourly(
    expected_return_forecast,
    duration_based_forecast,
    known_return_weight=0.75,
):
    """
    Create a simple hybrid hourly exit forecast.
    """

    expected = expected_return_forecast.copy()
    duration = duration_based_forecast.copy()

    expected = expected.rename(
        columns={"forecast_exits": "expected_return_forecast_exits"}
    )

    duration = duration.rename(
        columns={"forecast_exits": "duration_forecast_exits"}
    )

    hybrid = expected[["datetime_hour", "expected_return_forecast_exits"]].merge(
        duration[["datetime_hour", "duration_forecast_exits"]],
        on="datetime_hour",
        how="outer",
    ).fillna(0)

    hybrid["forecast_exits"] = (
        known_return_weight * hybrid["expected_return_forecast_exits"]
        + (1 - known_return_weight) * hybrid["duration_forecast_exits"]
    )

    hybrid["method"] = (
        f"hybrid_expected_return_{known_return_weight}"
        f"_duration_{1 - known_return_weight}"
    )

    return hybrid


def evaluate_daily_exit_forecast(forecast_df, daily_fastpark_actuals):
    """
    Evaluate a daily exit forecast against actual daily exits.
    """

    if forecast_df is None or forecast_df.empty:
        return pd.DataFrame()

    forecast = forecast_df.copy()
    actuals = daily_fastpark_actuals[["date", "exits"]].copy()

    forecast["date"] = pd.to_datetime(forecast["date"])
    actuals["date"] = pd.to_datetime(actuals["date"])

    eval_df = forecast.merge(actuals, on="date", how="left")

    eval_df["actual_exits"] = eval_df["exits"].fillna(0)

    eval_df["error"] = eval_df["forecast_exits"] - eval_df["actual_exits"]
    eval_df["absolute_error"] = eval_df["error"].abs()
    eval_df["percentage_error"] = (
        eval_df["error"] / eval_df["actual_exits"].replace(0, np.nan)
    )
    eval_df["absolute_percentage_error"] = eval_df["percentage_error"].abs()
    eval_df["squared_error"] = eval_df["error"] ** 2

    eval_df["weekday"] = eval_df["date"].dt.day_name()
    eval_df["month"] = eval_df["date"].dt.month

    return eval_df


def evaluate_hourly_exit_forecast(forecast_df, hourly_fastpark_actuals):
    """
    Evaluate an hourly exit forecast against actual hourly exits.
    """

    if forecast_df is None or forecast_df.empty:
        return pd.DataFrame()

    forecast = forecast_df.copy()
    actuals = hourly_fastpark_actuals[["datetime_hour", "exits"]].copy()

    forecast["datetime_hour"] = pd.to_datetime(forecast["datetime_hour"])
    actuals["datetime_hour"] = pd.to_datetime(actuals["datetime_hour"])

    eval_df = forecast.merge(actuals, on="datetime_hour", how="left")

    eval_df["actual_exits"] = eval_df["exits"].fillna(0)

    eval_df["error"] = eval_df["forecast_exits"] - eval_df["actual_exits"]
    eval_df["absolute_error"] = eval_df["error"].abs()
    eval_df["percentage_error"] = (
        eval_df["error"] / eval_df["actual_exits"].replace(0, np.nan)
    )
    eval_df["absolute_percentage_error"] = eval_df["percentage_error"].abs()
    eval_df["squared_error"] = eval_df["error"] ** 2

    eval_df["date"] = eval_df["datetime_hour"].dt.date
    eval_df["hour"] = eval_df["datetime_hour"].dt.hour
    eval_df["weekday"] = eval_df["datetime_hour"].dt.day_name()
    eval_df["month"] = eval_df["datetime_hour"].dt.month

    return eval_df


def summarise_exit_forecast_performance(exit_forecast_evaluation):
    """
    Summarise exit forecast performance by method.
    """

    if exit_forecast_evaluation is None or exit_forecast_evaluation.empty:
        return {
            "overall_performance": pd.DataFrame(),
            "performance_by_weekday": pd.DataFrame(),
            "performance_by_month": pd.DataFrame(),
            "performance_by_hour": pd.DataFrame(),
        }

    df = exit_forecast_evaluation.copy()

    overall = (
        df
        .groupby("method")
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            rmse=("squared_error", lambda x: (x.mean()) ** 0.5),
            total_forecast_exits=("forecast_exits", "sum"),
            total_actual_exits=("actual_exits", "sum"),
            records=("actual_exits", "count"),
        )
        .reset_index()
    )

    by_weekday = (
        df
        .groupby(["method", "weekday"])
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            records=("actual_exits", "count"),
        )
        .reset_index()
    )

    by_month = (
        df
        .groupby(["method", "month"])
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            records=("actual_exits", "count"),
        )
        .reset_index()
    )

    if "hour" in df.columns:
        by_hour = (
            df
            .groupby(["method", "hour"])
            .agg(
                mae=("absolute_error", "mean"),
                bias=("error", "mean"),
                mape=("absolute_percentage_error", "mean"),
                records=("actual_exits", "count"),
            )
            .reset_index()
        )
    else:
        by_hour = pd.DataFrame()

    return {
        "overall_performance": overall,
        "performance_by_weekday": by_weekday,
        "performance_by_month": by_month,
        "performance_by_hour": by_hour,
    }


def run_exit_forecast_method_comparison(
    master,
    daily_driver_dataset,
    daily_fastpark_actuals,
    hourly_fastpark_actuals,
    duration_df,
    config,
):
    """
    Run the full exit forecast method comparison.

    Compares:
        A. arriving passenger penetration
        B. expected/advised return date only
        C. expected return date + early/late adjustment
        D. entry cohort + duration distribution
        E. hybrid expected return + duration distribution

    Returns
    -------
    dict
        Exit forecast comparison outputs.
    """

    # --------------------------------------------------------
    # Method A: Arriving passenger penetration
    # --------------------------------------------------------
    exit_penetration_dataset = calculate_exit_penetration_baselines(
        daily_driver_dataset=daily_driver_dataset,
        config=config,
    )

    arriving_pax_eval_daily = backtest_exits_arriving_passenger_penetration(
        exit_penetration_dataset=exit_penetration_dataset,
        config=config,
    )

    # --------------------------------------------------------
    # Method B: Expected/advised return date only
    # --------------------------------------------------------
    expected_return_forecast_daily = create_expected_return_forecast_daily(master)
    expected_return_forecast_hourly = create_expected_return_forecast_hourly(master)

    expected_return_eval_daily = evaluate_daily_exit_forecast(
        forecast_df=expected_return_forecast_daily,
        daily_fastpark_actuals=daily_fastpark_actuals,
    )

    expected_return_eval_hourly = evaluate_hourly_exit_forecast(
        forecast_df=expected_return_forecast_hourly,
        hourly_fastpark_actuals=hourly_fastpark_actuals,
    )

    # --------------------------------------------------------
    # Method C: Expected return + return deviation adjustment
    # --------------------------------------------------------
    return_deviation_profile = create_return_deviation_profile(duration_df)

    adjusted_expected_return_forecast_hourly = (
        apply_return_deviation_profile_to_hourly_forecast(
            hourly_expected_return_forecast=expected_return_forecast_hourly,
            return_deviation_profile=return_deviation_profile,
        )
    )

    adjusted_expected_return_eval_hourly = evaluate_hourly_exit_forecast(
        forecast_df=adjusted_expected_return_forecast_hourly,
        hourly_fastpark_actuals=hourly_fastpark_actuals,
    )

    if not adjusted_expected_return_forecast_hourly.empty:
        adjusted_expected_return_forecast_daily = (
            adjusted_expected_return_forecast_hourly
            .assign(date=lambda x: pd.to_datetime(x["datetime_hour"]).dt.date)
            .groupby("date")
            .agg(forecast_exits=("forecast_exits", "sum"))
            .reset_index()
        )

        adjusted_expected_return_forecast_daily["date"] = pd.to_datetime(
            adjusted_expected_return_forecast_daily["date"]
        )

        adjusted_expected_return_forecast_daily["method"] = (
            "expected_return_plus_deviation_adjustment_daily"
        )
    else:
        adjusted_expected_return_forecast_daily = pd.DataFrame(
            columns=["date", "forecast_exits", "method"]
        )

    adjusted_expected_return_eval_daily = evaluate_daily_exit_forecast(
        forecast_df=adjusted_expected_return_forecast_daily,
        daily_fastpark_actuals=daily_fastpark_actuals,
    )

    # --------------------------------------------------------
    # Method D: Entry cohort + duration distribution
    # --------------------------------------------------------
    duration_distribution_daily = create_duration_distribution(
        duration_df=duration_df,
        duration_granularity="days",
    )

    duration_distribution_hourly = create_duration_distribution(
        duration_df=duration_df,
        duration_granularity="hours",
    )

    daily_entries = daily_fastpark_actuals[["date", "entries"]].copy()
    hourly_entries = hourly_fastpark_actuals[["datetime_hour", "entries"]].copy()

    duration_based_forecast_daily = create_duration_based_exit_forecast_daily(
        daily_entries=daily_entries,
        duration_distribution=duration_distribution_daily,
    )

    duration_based_forecast_hourly = create_duration_based_exit_forecast_hourly(
        hourly_entries=hourly_entries,
        duration_distribution=duration_distribution_hourly,
    )

    duration_based_eval_daily = evaluate_daily_exit_forecast(
        forecast_df=duration_based_forecast_daily,
        daily_fastpark_actuals=daily_fastpark_actuals,
    )

    duration_based_eval_hourly = evaluate_hourly_exit_forecast(
        forecast_df=duration_based_forecast_hourly,
        hourly_fastpark_actuals=hourly_fastpark_actuals,
    )

    # --------------------------------------------------------
    # Method E: Hybrid expected return + duration distribution
    # --------------------------------------------------------
    hybrid_forecast_daily = create_hybrid_exit_forecast_daily(
        expected_return_forecast=expected_return_forecast_daily,
        duration_based_forecast=duration_based_forecast_daily,
        known_return_weight=0.75,
    )

    hybrid_forecast_hourly = create_hybrid_exit_forecast_hourly(
        expected_return_forecast=expected_return_forecast_hourly,
        duration_based_forecast=duration_based_forecast_hourly,
        known_return_weight=0.75,
    )

    hybrid_eval_daily = evaluate_daily_exit_forecast(
        forecast_df=hybrid_forecast_daily,
        daily_fastpark_actuals=daily_fastpark_actuals,
    )

    hybrid_eval_hourly = evaluate_hourly_exit_forecast(
        forecast_df=hybrid_forecast_hourly,
        hourly_fastpark_actuals=hourly_fastpark_actuals,
    )

    # --------------------------------------------------------
    # Combine evaluations
    # --------------------------------------------------------
    daily_parts = [
        arriving_pax_eval_daily,
        expected_return_eval_daily,
        adjusted_expected_return_eval_daily,
        duration_based_eval_daily,
        hybrid_eval_daily,
    ]

    daily_parts = [x for x in daily_parts if x is not None and not x.empty]

    if daily_parts:
        daily_evaluations = pd.concat(daily_parts, ignore_index=True)
    else:
        daily_evaluations = pd.DataFrame()

    hourly_parts = [
        expected_return_eval_hourly,
        adjusted_expected_return_eval_hourly,
        duration_based_eval_hourly,
        hybrid_eval_hourly,
    ]

    hourly_parts = [x for x in hourly_parts if x is not None and not x.empty]

    if hourly_parts:
        hourly_evaluations = pd.concat(hourly_parts, ignore_index=True)
    else:
        hourly_evaluations = pd.DataFrame()

    # --------------------------------------------------------
    # Summarise performance
    # --------------------------------------------------------
    daily_performance_summary = summarise_exit_forecast_performance(
        daily_evaluations
    )

    hourly_performance_summary = summarise_exit_forecast_performance(
        hourly_evaluations
    )

    # --------------------------------------------------------
    # Package outputs
    # --------------------------------------------------------
    outputs = {
        "exit_penetration_dataset": exit_penetration_dataset,

        "arriving_pax_eval_daily": arriving_pax_eval_daily,

        "expected_return_forecast_daily": expected_return_forecast_daily,
        "expected_return_forecast_hourly": expected_return_forecast_hourly,
        "expected_return_eval_daily": expected_return_eval_daily,
        "expected_return_eval_hourly": expected_return_eval_hourly,

        "return_deviation_profile": return_deviation_profile,
        "adjusted_expected_return_forecast_daily": adjusted_expected_return_forecast_daily,
        "adjusted_expected_return_forecast_hourly": adjusted_expected_return_forecast_hourly,
        "adjusted_expected_return_eval_daily": adjusted_expected_return_eval_daily,
        "adjusted_expected_return_eval_hourly": adjusted_expected_return_eval_hourly,

        "duration_distribution_daily": duration_distribution_daily,
        "duration_distribution_hourly": duration_distribution_hourly,
        "duration_based_forecast_daily": duration_based_forecast_daily,
        "duration_based_forecast_hourly": duration_based_forecast_hourly,
        "duration_based_eval_daily": duration_based_eval_daily,
        "duration_based_eval_hourly": duration_based_eval_hourly,

        "hybrid_forecast_daily": hybrid_forecast_daily,
        "hybrid_forecast_hourly": hybrid_forecast_hourly,
        "hybrid_eval_daily": hybrid_eval_daily,
        "hybrid_eval_hourly": hybrid_eval_hourly,

        "daily_exit_forecast_evaluations": daily_evaluations,
        "hourly_exit_forecast_evaluations": hourly_evaluations,

        "daily_exit_performance_summary": daily_performance_summary,
        "hourly_exit_performance_summary": hourly_performance_summary,
    }

    return outputs

# ============================================================
# 11. TENDENCY / ROLLING PENETRATION ANALYSIS
# ============================================================

def calculate_rolling_entry_penetration(daily_driver_dataset, window_weeks):
    """
    Calculate rolling entry penetration over a chosen number of weeks.

    Entry penetration:
        entries / departing passengers

    Important:
        Use only historical data available before the forecast date.
        Do not leak future actuals into the rolling calculation.

    Parameters:
        daily_driver_dataset: pandas.DataFrame
            Daily fastpark and passenger dataset.

        window_weeks: int
            Number of weeks to include in rolling tendency window.

    Returns:
        pandas.DataFrame:
            Dataset with rolling entry penetration.
    """

    df = daily_driver_dataset.sort_values("date").copy()
    window_days = window_weeks * 7
    
    # Use shifted rolling sums to prevent leakage:
    df[f"rolling_{window_weeks}wk_entries"] = (
        df["entries"].shift(1).rolling(window_days).sum()
    )
    df[f"rolling_{window_weeks}wk_departing_pax"] = (
        df["departing_pax"].shift(1).rolling(window_days).sum()
    )
    df[f"rolling_{window_weeks}wk_entry_penetration"] = (
        df[f"rolling_{window_weeks}wk_entries"]
        / df[f"rolling_{window_weeks}wk_departing_pax"].replace(0, np.nan)
    )


    return df


def calculate_same_weekday_entry_penetration(daily_driver_dataset, n_occurrences):
    """
    Calculate same-weekday rolling entry penetration.

    Example:
        To forecast a Friday, use the last 4 Fridays or last 8 Fridays.

    Purpose:
        Avoid Monday behaviour contaminating Friday behaviour.

    Parameters:
        daily_driver_dataset: pandas.DataFrame
            Daily fastpark and passenger dataset.

        n_occurrences: int
            Number of previous same-weekday observations to use.

    Returns:
        pandas.DataFrame:
            Dataset with same-weekday rolling penetration.
    """


    df = daily_driver_dataset.sort_values("date").copy()

    pen_col = f"same_weekday_last_{n_occurrences}_entry_penetration"
    entries_col = f"same_weekday_last_{n_occurrences}_entries"
    pax_col = f"same_weekday_last_{n_occurrences}_departing_pax"

    df[pen_col] = np.nan
    df[entries_col] = np.nan
    df[pax_col] = np.nan

    for weekday in df["weekday"].dropna().unique():
        mask = df["weekday"].eq(weekday)
        sub = df.loc[mask].copy().sort_values("date")

        rolling_entries = sub["entries"].shift(1).rolling(n_occurrences).sum()
        rolling_pax = sub["departing_pax"].shift(1).rolling(n_occurrences).sum()

        df.loc[mask, entries_col] = rolling_entries
        df.loc[mask, pax_col] = rolling_pax
        df.loc[mask, pen_col] = rolling_entries / rolling_pax.replace(0, np.nan)

    return df




def run_tendency_window_backtest(daily_driver_dataset, config):
    """
    Back-test multiple tendency windows.

    Purpose:
        Decide whether 2, 4, 6, 8, 13 weeks, same-weekday rolling,
        or another method gives the best forecast.

    Parameters:
        daily_driver_dataset: pandas.DataFrame
            Daily fastpark and passenger context.

        config: dict
            Configuration dictionary.

    Returns:
        pandas.DataFrame:
            Back-test results with:
                - forecast_date
                - target_date
                - method
                - forecast_entries
                - actual_entries
                - error
                - absolute_error
                - percentage_error
                - weekday
                - month
                - period_type if available
    """

    results = []

    base = daily_driver_dataset.copy()
    base["date"] = pd.to_datetime(base["date"])

    for window in config["tendency_windows_weeks"]:
        df_window = calculate_rolling_entry_penetration(base, window)

        pen_col = f"rolling_{window}wk_entry_penetration"

        temp = df_window.copy()
        temp["method"] = f"rolling_{window}wk_entry_penetration"
        temp["forecast_entries"] = temp["departing_pax"] * temp[pen_col]
        temp["actual_entries"] = temp["entries"]

        temp["error"] = temp["forecast_entries"] - temp["actual_entries"]
        temp["absolute_error"] = temp["error"].abs()
        temp["percentage_error"] = (
            temp["error"] / temp["actual_entries"].replace(0, np.nan)
        )
        temp["absolute_percentage_error"] = temp["percentage_error"].abs()
        temp["squared_error"] = temp["error"] ** 2

        results.append(
            temp[
                [
                    "date",
                    "weekday",
                    "month",
                    "method",
                    "forecast_entries",
                    "actual_entries",
                    "error",
                    "absolute_error",
                    "percentage_error",
                    "absolute_percentage_error",
                    "squared_error",
                ]
            ]
        )

    for n in config["same_weekday_occurrences"]:
        df_same = calculate_same_weekday_entry_penetration(base, n)

        pen_col = f"same_weekday_last_{n}_entry_penetration"

        temp = df_same.copy()
        temp["method"] = f"same_weekday_last_{n}_entry_penetration"
        temp["forecast_entries"] = temp["departing_pax"] * temp[pen_col]
        temp["actual_entries"] = temp["entries"]

        temp["error"] = temp["forecast_entries"] - temp["actual_entries"]
        temp["absolute_error"] = temp["error"].abs()
        temp["percentage_error"] = (
            temp["error"] / temp["actual_entries"].replace(0, np.nan)
        )
        temp["absolute_percentage_error"] = temp["percentage_error"].abs()
        temp["squared_error"] = temp["error"] ** 2

        results.append(
            temp[
                [
                    "date",
                    "weekday",
                    "month",
                    "method",
                    "forecast_entries",
                    "actual_entries",
                    "error",
                    "absolute_error",
                    "percentage_error",
                    "absolute_percentage_error",
                    "squared_error",
                ]
            ]
        )

    if results:
        return pd.concat(results, ignore_index=True)

    return pd.DataFrame()


def summarise_backtest_results(backtest_results):
    """
    Summarise tendency back-test performance.

    Purpose:
        Identify which tendency method performs best overall and by segment.

    Parameters:
        backtest_results: pandas.DataFrame
            Output from run_tendency_window_backtest().

    Returns:
        dict:
            Summary tables:
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
        .groupby("method")
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            rmse=("squared_error", lambda x: (x.mean()) ** 0.5),
            total_forecast_entries=("forecast_entries", "sum"),
            total_actual_entries=("actual_entries", "sum"),
            records=("actual_entries", "count"),
        )
        .reset_index()
    )

    performance_by_weekday = (
        df
        .groupby(["method", "weekday"])
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            records=("actual_entries", "count"),
        )
        .reset_index()
    )

    performance_by_month = (
        df
        .groupby(["method", "month"])
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            records=("actual_entries", "count"),
        )
        .reset_index()
    )

    return {
        "overall_method_performance": overall_method_performance,
        "performance_by_weekday": performance_by_weekday,
        "performance_by_month": performance_by_month,
    }

# ============================================================
# 12. ENTRY FORECAST METHOD COMPARISON
# ============================================================

def forecast_entries_booking_curve(
    booking_curve,
    completion_basis_col="completion_vs_actual_entries",
):
    """
    Forecast final FastPark entries using booking curve completion rates.

    Purpose:
        Estimate final entries from known bookings at each lead time checkpoint.

    Method:
        For each lead time, calculate the historical completion rate excluding
        the current entry date, then forecast:

            forecast_entries = bookings_known / historical_completion_rate

    Why exclude the current row:
        This avoids directly using the target day's own completion rate, which
        would make the comparison artificially perfect.

    Parameters
    ----------
    booking_curve : pandas.DataFrame
        Booking curve dataset from create_booking_curve_dataset().

    completion_basis_col : str
        Which completion rate column to use:
            - completion_vs_actual_entries
            - completion_vs_valid_bookings

    Returns
    -------
    pandas.DataFrame
        Booking curve forecast evaluation-ready dataframe.
    """

    df = booking_curve.copy()

    df["entry_date"] = pd.to_datetime(df["entry_date"])
    df["date"] = df["entry_date"]

    # Choose final actual target depending on basis.
    if completion_basis_col == "completion_vs_actual_entries":
        actual_col = "final_actual_entries"
    elif completion_basis_col == "completion_vs_valid_bookings":
        actual_col = "final_valid_bookings"
    else:
        raise ValueError(
            "completion_basis_col must be 'completion_vs_actual_entries' "
            "or 'completion_vs_valid_bookings'"
        )

    # Aggregate by lead time so we can create an excluding-current-row completion rate.
    lead_totals = (
        df
        .groupby("lead_time_checkpoint")
        .agg(
            total_bookings_known=("bookings_known", "sum"),
            total_actual=("final_actual_entries", "sum"),
            total_valid=("final_valid_bookings", "sum"),
        )
        .reset_index()
    )

    df = df.merge(
        lead_totals,
        on="lead_time_checkpoint",
        how="left",
    )

    if completion_basis_col == "completion_vs_actual_entries":
        df["completion_denominator_excl_current"] = (
            df["total_actual"] - df["final_actual_entries"]
        )
    else:
        df["completion_denominator_excl_current"] = (
            df["total_valid"] - df["final_valid_bookings"]
        )

    df["bookings_known_excl_current"] = (
        df["total_bookings_known"] - df["bookings_known"]
    )

    df["historical_completion_rate_excl_current"] = (
        df["bookings_known_excl_current"]
        / df["completion_denominator_excl_current"].replace(0, np.nan)
    )

    df["forecast_entries"] = (
        df["bookings_known"]
        / df["historical_completion_rate_excl_current"].replace(0, np.nan)
    )

    df["actual_entries"] = df[actual_col]

    df["method"] = f"booking_curve_{completion_basis_col}_lead_only"

    return df[
        [
            "date",
            "weekday",
            "month",
            "lead_time_checkpoint",
            "method",
            "bookings_known",
            "historical_completion_rate_excl_current",
            "forecast_entries",
            "actual_entries",
        ]
    ]


def select_best_tendency_method(tendency_backtest_results):
    """
    Select the best tendency method based on lowest MAE.

    Parameters
    ----------
    tendency_backtest_results : pandas.DataFrame
        Output from run_tendency_window_backtest().

    Returns
    -------
    str
        Best method name.
    """

    if tendency_backtest_results is None or tendency_backtest_results.empty:
        return None

    method_summary = (
        tendency_backtest_results
        .groupby("method")
        .agg(mae=("absolute_error", "mean"))
        .reset_index()
        .sort_values("mae")
    )

    if method_summary.empty:
        return None

    return method_summary.iloc[0]["method"]


def forecast_entries_from_tendency(
    tendency_backtest_results,
    method=None,
):
    """
    Extract entry forecasts from the tendency backtest results.

    Purpose:
        Use the rolling/same-weekday tendency method as the passenger/tendency
        based forecast.

    Parameters
    ----------
    tendency_backtest_results : pandas.DataFrame
        Output from run_tendency_window_backtest().

    method : str, optional
        Specific method to use. If None, the method with lowest MAE is selected.

    Returns
    -------
    pandas.DataFrame
        Tendency forecast dataframe.
    """

    if tendency_backtest_results is None or tendency_backtest_results.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "weekday",
                "month",
                "method",
                "forecast_entries",
                "actual_entries",
            ]
        )

    selected_method = method or select_best_tendency_method(tendency_backtest_results)

    if selected_method is None:
        return pd.DataFrame(
            columns=[
                "date",
                "weekday",
                "month",
                "method",
                "forecast_entries",
                "actual_entries",
            ]
        )

    df = tendency_backtest_results[
        tendency_backtest_results["method"].eq(selected_method)
    ].copy()

    df["method"] = f"tendency_{selected_method}"

    return df[
        [
            "date",
            "weekday",
            "month",
            "method",
            "forecast_entries",
            "actual_entries",
        ]
    ]


def forecast_entries_hybrid(
    booking_curve_forecast,
    tendency_forecast,
):
    """
    Create hybrid entry forecast.

    Concept:
        Close to entry date, rely more heavily on booking curve.
        Further away, rely more heavily on tendency/passenger forecast.

    Weighting rule:
        lead time <= 3 days   -> 90% booking curve
        lead time <= 7 days   -> 75% booking curve
        lead time <= 14 days  -> 50% booking curve
        lead time <= 28 days  -> 30% booking curve
        lead time > 28 days   -> 10% booking curve

    Parameters
    ----------
    booking_curve_forecast : pandas.DataFrame
        Output from forecast_entries_booking_curve().

    tendency_forecast : pandas.DataFrame
        Output from forecast_entries_from_tendency().

    Returns
    -------
    pandas.DataFrame
        Hybrid forecast dataframe.
    """

    booking = booking_curve_forecast.copy()
    tendency = tendency_forecast.copy()

    if booking.empty or tendency.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "weekday",
                "month",
                "lead_time_checkpoint",
                "method",
                "booking_weight",
                "bookings_known",
                "booking_curve_forecast_entries",
                "tendency_forecast_entries",
                "forecast_entries",
                "actual_entries",
            ]
        )

    booking["date"] = pd.to_datetime(booking["date"])
    tendency["date"] = pd.to_datetime(tendency["date"])

    tendency = tendency.rename(
        columns={
            "forecast_entries": "tendency_forecast_entries",
            "actual_entries": "actual_entries_tendency",
        }
    )

    booking = booking.rename(
        columns={
            "forecast_entries": "booking_curve_forecast_entries",
            "actual_entries": "actual_entries_booking",
        }
    )

    hybrid = booking.merge(
        tendency[
            [
                "date",
                "tendency_forecast_entries",
                "actual_entries_tendency",
            ]
        ],
        on="date",
        how="left",
    )

    def get_booking_weight(lead_time):
        if lead_time <= 3:
            return 0.90
        if lead_time <= 7:
            return 0.75
        if lead_time <= 14:
            return 0.50
        if lead_time <= 28:
            return 0.30
        return 0.10

    hybrid["booking_weight"] = hybrid["lead_time_checkpoint"].apply(get_booking_weight)

    # Fill missing forecast components so the hybrid does not collapse to NaN.
    # If tendency is missing, fall back to booking curve.
    # If booking curve is missing, fall back to tendency.
    hybrid["tendency_forecast_entries"] = hybrid["tendency_forecast_entries"].fillna(
        hybrid["booking_curve_forecast_entries"]
    )

    hybrid["booking_curve_forecast_entries"] = hybrid["booking_curve_forecast_entries"].fillna(
        hybrid["tendency_forecast_entries"]
    )

    hybrid["forecast_entries"] = (
        hybrid["booking_weight"] * hybrid["booking_curve_forecast_entries"]
        + (1 - hybrid["booking_weight"]) * hybrid["tendency_forecast_entries"]
    )

    hybrid["actual_entries"] = hybrid["actual_entries_booking"]

    hybrid["method"] = "hybrid_booking_curve_plus_tendency"

    return hybrid[
        [
            "date",
            "weekday",
            "month",
            "lead_time_checkpoint",
            "method",
            "booking_weight",
            "bookings_known",
            "booking_curve_forecast_entries",
            "tendency_forecast_entries",
            "forecast_entries",
            "actual_entries",
        ]
    ]


def evaluate_entry_forecast(entry_forecast):
    """
    Add forecast error columns to an entry forecast dataframe.

    Parameters
    ----------
    entry_forecast : pandas.DataFrame
        Forecast dataframe containing:
            - forecast_entries
            - actual_entries

    Returns
    -------
    pandas.DataFrame
        Forecast dataframe with error columns.
    """

    if entry_forecast is None or entry_forecast.empty:
        return pd.DataFrame()

    df = entry_forecast.copy()

    df["error"] = df["forecast_entries"] - df["actual_entries"]
    df["absolute_error"] = df["error"].abs()
    df["percentage_error"] = (
        df["error"] / df["actual_entries"].replace(0, np.nan)
    )
    df["absolute_percentage_error"] = df["percentage_error"].abs()
    df["squared_error"] = df["error"] ** 2

    return df


def summarise_entry_forecast_performance(entry_evaluation):
    """
    Summarise entry forecast performance.

    Parameters
    ----------
    entry_evaluation : pandas.DataFrame
        Evaluation dataframe from evaluate_entry_forecast().

    Returns
    -------
    dict
        Performance summaries.
    """

    if entry_evaluation is None or entry_evaluation.empty:
        return {
            "overall_performance": pd.DataFrame(),
            "performance_by_weekday": pd.DataFrame(),
            "performance_by_month": pd.DataFrame(),
            "performance_by_lead_time": pd.DataFrame(),
        }

    df = entry_evaluation.copy()

    overall = (
        df
        .groupby("method")
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            rmse=("squared_error", lambda x: (x.mean()) ** 0.5),
            total_forecast_entries=("forecast_entries", "sum"),
            total_actual_entries=("actual_entries", "sum"),
            records=("actual_entries", "count"),
        )
        .reset_index()
    )

    by_weekday = (
        df
        .groupby(["method", "weekday"])
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            records=("actual_entries", "count"),
        )
        .reset_index()
    )

    by_month = (
        df
        .groupby(["method", "month"])
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            records=("actual_entries", "count"),
        )
        .reset_index()
    )

    if "lead_time_checkpoint" in df.columns:
        by_lead_time = (
            df
            .groupby(["method", "lead_time_checkpoint"])
            .agg(
                mae=("absolute_error", "mean"),
                bias=("error", "mean"),
                mape=("absolute_percentage_error", "mean"),
                records=("actual_entries", "count"),
            )
            .reset_index()
        )
    else:
        by_lead_time = pd.DataFrame()

    return {
        "overall_performance": overall,
        "performance_by_weekday": by_weekday,
        "performance_by_month": by_month,
        "performance_by_lead_time": by_lead_time,
    }


def run_entry_forecast_method_comparison(
    booking_curve,
    tendency_backtest_results,
):
    """
    Run entry forecast method comparison.

    Methods compared:
        A. best tendency method from Section 11
        B. booking curve forecast from Section 9
        C. hybrid booking curve + best tendency method

    Parameters
    ----------
    booking_curve : pandas.DataFrame
        Output from create_booking_curve_dataset().

    tendency_backtest_results : pandas.DataFrame
        Output from run_tendency_window_backtest().

    Returns
    -------
    dict
        Entry forecast comparison outputs.
    """

    tendency_forecast = forecast_entries_from_tendency(
        tendency_backtest_results=tendency_backtest_results,
        method=None,
    )

    booking_curve_forecast = forecast_entries_booking_curve(
        booking_curve=booking_curve,
        completion_basis_col="completion_vs_actual_entries",
    )

    hybrid_forecast = forecast_entries_hybrid(
        booking_curve_forecast=booking_curve_forecast,
        tendency_forecast=tendency_forecast,
    )

    tendency_eval = evaluate_entry_forecast(tendency_forecast)
    booking_curve_eval = evaluate_entry_forecast(booking_curve_forecast)
    hybrid_eval = evaluate_entry_forecast(hybrid_forecast)

    eval_parts = [
        tendency_eval,
        booking_curve_eval,
        hybrid_eval,
    ]

    eval_parts = [x for x in eval_parts if x is not None and not x.empty]

    if eval_parts:
        all_evaluations = pd.concat(eval_parts, ignore_index=True)
    else:
        all_evaluations = pd.DataFrame()

    performance_summary = summarise_entry_forecast_performance(all_evaluations)

    return {
        "tendency_forecast": tendency_forecast,
        "booking_curve_forecast": booking_curve_forecast,
        "hybrid_forecast": hybrid_forecast,
        "entry_forecast_evaluations": all_evaluations,
        "entry_forecast_performance_summary": performance_summary,
    }




# ============================================================
# 13. FORECAST EVALUATION
# ============================================================

def calculate_forecast_errors(
    forecast_df,
    actual_df,
    target_col,
    forecast_col,
    date_cols,
    method_col="method",
):
    """
    Calculate standard forecast error metrics.

    Parameters
    ----------
    forecast_df : pandas.DataFrame
        Forecast results.

    actual_df : pandas.DataFrame
        Actual demand results.

    target_col : str
        Name of actual column, e.g. "entries" or "exits".

    forecast_col : str
        Name of forecast column.

    date_cols : list
        Columns to join on, e.g. ["date"] or ["datetime_hour"].

    method_col : str
        Forecast method column.

    Returns
    -------
    pandas.DataFrame
        Forecast with error columns.
    """

    if forecast_df is None or forecast_df.empty:
        return pd.DataFrame()

    forecast = forecast_df.copy()
    actual = actual_df.copy()

    df = forecast.merge(
        actual[date_cols + [target_col]],
        on=date_cols,
        how="left",
    )

    df[target_col] = df[target_col].fillna(0)

    df["error"] = df[forecast_col] - df[target_col]
    df["absolute_error"] = df["error"].abs()
    df["percentage_error"] = (
        df["error"] / df[target_col].replace(0, np.nan)
    )
    df["absolute_percentage_error"] = df["percentage_error"].abs()
    df["squared_error"] = df["error"] ** 2

    return df


def summarise_forecast_errors(error_df, group_cols):
    """
    Summarise forecast errors by method and segment.

    Parameters
    ----------
    error_df : pandas.DataFrame
        Forecast dataframe with error columns.

    group_cols : list
        Columns to group by.

    Returns
    -------
    pandas.DataFrame
        Error summary with MAE, MAPE, RMSE and bias.
    """

    if error_df is None or error_df.empty:
        return pd.DataFrame()

    summary = (
        error_df
        .groupby(group_cols)
        .agg(
            mae=("absolute_error", "mean"),
            bias=("error", "mean"),
            mape=("absolute_percentage_error", "mean"),
            rmse=("squared_error", lambda x: (x.mean()) ** 0.5),
            n=("absolute_error", "count"),
        )
        .reset_index()
    )

    return summary



# # ============================================================
# # 14. OPERATIONAL / FTE TRANSLATION
# # ============================================================

# def estimate_hourly_workload(hourly_forecast, assumptions):
#     """
#     Convert hourly entry and exit forecasts into workload.

#     Purpose:
#         Translate hourly forecast entries/exits into operational effort.

#     Parameters
#     ----------
#     hourly_forecast : pandas.DataFrame
#         Forecast by hour with:
#             - forecast_entries
#             - forecast_exits

#     assumptions : dict
#         Example:
#             {
#                 "minutes_per_entry_move": 6,
#                 "minutes_per_exit_move": 6,
#                 "minutes_per_internal_move": 0,
#                 "productive_minutes_per_fte_hour": 45
#             }

#     Returns
#     -------
#     pandas.DataFrame
#         Hourly workload table.
#     """

#     if hourly_forecast is None or hourly_forecast.empty:
#         return pd.DataFrame()

#     df = hourly_forecast.copy()

#     if "forecast_entries" not in df.columns:
#         df["forecast_entries"] = 0

#     if "forecast_exits" not in df.columns:
#         df["forecast_exits"] = 0

#     if "forecast_internal_moves" not in df.columns:
#         df["forecast_internal_moves"] = 0

#     df["entry_workload_minutes"] = (
#         df["forecast_entries"] * assumptions.get("minutes_per_entry_move", 0)
#     )

#     df["exit_workload_minutes"] = (
#         df["forecast_exits"] * assumptions.get("minutes_per_exit_move", 0)
#     )

#     df["internal_move_workload_minutes"] = (
#         df["forecast_internal_moves"] * assumptions.get("minutes_per_internal_move", 0)
#     )

#     df["total_workload_minutes"] = (
#         df["entry_workload_minutes"]
#         + df["exit_workload_minutes"]
#         + df["internal_move_workload_minutes"]
#     )

#     df["required_fte"] = (
#         df["total_workload_minutes"]
#         / assumptions.get("productive_minutes_per_fte_hour", 60)
#     )

#     return df


# def estimate_capacity_pressure(hourly_forecast, capacity_assumptions):
#     """
#     Estimate simple block parking pressure from hourly entries and exits.

#     Purpose:
#         Identify whether forecast net flow creates occupancy pressure.

#     Parameters
#     ----------
#     hourly_forecast : pandas.DataFrame
#         Forecast by hour with:
#             - datetime_hour
#             - forecast_entries
#             - forecast_exits

#     capacity_assumptions : dict
#         Example:
#             {
#                 "opening_occupancy": 0,
#                 "block_parking_capacity": 1000,
#                 "return_bay_capacity": 100,
#                 "ferry_lane_capacity": 100
#             }

#     Returns
#     -------
#     pandas.DataFrame
#         Capacity pressure table.
#     """

#     if hourly_forecast is None or hourly_forecast.empty:
#         return pd.DataFrame()

#     df = hourly_forecast.copy()

#     if "forecast_entries" not in df.columns:
#         df["forecast_entries"] = 0

#     if "forecast_exits" not in df.columns:
#         df["forecast_exits"] = 0

#     df = df.sort_values("datetime_hour").copy()

#     opening_occupancy = capacity_assumptions.get("opening_occupancy", 0)

#     df["forecast_net_flow"] = df["forecast_entries"] - df["forecast_exits"]

#     df["estimated_block_parking_occupancy"] = (
#         opening_occupancy + df["forecast_net_flow"].cumsum()
#     )

#     df["block_parking_breach_flag"] = (
#         df["estimated_block_parking_occupancy"]
#         > capacity_assumptions.get("block_parking_capacity", np.inf)
#     )

#     df["ferry_lane_breach_flag"] = (
#         df["forecast_entries"]
#         > capacity_assumptions.get("ferry_lane_capacity", np.inf)
#     )

#     df["return_bay_breach_flag"] = (
#         df["forecast_exits"]
#         > capacity_assumptions.get("return_bay_capacity", np.inf)
#     )

#     return df


# ============================================================
# 15. OUTPUT TABLES
# ============================================================

def build_executive_summary(
    tendency_summary,
    entry_forecast_comparison,
    exit_forecast_comparison,
):
    """
    Build a simple executive summary table for Excel.

    Returns
    -------
    pandas.DataFrame
    """

    rows = []

    # ----------------------------
    # Best tendency method
    # ----------------------------
    try:

        tendency_perf = (
            tendency_summary["overall_method_performance"]
            .sort_values("mae")
        )

        best_tendency = tendency_perf.iloc[0]

        rows.append(
            {
                "metric": "Best Tendency Method",
                "value": best_tendency["method"],
            }
        )

        rows.append(
            {
                "metric": "Best Tendency MAE",
                "value": round(best_tendency["mae"], 2),
            }
        )

    except Exception:
        pass

    # ----------------------------
    # Best entry method
    # ----------------------------
    try:

        entry_perf = (
            entry_forecast_comparison[
                "entry_forecast_performance_summary"
            ]["overall_performance"]
            .sort_values("mae")
        )

        best_entry = entry_perf.iloc[0]

        rows.append(
            {
                "metric": "Best Entry Forecast Method",
                "value": best_entry["method"],
            }
        )

        rows.append(
            {
                "metric": "Best Entry Forecast MAE",
                "value": round(best_entry["mae"], 2),
            }
        )

    except Exception:
        pass

    # ----------------------------
    # Best exit method
    # ----------------------------
    try:

        exit_perf = (
            exit_forecast_comparison[
                "daily_exit_performance_summary"
            ]["overall_performance"]
            .sort_values("mae")
        )

        best_exit = exit_perf.iloc[0]

        rows.append(
            {
                "metric": "Best Exit Forecast Method",
                "value": best_exit["method"],
            }
        )

        rows.append(
            {
                "metric": "Best Exit Forecast MAE",
                "value": round(best_exit["mae"], 2),
            }
        )

    except Exception:
        pass

    return pd.DataFrame(rows)

def build_output_tables(
    executive_summary,
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
        "executive_summary": executive_summary,
        "booking_operation_match_summary": reconciliation_summary,
        "daily_fastpark_actuals": daily_fastpark_actuals,
        "hourly_fastpark_actuals": hourly_fastpark_actuals,
        "daily_driver_dataset": daily_driver_dataset,
        "booking_curve_summary": booking_curve,
        "tendency_backtest_results": tendency_backtest_results,
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

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:

        used_sheet_names = set()
        sheets_written = 0

        for raw_sheet_name, df in flat_outputs.items():

            if df is None or not isinstance(df, pd.DataFrame):
                continue

            safe_sheet_name = clean_sheet_name(raw_sheet_name)

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

    print("[1/17] Loading FastPark Bookings…")
    bookings_raw = get_fastpark_bookings(
        start=config["analysis_start_date"],
        end=config["analysis_end_date"],
        statuses=["B", "CX", "F"],
        asset_name=config["asset_name"],
        engine=sql_connection,
    )

    t1 = step(t0, f"Loaded FastPark Bookings ({len(bookings_raw):,} rows)")

    print("[2/17] Loading FastPark Actuals…")

    operations_raw = get_fastpark_entry_exits(
        start=config["analysis_start_date"],
        end=config["analysis_end_date"],
        engine=sql_connection,
    )

    t2 = step(t1, f"Loaded FastPark Actuals ({len(operations_raw):,} rows)")

    print("[3/17] Loading Historical Flights…")

    flights_raw = get_historical_flight_performance(
        start=config["analysis_start_date"],
        end=config["analysis_end_date"],
        engine=sql_connection,
    )
    t3 = step(t2, f"Loaded Historical Flight Performance ({len(flights_raw):,} rows)")



    # ----------------------------
    # Clean raw data
    # ----------------------------

    print("[4/17] Cleaning FastPark Bookings, Actuals and Flights…")

    bookings_clean = clean_bookings(bookings_raw, config)
    operations_clean = clean_operations(operations_raw, config)
    flights_clean = clean_flights(flights_raw, config)

    t4 = step(t3, "Cleaned FastPark Bookings, Actuals and Flights")

    # ----------------------------
    # Reconcile bookings and operations
    # ----------------------------

    print("[5/17] Reconciling FastPark Bookings and Actuals…")

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

    print("[6/17] Analysing Booking Statuses, Cancellations and No-Shows…")

    status_summary = analyse_booking_statuses(bookings_clean)
    cancellation_results = analyse_cancellations(bookings_clean)
    no_show_results = analyse_no_shows(master)

    t6 = step(t5, "Analysed Booking Statuses, Cancellations and No-Shows")


    # ----------------------------
    # Actual FastPark demand
    # ----------------------------

    print("[7/17] Creating Daily and Hourly FastPark Actuals…")

    daily_fastpark_actuals = create_daily_fastpark_actuals(master)
    hourly_fastpark_actuals = create_hourly_fastpark_actuals(master)
    hourly_profiles = create_hourly_profiles(hourly_fastpark_actuals)

    t7 = step(t6, "Created Daily and Hourly FastPark Actuals")


    # ----------------------------
    # Passenger / flight context
    # ----------------------------

    print("[8/17] Creating Passenger Context Features…")
    daily_passenger_summary = create_daily_passenger_summary(flights_clean, config)
    hourly_passenger_summary = create_hourly_passenger_summary(flights_clean, config)
    mix_features = create_airline_country_mix_features(flights_clean)

    t8 = step(t7, "Created Passenger Context Features")

    # ----------------------------
    # Hourly FastPark vs passenger offset analysis
    # ----------------------------

    print("[9/17] Analysing Hourly FastPark vs Passenger Offsets…")

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

    print("[10/17] Analysing Daily FastPark Drivers…")
    daily_driver_dataset = create_daily_driver_dataset(
        daily_fastpark_actuals,
        daily_passenger_summary,
    )

    driver_analysis = analyse_actual_demand_drivers(daily_driver_dataset)

    t10 = step(t9, "Analysed Daily FastPark Drivers")

    # ----------------------------
    # Booking curve
    # ----------------------------

    print("[11/17] Analysing Booking Curve…")
    
    booking_curve = create_booking_curve_dataset(bookings_clean, master, config)
    booking_curve_segments = analyse_booking_curve_segments(booking_curve)

    t11 = step(t10, "Analysed Booking Curve")

    # ----------------------------
    # Duration and return behaviour
    # ----------------------------

    print("[12/17] Analysing Duration and Return Behaviour…")

    duration_df = create_duration_analysis_dataset(master, config)
    duration_patterns = analyse_duration_patterns(duration_df)
    return_deviation = analyse_return_deviation(duration_df)
    known_booked_exit_forecast = create_known_booked_exit_forecast(master)

    t12 = step(t11, "Analysed Duration and Return Behaviour")

    # ----------------------------
    # Exit forecast method comparison
    # ----------------------------

    print("[13/17] Comparing Exit Forecast Methods…")

    exit_forecast_comparison = run_exit_forecast_method_comparison(
        master=master,
        daily_driver_dataset=daily_driver_dataset,
        daily_fastpark_actuals=daily_fastpark_actuals,
        hourly_fastpark_actuals=hourly_fastpark_actuals,
        duration_df=duration_df,
        config=config,
    )

    t13 = step(t12, "Compared Exit Forecast Methods")

    # ----------------------------
    # Tendency back-testing
    # ----------------------------

    print("[14/17] Back-testing Tendency Forecast Methods…")

    tendency_backtest_results = run_tendency_window_backtest(
        daily_driver_dataset,
        config,
    )

    tendency_summary = summarise_backtest_results(tendency_backtest_results)

    t14 = step(t13, "Back-tested Tendency Forecast Methods")

    # ----------------------------
    # Entry forecast method comparison
    # ----------------------------

    print("[15/17] Comparing Entry Forecast Methods…")

    entry_forecast_comparison = run_entry_forecast_method_comparison(
        booking_curve=booking_curve,
        tendency_backtest_results=tendency_backtest_results,
    )

    t15 = step(t14, "Compared Entry Forecast Methods")

    # ----------------------------
    # Executive summary 
    # ----------------------------

    print("[16/17] Building Executive Summary…")

    executive_summary = build_executive_summary(
        tendency_summary=tendency_summary,
        entry_forecast_comparison=entry_forecast_comparison,
        exit_forecast_comparison=exit_forecast_comparison,
    )

    t16 = step(t15, "Built Executive Summary")
    # ----------------------------
    # Package outputs
    # ----------------------------

    print("[17/17] Building Outputs…")
    forecast_error_summaries = {
        "status_summary": status_summary,
        "cancellation_results": cancellation_results,
        "no_show_results": no_show_results,
        "hourly_profiles": hourly_profiles,
        "hourly_offset_analysis": hourly_offset_analysis,
        "driver_analysis": driver_analysis,
        "mix_features": mix_features,
        "booking_curve_segments": booking_curve_segments,
        "duration_patterns": duration_patterns,
        "return_deviation": return_deviation,
        "known_booked_exit_forecast": known_booked_exit_forecast,
        "exit_forecast_comparison": exit_forecast_comparison,
        "tendency_summary": tendency_summary,
        "entry_forecast_comparison": entry_forecast_comparison,
    }

    outputs = build_output_tables(
        executive_summary=executive_summary,
        reconciliation_summary=reconciliation_summary,
        daily_fastpark_actuals=daily_fastpark_actuals,
        hourly_fastpark_actuals=hourly_fastpark_actuals,
        daily_driver_dataset=daily_driver_dataset,
        booking_curve=booking_curve,
        duration_df=duration_df,
        tendency_backtest_results=tendency_backtest_results,
        forecast_error_summaries=forecast_error_summaries,
    )

    t17 = step(t16, "Built Outputs")

    
    if output_path is not None:
        print(f"Exporting Outputs to {output_path}…")
        export_outputs_to_excel(
            outputs=outputs,
            output_path=output_path,
        )
        t18 = step(t17, f"Exported Outputs to {output_path}")

    print(f"Completed FastPark Historical Analysis in {t18 - t0:.2f} seconds.")

    return outputs




if __name__ == "__main__":
    dsn = 'AzureConnection'
    user = 'jamie_douglas'


    sql_connection = get_engine(dsn=dsn, username=user)

    outputs = run_fastpark_historical_analysis(
        sql_connection=sql_connection,
        output_path=r"output\fastpark\reports\fastpark_historical_analysis.xlsx",
    )