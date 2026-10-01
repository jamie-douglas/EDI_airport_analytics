"""
FASTPARK SHORT TERM FORECAST 2026
=================================

Purpose
-------
Generate operational FastPark entry and exit forecasts for the next
56 days.

This script is the production implementation of the FastPark forecasting
framework developed through:

    Generation 1 Historical Simulation
    Generation 2 Rolling-Origin Ensemble
    Generation 2.1 Non-Price Refinement

The forecast uses evidence-based component weights derived from the
calibration process and stored in:

    forecast_weights.csv

Core Principles
---------------
The forecast combines multiple independently useful signals:

ENTRIES
    - Booking visibility
    - Historical same weekday demand
    - Historical weekday-month demand
    - Trend-adjusted seasonality

EXITS
    - Exit booking visibility
    - Stay-duration behaviour
    - Historical same weekday demand
    - Trend-adjusted seasonality

The forecasting script itself does NOT:

    - calibrate weights
    - test alternative models
    - optimise performance
    - backtest forecasts

Those activities belong to:

    create_forecast_weights.py
    fastpark_forecast_weight_recalibration.py

Outputs
-------
Daily Forecast
    One row per forecast date.

Hourly Forecast
    One row per forecast hour.

Forecast Components
    Full audit trail showing the contribution of each forecasting component.

Primary Data Sources
--------------------

AirportX.v_Bookings
    Booking visibility and cancellation behaviour.

FastPark.v_EntryAndExits
    Operational entries, exits and stay durations.

EAL.FlightPerformance
    Optional monitoring and future enhancements.
"""

# =============================================================================
# 1. IMPORTS
# =============================================================================

import pathlib
import sys
import time

from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


from modules.utils.db import get_engine
from modules.utils.progress import step

# =============================================================================
# 2. CONFIGURATION
# =============================================================================

def get_forecast_config():
    """
    Production forecast configuration.

    IMPORTANT
    ---------
    This script should not contain forecasting logic inside the
    configuration.

    Forecast logic is derived from:

        forecast_weights.csv
        forecast_parameters.csv

    generated from the simulation framework.

    Only operational settings, data ranges and default fallback values
    should be stored here.
    """

    base_dir = Path(__file__).resolve().parent

    return {

        # ============================================================
        # HISTORICAL DATA WINDOW
        # ============================================================
        #
        # Forecast components rely on historical actuals.
        #
        # We use January 2024 because:
        #
        #   - It was the historical period used throughout
        #     the analysis framework.
        #
        #   - It provides sufficient history for:
        #
        #         weekday models
        #         month models
        #         duration curves
        #         booking curves
        #
        # Change only if older history becomes available and has
        # acceptable data quality.
        #
        # ============================================================

        "history_start": "2024-01-01",

        # ============================================================
        # FORECAST HORIZON
        # ============================================================
        #
        # Production forecast length.
        #
        # 56 days was used throughout:
        #
        #     Generation 1
        #     Generation 2
        #     Generation 2.1
        #
        # This should only change if the operational planning horizon
        # changes.
        #
        # ============================================================

        "forecast_days": 56,


        # ============================================================
        # TREND SETTINGS
        # ============================================================
        #
        # Trend was one of the strongest surviving components during
        # Generation 2.
        #
        # Trend factor:
        #
        #     Recent 28 days
        #     compared against
        #     previous 84 days
        #
        # The clipping limits prevent temporary spikes from causing
        # unrealistic forecasts.
        #
        # ============================================================

        "trend_recent_days": 28,

        "trend_comparison_days": 84,

        "trend_factor_min": 0.70,

        "trend_factor_max": 1.30,


        # ============================================================
        # DURATION CURVE
        # ============================================================
        #
        # Maximum duration included in exit-duration modelling.
        #
        # This is not a forecast parameter.
        #
        # It is a data-quality guardrail designed to remove unusual
        # stays that distort duration distributions.
        #
        # Source:
        #
        #     Existing production forecast.
        #
        "maximum_duration_days": 35,

        # ============================================================
        # CONFIGURATION FILES
        # ============================================================

        # Production forecasting weights.
        #
        # Created by:
        #
        #     create_forecast_weights.py
        #
        "weights_file": (
            base_dir
            / "forecast_weights.csv"
        ),

        # Production forecasting parameters.
        #
        # Created by:
        #
        #     create_forecast_parameters.py
        #
        "parameters_file": (
            base_dir
            / "forecast_parameters.csv"
        ),

        # ============================================================
        # OUTPUTS
        # ============================================================

        "output_folder": (
            base_dir
            / "Forecast Outputs"
        ),

        # ============================================================
        # PRODUCT
        # ============================================================

        "asset_name": "FastPark",

    }

# =============================================================================
# 3. FORECAST WEIGHTS
# =============================================================================

def load_forecast_weights(weight_file):
    """
    Load the production forecast weights.

    These weights originate from the rolling-origin simulation framework
    and should not be manually edited within this forecasting script.
    """

    weights = pd.read_csv(weight_file)

    required_columns = {
        "demand_type",
        "horizon_days",
    }

    missing_columns = (
        required_columns
        - set(weights.columns)
    )

    if missing_columns:

        raise ValueError(
            "Missing required weight columns:\n"
            f"{sorted(missing_columns)}"
        )

    return (
        weights
        .sort_values(
            [
                "demand_type",
                "horizon_days",
            ]
        )
        .reset_index(drop=True)
    )


def get_horizon_weights(
    weights_df,
    demand_type,
    horizon_days,
):
    """
    Retrieve the forecast component weights for a specific
    flow and horizon.

    Example:

        Entry T-14

    Returns:

        booking weight
        weekday weight
        month weight
        trend weight
    """

    result = weights_df[
        weights_df["demand_type"].eq(
            demand_type
        )
        &
        weights_df["horizon_days"].eq(
            horizon_days
        )
    ]

    if result.empty:

        raise ValueError(
            f"No weight configuration found for "
            f"{demand_type} "
            f"T-{horizon_days}"
        )

    return result.iloc[0].to_dict()

# =============================================================================
# 4. FORECAST PARAMETERS
# =============================================================================

def load_forecast_parameters(parameter_file):
    """
    Load production forecasting parameters.

    Parameters originate from:

        forecast_parameters.csv

    created by:

        create_forecast_parameters.py

    These values determine:

        - booking shrinkage
        - hourly profile windows
        - same-weekday history windows
    """

    parameters = pd.read_csv(parameter_file)

    required_columns = {
        "flow",
        "horizon_days",
        "booking_shrinkage_strength",
        "hourly_profile_window",
        "same_weekday_history",
    }

    missing_columns = (
        required_columns
        - set(parameters.columns)
    )

    if missing_columns:

        raise ValueError(
            "Missing parameter columns:\n"
            f"{sorted(missing_columns)}"
        )

    return (
        parameters
        .sort_values(
            [
                "flow",
                "horizon_days",
            ]
        )
        .reset_index(drop=True)
    )


def get_horizon_parameters(
    parameters_df,
    flow,
    horizon_days,
):
    """
    Retrieve calibrated production parameters for a
    specific flow and forecast horizon.
    """

    result = parameters_df[
        parameters_df["flow"].eq(flow)
        &
        parameters_df["horizon_days"].eq(horizon_days)
    ]

    if result.empty:

        raise ValueError(
            f"No parameters found for "
            f"{flow} "
            f"T-{horizon_days}"
        )

    return result.iloc[0].to_dict()

# =============================================================================
# 5. DATA EXTRACTION
# =============================================================================

def get_bookings(
    engine,
    config,
):
    """
    Load FastPark bookings.

    Source
    ------
    AirportX.v_Bookings

    Purpose
    -------
    Used for:

        - Booking visibility
        - Booking pace
        - Cancellation analysis
        - Planned entry dates
        - Planned exit dates
        - Stay duration calculations

    Notes
    -----
    Only FastPark bookings are required.

    Historical simulations proved that booking information is the
    strongest forecasting signal at shorter horizons.
    """

    sql = """
    SELECT
        bookingId,
        createdAt,
        cancelledAt,
        entryDate,
        exitDate,
        status,
        assetName,
        leadtime,
        duration
    FROM AirportX.v_Bookings
    WHERE assetName = ?
    """

    bookings = pd.read_sql(
        sql,
        engine,
        params=[config["asset_name"]],
    )

    datetime_columns = [
        "createdAt",
        "cancelledAt",
        "entryDate",
        "exitDate",
    ]

    for column in datetime_columns:

        bookings[column] = pd.to_datetime(
            bookings[column],
            errors="coerce",
        )

    return bookings


def get_operations(
    engine,
):
    """
    Load FastPark operational movements.

    Source
    ------
    FastPark.v_EntryAndExits

    Purpose
    -------
    Used for:

        - Actual entries
        - Actual exits
        - Historical hourly profiles
        - Stay-duration modelling
        - Entry/exit trend calculations

    Notes
    -----
    This is the operational truth dataset used throughout
    the historical analysis and simulation framework.
    """

    sql = """
    SELECT
        BookingReference,
        CheckInStarted,
        CheckInEnded,
        ExpectedReturnDate,
        ActualCheckedOutDate
    FROM FastPark.v_EntryAndExits
    """

    operations = pd.read_sql(
        sql,
        engine,
    )

    datetime_columns = [
        "CheckInStarted",
        "CheckInEnded",
        "ExpectedReturnDate",
        "ActualCheckedOutDate",
    ]

    for column in datetime_columns:

        operations[column] = pd.to_datetime(
            operations[column],
            errors="coerce",
        )

    return operations


def get_flights(
    engine,
):
    """
    Load historical passenger and flight information.

    Source
    ------
    EAL.FlightPerformance

    Current Usage
    -------------
    This dataset is currently retained primarily for:

        - Monitoring
        - Diagnostics
        - Future model development

    Forecasting input usage is intentionally limited in Version 1,
    because the simulation work demonstrated that booking behaviour
    outperformed penetration-based forecasting.
    """

    sql = """
    SELECT
        FlightID,
        ScheduledDateTime_Local,
        ArrDeptureCode,
        Domestic_International,
        Passengers,
        Pax_MostConfident
    FROM EAL.FlightPerformance
    WHERE IsPassengerFlight = 1
    """

    flights = pd.read_sql(
        sql,
        engine,
    )

    flights[
        "ScheduledDateTime_Local"
    ] = pd.to_datetime(
        flights[
            "ScheduledDateTime_Local"
        ],
        errors="coerce",
    )

    return flights


def load_forecast_data(
    config,
):
    """
    Load every dataset required by the production forecast.

    Returns
    -------
    dict

        bookings
        operations
        flights
    """

    print("=" * 80)
    print("LOADING FORECAST DATA")
    print("=" * 80)

    engine = get_engine()

    bookings = get_bookings(
        engine=engine,
        config=config,
    )

    operations = get_operations(
        engine=engine,
    )

    flights = get_flights(
        engine=engine,
    )

    print(
        f"Bookings:   {len(bookings):,}"
    )

    print(
        f"Operations: {len(operations):,}"
    )

    print(
        f"Flights:    {len(flights):,}"
    )

    return {
        "bookings": bookings,
        "operations": operations,
        "flights": flights,
    }

# =============================================================================
# 6. HISTORICAL DATASETS
# =============================================================================

def create_daily_actuals(
    operations,
):
    """
    Create actual daily FastPark demand.

    Definitions
    -----------

    Entry
        CheckInEnded

    Exit
        ActualCheckedOutDate

    These definitions were used consistently throughout:

        Historical Analysis
        Generation 1
        Generation 2
        Generation 2.1
    """

    df = operations.copy()

    # ---------------------------------------------------------------------
    # Entries
    # ---------------------------------------------------------------------

    entries = (
        df
        .dropna(
            subset=["CheckInEnded"]
        )
        .assign(
            date=lambda x:
                x["CheckInEnded"]
                .dt.normalize()
        )
        .groupby(
            "date",
            as_index=False,
        )
        .agg(
            entries=(
                "BookingReference",
                "nunique",
            )
        )
    )

    # ---------------------------------------------------------------------
    # Exits
    # ---------------------------------------------------------------------

    exits = (
        df
        .dropna(
            subset=["ActualCheckedOutDate"]
        )
        .assign(
            date=lambda x:
                x["ActualCheckedOutDate"]
                .dt.normalize()
        )
        .groupby(
            "date",
            as_index=False,
        )
        .agg(
            exits=(
                "BookingReference",
                "nunique",
            )
        )
    )

    daily = (
        entries
        .merge(
            exits,
            on="date",
            how="outer",
        )
        .fillna(0)
    )

    daily["entries"] = daily["entries"].astype(int)
    daily["exits"] = daily["exits"].astype(int)

    daily["weekday"] = (
        daily["date"]
        .dt.day_name()
    )

    daily["weekday_num"] = (
        daily["date"]
        .dt.weekday
    )

    daily["month"] = (
        daily["date"]
        .dt.month
    )

    daily["year"] = (
        daily["date"]
        .dt.year
    )

    return (
        daily
        .sort_values("date")
        .reset_index(drop=True)
    )


def create_hourly_actuals(
    operations,
):
    """
    Create historical hourly demand.

    This dataset is used to:

        - Build hourly profiles
        - Convert daily forecasts into hourly forecasts
    """

    entries = (
        operations
        .dropna(subset=["CheckInEnded"])
        .assign(
            datetime=lambda x:
                x["CheckInEnded"]
                .dt.floor("h")
        )
        .groupby(
            "datetime",
            as_index=False,
        )
        .agg(
            entries=(
                "BookingReference",
                "nunique",
            )
        )
    )

    exits = (
        operations
        .dropna(subset=["ActualCheckedOutDate"])
        .assign(
            datetime=lambda x:
                x["ActualCheckedOutDate"]
                .dt.floor("h")
        )
        .groupby(
            "datetime",
            as_index=False,
        )
        .agg(
            exits=(
                "BookingReference",
                "nunique",
            )
        )
    )

    hourly = (
        entries
        .merge(
            exits,
            on="datetime",
            how="outer",
        )
        .fillna(0)
    )

    hourly["entries"] = hourly["entries"].astype(int)
    hourly["exits"] = hourly["exits"].astype(int)

    hourly["date"] = (
        hourly["datetime"]
        .dt.normalize()
    )

    hourly["hour"] = (
        hourly["datetime"]
        .dt.hour
    )

    hourly["weekday"] = (
        hourly["datetime"]
        .dt.day_name()
    )

    hourly["weekday_num"] = (
        hourly["datetime"]
        .dt.weekday
    )

    return (
        hourly
        .sort_values("datetime")
        .reset_index(drop=True)
    )


def create_duration_distribution(
    bookings,
    config,
):
    """
    Create historical planned duration distribution.

    Purpose
    -------
    Used by the exit duration component.

    We use planned duration because:

        - Available for future bookings
        - Proven highly correlated with actual duration
          during the historical analysis work.
    """

    df = bookings.copy()

    df = df.dropna(
        subset=[
            "entryDate",
            "exitDate",
        ]
    )

    df["planned_duration_days"] = (
        (
            df["exitDate"]
            - df["entryDate"]
        )
        .dt.total_seconds()
        / 86400
    )

    df = df[
        df["planned_duration_days"].ge(0)
    ]

    df = df[
        df["planned_duration_days"].le(
            config["maximum_duration_days"]
        )
    ]

    distribution = (
        df
        .groupby(
            "planned_duration_days"
        )
        .size()
        .rename("bookings")
        .reset_index()
    )

    distribution["probability"] = (
        distribution["bookings"]
        /
        distribution["bookings"].sum()
    )

    return distribution

# =============================================================================
# 7. FORECAST COMPONENTS
# =============================================================================

def get_bookings_active_as_of(
    bookings,
    cutoff_timestamp,
):
    """
    Return bookings that were active at a historical point in time.

    Logic
    -----

    Booking must:

        - have been created
        - not yet cancelled

    as of the forecast cutoff.

    This preserves the no-look-ahead behaviour developed during
    the simulation framework.
    """

    cutoff_timestamp = pd.Timestamp(
        cutoff_timestamp
    )

    active_mask = (
        bookings["createdAt"].le(
            cutoff_timestamp
        )
        &
        (
            bookings["cancelledAt"].isna()
            |
            bookings["cancelledAt"].gt(
                cutoff_timestamp
            )
        )
    )

    return bookings.loc[
        active_mask
    ].copy()

def calculate_entry_booking_component(
    bookings,
    target_date,
    cutoff_timestamp,
):
    """
    Visible bookings for the target entry date.

    This is the strongest short-horizon forecasting signal.
    """

    active = get_bookings_active_as_of(
        bookings=bookings,
        cutoff_timestamp=cutoff_timestamp,
    )

    count = (
        active[
            active["entryDate"]
            .dt.normalize()
            .eq(
                pd.Timestamp(
                    target_date
                ).normalize()
            )
        ]["bookingId"]
        .nunique()
    )

    return float(count)

def calculate_exit_booking_component(
    bookings,
    target_date,
    cutoff_timestamp,
):
    """
    Visible bookings for the target exit date.
    """

    active = get_bookings_active_as_of(
        bookings=bookings,
        cutoff_timestamp=cutoff_timestamp,
    )

    count = (
        active[
            active["exitDate"]
            .dt.normalize()
            .eq(
                pd.Timestamp(
                    target_date
                ).normalize()
            )
        ]["bookingId"]
        .nunique()
    )

    return float(count)

def calculate_same_weekday_component(
    daily_actuals,
    target_date,
    cutoff_timestamp,
    target_col,
    history_length,
):
    """
    Forecast using previous matching weekdays.

    Example:

        Forecasting Saturday

    Uses:

        previous N Saturdays
    """

    target_date = pd.Timestamp(
        target_date
    ).normalize()

    cutoff_date = pd.Timestamp(
        cutoff_timestamp
    ).normalize()

    weekday = target_date.weekday()

    history = (
        daily_actuals[
            daily_actuals["date"].lt(
                cutoff_date
            )
            &
            daily_actuals[
                "date"
            ].dt.weekday.eq(
                weekday
            )
        ]
        .sort_values("date")
        .tail(history_length)
    )

    if len(history) < history_length:
        return np.nan

    return float(
        history[target_col].mean()
    )

def calculate_weekday_month_component(
    daily_actuals,
    target_date,
    cutoff_timestamp,
    target_col,
    max_history,
):
    """
    Same weekday + same month forecast.

    Example:

        July Saturday

    compares against:

        historical July Saturdays
    """

    target_date = pd.Timestamp(
        target_date
    ).normalize()

    cutoff_date = pd.Timestamp(
        cutoff_timestamp
    ).normalize()

    target_month = (
        target_date.month
    )

    target_weekday = (
        target_date.weekday()
    )

    history = (
        daily_actuals[
            daily_actuals["date"].lt(
                cutoff_date
            )
            &
            daily_actuals["month"].eq(
                target_month
            )
            &
            daily_actuals[
                "weekday_num"
            ].eq(
                target_weekday
            )
        ]
        .sort_values("date")
        .tail(max_history)
    )

    if history.empty:
        return np.nan

    return float(
        history[target_col].mean()
    )

def calculate_trend_factor(
    daily_actuals,
    cutoff_timestamp,
    target_col,
    config,
):
    """
    Calculate trend adjustment.

    Trend compares:

        recent period

    vs

        prior period
    """

    cutoff_date = pd.Timestamp(
        cutoff_timestamp
    ).normalize()

    recent_start = (
        cutoff_date
        - pd.Timedelta(
            days=config["trend_recent_days"]
        )
    )

    comparison_start = (
        recent_start
        - pd.Timedelta(
            days=config["trend_comparison_days"]
        )
    )

    recent = (
        daily_actuals[
            daily_actuals["date"].between(
                recent_start,
                cutoff_date,
                inclusive="left",
            )
        ][target_col]
    )

    comparison = (
        daily_actuals[
            daily_actuals["date"].between(
                comparison_start,
                recent_start,
                inclusive="left",
            )
        ][target_col]
    )

    if (
        len(recent) == 0
        or len(comparison) == 0
    ):
        return 1.0

    comparison_mean = (
        comparison.mean()
    )

    if comparison_mean <= 0:
        return 1.0

    factor = (
        recent.mean()
        /
        comparison_mean
    )

    return float(
        np.clip(
            factor,
            config["trend_factor_min"],
            config["trend_factor_max"],
        )
    )

def calculate_entry_trend_component(
    daily_actuals,
    target_date,
    cutoff_timestamp,
    config,
):
    """
    Trend-adjusted seasonal entry forecast.
    """

    month_component = (
        calculate_weekday_month_component(
            daily_actuals=daily_actuals,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            target_col="entries",
            max_history=config[
                "weekday_month_history"
            ],
        )
    )

    if pd.isna(month_component):
        return np.nan

    trend_factor = (
        calculate_trend_factor(
            daily_actuals=daily_actuals,
            cutoff_timestamp=cutoff_timestamp,
            target_col="entries",
            config=config,
        )
    )

    return (
        month_component
        * trend_factor
    )

def calculate_exit_trend_component(
    daily_actuals,
    target_date,
    cutoff_timestamp,
    config,
):
    """
    Trend-adjusted seasonal exit forecast.
    """

    month_component = (
        calculate_weekday_month_component(
            daily_actuals=daily_actuals,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            target_col="exits",
            max_history=config[
                "weekday_month_history"
            ],
        )
    )

    if pd.isna(month_component):
        return

def calculate_exit_duration_component(
    duration_distribution,
    bookings,
    target_date,
):
    """
    Expected exits generated from historical
    duration behaviour.

    Version 1 uses planned durations because
    they are observable for future bookings.
    """

    target_date = pd.Timestamp(
        target_date
    ).normalize()

    active = bookings.copy()

    active["entry_day"] = (
        active["entryDate"]
        .dt.normalize()
    )

    active["exit_day"] = (
        active["exitDate"]
        .dt.normalize()
    )

    matching = active[
        active["exit_day"].eq(
            target_date
        )
    ]

    return float(
        matching["bookingId"]
        .nunique()
    )

# =============================================================================
# 8. DAILY FORECAST ASSEMBLY
# =============================================================================

def apply_booking_shrinkage(
    booking_forecast,
    historical_forecast,
    known_bookings,
    shrinkage_strength,
):
    """
    Apply booking shrinkage.

    Purpose
    -------
    At long horizons a booking-based forecast can become unstable when
    only a handful of bookings are visible.

    Shrinkage gradually moves the forecast toward the historical model
    when booking visibility is weak.

    Behaviour
    ---------
    Large booking count:
        Trust bookings.

    Small booking count:
        Trust historical demand.
    """

    if pd.isna(booking_forecast):
        return historical_forecast

    if pd.isna(historical_forecast):
        return booking_forecast

    known_bookings = max(
        0,
        float(known_bookings),
    )

    shrinkage_strength = max(
        0,
        float(shrinkage_strength),
    )

    if shrinkage_strength == 0:
        return booking_forecast

    reliability = (
        known_bookings
        /
        (
            known_bookings
            + shrinkage_strength
        )
    )

    return (
        reliability
        * booking_forecast
        +
        (1 - reliability)
        * historical_forecast
    )


def combine_entry_components(
    components,
    weights,
):
    """
    Combine entry forecasting components using calibrated weights.
    """

    component_values = {
        "entry_booking_shrunk":
            components["entry_booking_shrunk"],

        "entry_weekday":
            components["entry_weekday"],

        "entry_month":
            components["entry_month"],

        "entry_trend_month":
            components["entry_trend_month"],
    }

    weighted_values = []

    weighted_weights = []

    for component_name, value in component_values.items():

        if pd.isna(value):
            continue

        weight = weights.get(
            component_name,
            0,
        )

        weighted_values.append(value)

        weighted_weights.append(weight)

    if not weighted_values:
        return np.nan

    return float(
        np.average(
            weighted_values,
            weights=weighted_weights,
        )
    )


def combine_exit_components(
    components,
    weights,
):
    """
    Combine exit forecasting components using calibrated weights.
    """

    component_values = {
        "exit_booking_shrunk":
            components["exit_booking_shrunk"],

        "exit_duration":
            components["exit_duration"],

        "exit_weekday":
            components["exit_weekday"],

        "exit_trend_month":
            components["exit_trend_month"],
    }

    weighted_values = []

    weighted_weights = []

    for component_name, value in component_values.items():

        if pd.isna(value):
            continue

        weight = weights.get(
            component_name,
            0,
        )

        weighted_values.append(value)

        weighted_weights.append(weight)

    if not weighted_values:
        return np.nan

    return float(
        np.average(
            weighted_values,
            weights=weighted_weights,
        )
    )

def create_forecast_for_date(
    target_date,
    horizon_days,
    bookings,
    daily_actuals,
    duration_distribution,
    weights_df,
    parameters_df,
    config,
):
    """
    Create entry and exit forecasts for one target date
    and one forecast horizon.
    """

    target_date = pd.Timestamp(
        target_date
    ).normalize()

    cutoff_timestamp = (
        target_date
        -
        pd.Timedelta(
            days=horizon_days
        )
        +
        pd.Timedelta(hours=7)
    )

    entry_params = (
        get_horizon_parameters(
            parameters_df,
            "entry",
            horizon_days,
        )
    )

    exit_params = (
        get_horizon_parameters(
            parameters_df,
            "exit",
            horizon_days,
        )
    )

    entry_weights = (
        get_horizon_weights(
            weights_df,
            "entry",
            horizon_days,
        )
    )

    exit_weights = (
        get_horizon_weights(
            weights_df,
            "exit",
            horizon_days,
        )
    )

    # ============================================================
    # ENTRY COMPONENTS
    # ============================================================

    entry_booking = (
        calculate_entry_booking_component(
            bookings=bookings,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
        )
    )

    entry_weekday = (
        calculate_same_weekday_component(
            daily_actuals=daily_actuals,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            target_col="entries",
            history_length=int(
                entry_params[
                    "same_weekday_history"
                ]
            ),
        )
    )

    entry_month = (
        calculate_weekday_month_component(
            daily_actuals=daily_actuals,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            target_col="entries",
            max_history=config[
                "weekday_month_history"
            ],
        )
    )

    entry_trend = (
        calculate_entry_trend_component(
            daily_actuals=daily_actuals,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            config=config,
        )
    )

    entry_historical = (
        entry_trend
        if pd.notna(entry_trend)
        else entry_month
    )

    entry_booking_shrunk = (
        apply_booking_shrinkage(
            booking_forecast=entry_booking,
            historical_forecast=entry_historical,
            known_bookings=entry_booking,
            shrinkage_strength=
            entry_params[
                "booking_shrinkage_strength"
            ],
        )
    )

    # ============================================================
    # EXIT COMPONENTS
    # ============================================================

    exit_booking = (
        calculate_exit_booking_component(
            bookings=bookings,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
        )
    )

    exit_duration = (
        calculate_exit_duration_component(
            duration_distribution=
                duration_distribution,
            bookings=bookings,
            target_date=target_date,
        )
    )

    exit_weekday = (
        calculate_same_weekday_component(
            daily_actuals=daily_actuals,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            target_col="exits",
            history_length=int(
                exit_params[
                    "same_weekday_history"
                ]
            ),
        )
    )

    exit_trend = (
        calculate_exit_trend_component(
            daily_actuals=daily_actuals,
            target_date=target_date,
            cutoff_timestamp=cutoff_timestamp,
            config=config,
        )
    )

    exit_historical = (
        exit_trend
        if pd.notna(exit_trend)
        else exit_weekday
    )

    exit_booking_shrunk = (
        apply_booking_shrinkage(
            booking_forecast=exit_booking,
            historical_forecast=exit_historical,
            known_bookings=exit_booking,
            shrinkage_strength=
            exit_params[
                "booking_shrinkage_strength"
            ],
        )
    )

    entry_components = {
        "entry_booking_shrunk":
            entry_booking_shrunk,
        "entry_weekday":
            entry_weekday,
        "entry_month":
            entry_month,
        "entry_trend_month":
            entry_trend,
    }

    exit_components = {
        "exit_booking_shrunk":
            exit_booking_shrunk,
        "exit_duration":
            exit_duration,
        "exit_weekday":
            exit_weekday,
        "exit_trend_month":
            exit_trend,
    }

    entry_forecast = (
        combine_entry_components(
            entry_components,
            entry_weights,
        )
    )

    exit_forecast = (
        combine_exit_components(
            exit_components,
            exit_weights,
        )
    )

    return {
        "target_date": target_date,
        "horizon_days": horizon_days,

        "entry_forecast": round(entry_forecast),
        "exit_forecast": round(exit_forecast),

        **entry_components,
        **exit_components,
    }

def build_daily_forecast(
    bookings,
    daily_actuals,
    duration_distribution,
    weights_df,
    parameters_df,
    config,
):
    """
    Generate forecasts for the next N days.
    """

    forecast_rows = []

    today = pd.Timestamp.today().normalize()

    for horizon_days in range(
        0,
        config["forecast_days"] + 1,
    ):

        target_date = (
            today
            + pd.Timedelta(
                days=horizon_days
            )
        )

        forecast_rows.append(
            create_forecast_for_date(
                target_date=target_date,
                horizon_days=horizon_days,
                bookings=bookings,
                daily_actuals=daily_actuals,
                duration_distribution=
                    duration_distribution,
                weights_df=weights_df,
                parameters_df=parameters_df,
                config=config,
            )
        )

    return pd.DataFrame(
        forecast_rows
    )

# =============================================================================
# 9. HOURLY FORECAST GENERATION
# =============================================================================
def create_entry_hourly_forecast_from_bookings(
    bookings,
    target_date,
    cutoff_timestamp,
):
    """
    Create an hourly entry forecast directly from visible bookings.

    Purpose
    -------
    For short horizons the booked entry timestamp is a stronger signal
    than a generic historical profile.

    Logic
    -----

    1. Keep bookings visible at the forecast cutoff.
    2. Filter to the target entry date.
    3. Group by booked entry hour.
    4. Forecast entries by hour.
    """

    active = get_bookings_active_as_of(
        bookings=bookings,
        cutoff_timestamp=cutoff_timestamp,
    )

    target_date = pd.Timestamp(
        target_date
    ).normalize()

    entries = active[
        active["entryDate"]
        .dt.normalize()
        .eq(target_date)
    ].copy()

    if entries.empty:
        return pd.DataFrame()

    entries["hour"] = (
        entries["entryDate"]
        .dt.hour
    )

    forecast = (
        entries
        .groupby("hour")
        .agg(
            booked_entries=(
                "bookingId",
                "nunique",
            )
        )
        .reset_index()
    )

    return forecast

def create_exit_hourly_forecast_from_expected_returns(
    operations,
    target_date,
):
    """
    Create an hourly exit forecast from ExpectedReturnDate.

    Purpose
    -------
    For short horizons the operational return timestamp contains more
    information than a historical profile.

    Logic
    -----

    1. Find bookings returning on the target date.
    2. Group by expected return hour.
    3. Forecast exits by hour.
    """

    target_date = pd.Timestamp(
        target_date
    ).normalize()

    ops = operations.copy()

    ops = ops[
        ops["ExpectedReturnDate"]
        .notna()
    ]

    ops = ops[
        ops["ExpectedReturnDate"]
        .dt.normalize()
        .eq(target_date)
    ]

    if ops.empty:
        return pd.DataFrame()

    ops["hour"] = (
        ops["ExpectedReturnDate"]
        .dt.hour
    )

    forecast = (
        ops
        .groupby("hour")
        .agg(
            expected_returns=(
                "BookingReference",
                "nunique",
            )
        )
        .reset_index()
    )

    return forecast

def build_hourly_profile(
    hourly_actuals,
    target_date,
    flow,
    history_weeks,
):
    """
    Historical hourly profile.

    Used when booking visibility becomes weak.
    """

    target_date = pd.Timestamp(
        target_date
    ).normalize()

    weekday = target_date.weekday()

    demand_col = (
        "entries"
        if flow == "entry"
        else "exits"
    )

    history = hourly_actuals[
        hourly_actuals["date"]
        .lt(target_date)
        &
        hourly_actuals["weekday_num"]
        .eq(weekday)
    ]

    dates = (
        history["date"]
        .drop_duplicates()
        .sort_values()
        .tail(history_weeks)
    )

    history = history[
        history["date"].isin(dates)
    ]

    hourly = (
        history
        .groupby("hour")[demand_col]
        .sum()
        .reindex(range(24), fill_value=0)
    )

    if hourly.sum() <= 0:
        return None

    return (
        hourly
        / hourly.sum()
    ).to_dict()

def distribute_daily_forecast(
    daily_forecast,
    profile,
):
    """
    Convert daily demand into hourly demand.
    """

    result = {}

    for hour in range(24):

        result[hour] = (
            daily_forecast
            * profile.get(hour, 0)
        )

    return result
