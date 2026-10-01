# =============================================================================
# 1. IMPORTS
# =============================================================================
# Standard library imports
import pathlib
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

# Third-party library imports
import numpy as np
import pandas as pd

# Project-specific utility imports
# Assuming modules.utils are available in sys.path or directly importable
# (These would be the same as in your simulation scripts)
from modules.utils.db import get_engine
from modules.utils.progress import step # Or a simple print for production

# =============================================================================
# 2. CONFIGURATION
# =============================================================================

def get_forecast_config():
    """
    Production forecast configuration.

    This script should not contain forecasting logic inside the
    configuration. Forecast logic is derived from forecast_weights.csv
    and forecast_parameters.csv, which are generated from the simulation
    framework.
    """
    base_dir = Path(__file__).resolve().parent

    return {
        "history_start": "2024-01-01", # Needs to be far enough back for all historical components
        "forecast_days": 56, # The operational planning horizon
        "maximum_duration_days": 35, # For duration distribution clipping

        # Configuration files - these paths must be correct
        "weights_file": base_dir / "forecast_weights.csv",
        "parameters_file": base_dir / "forecast_parameters.csv",

        # Product filtering
        "asset_name": "FastPark",

        # Output folder
        "output_folder": base_dir / "Forecast Outputs",

        # (Existing Capping Parameters - if these are still needed as post-processing)
        "peak_cap_value": 350.0,
        "peak_hours_per_peak": 4,
        "peak_hourly_caps": {3: 80, 4: 140, 5: 80, 10: 80, 11: 140, 12: 80},
        "peaks_definition": {
            "Peak 1": {"label": "03:00-06:00", "hours": [3, 4, 5]},
            "Peak 2": {"label": "10:00-13:00", "hours": [10, 11, 12]},
        },
        "daily_caps": { # Example, these would need to be dynamically managed or loaded
            pd.Timestamp("2026-09-16").date(): 678,
            pd.Timestamp("2026-09-17").date(): 683,
            pd.Timestamp("2026-09-18").date(): 769,
            pd.Timestamp("2026-09-19").date(): 877,
        },
        "cap_end_date": pd.Timestamp("2026-10-31"), # Date until which caps are applied
    }

# =============================================================================
# 3. LOAD CALIBRATED WEIGHTS AND PARAMETERS
# =============================================================================
# These functions are copied directly from your `create_forecast_weights.py`
# and `create_forecast_parameters.py` scripts.

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
        # Fallback to a default if no specific weights are found (e.g., for very far horizons not in simulation)
        # Or raise an error if strict adherence is required.
        print(f"Warning: No weights found for {demand_type} T-{horizon_days}. Using default equal weights.")
        component_cols = [col for col in weights_df.columns if col.startswith(f"{demand_type}_")]
        num_components = len(component_cols)
        if num_components > 0:
            default_weight = 1.0 / num_components
            default_weights = {col: default_weight for col in component_cols}
            default_weights["demand_type"] = demand_type
            default_weights["horizon_days"] = horizon_days
            return default_weights
        return {} # Return empty if no components at all

    return result.iloc[0].to_dict()

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
# 4. DATA EXTRACTION AND PREPARATION (FROM ORIGINAL ANALYSIS SCRIPT)
# =============================================================================
# These functions are adapted from `01 Historical Analysis/fastpark_forecast_analysis.py`
# or similar data loading sections in the simulation scripts.
# The `get_bookings`, `get_operations`, `get_flights` from your existing forecast script are
# good starting points, but ensure they pull ALL necessary history, not just recent.

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
# 5. HISTORICAL DATASET CREATION
# =============================================================================
# These functions prepare the historical context for component calculation.

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
# 6. FORECAST COMPONENTS (ADAPTED FROM SIMULATION SCRIPTS)
# =============================================================================
# These functions calculate the value of each independent signal for a given
# target date and cutoff. They are adapted from the `forecast_entry_scenario`
# and `forecast_exit_scenario` helpers in Generation 1/2.

# Helper for safe mean (from Gen1/2)
def safe_mean(values):
    """Mean after removing missing and infinite values."""
    values = pd.Series(values, dtype=float).replace([np.inf, -np.inf], np.nan).dropna()
    if values.empty: return np.nan
    return float(values.mean())

# Helper for clipping (from Gen1/2)
def clip_forecast(value):
    """Return a non-negative float or NaN."""
    if value is None or pd.isna(value): return np.nan
    return max(0.0, float(value))

def get_bookings_active_as_of(bookings, cutoff_timestamp):
    """Return bookings active at the historical forecast cutoff."""
    # ... (copy exact code from `02 Simulation Gen 1/fastpark_forecast_simulation.py`) ...
    cutoff_timestamp = pd.Timestamp(cutoff_timestamp)
    active_mask = (bookings["createdAt"].le(cutoff_timestamp) & (bookings["cancelledAt"].isna() | bookings["cancelledAt"].gt(cutoff_timestamp)))
    return bookings.loc[active_mask].copy()

def calculate_entry_booking_component(bookings, target_date, cutoff_timestamp):
    """Visible bookings for the target entry date."""
    # EVIDENCE: "Booking visibility was identified as a primary forecasting signal." (Historical Analysis, Finding 1 Decision)
    # EVIDENCE: "Short Horizons (T-0 to T-6): Booking Visibility (Model E11) consistently dominated." (Generation 1, Entry Forecasting Results)
    active = get_bookings_active_as_of(bookings=bookings, cutoff_timestamp=cutoff_timestamp)
    count = active[active["entryDate"].dt.normalize().eq(pd.Timestamp(target_date).normalize())]["bookingId"].nunique()
    return float(count)

def calculate_exit_booking_component(bookings, target_date, cutoff_timestamp):
    """Visible bookings for the target exit date."""
    # EVIDENCE: "Exit visibility consistently exhibited higher visibility than entry demand." (Historical Analysis, Finding 1 Evidence)
    # EVIDENCE: "Short Horizons (T-0 to T-2): Exit Booking Visibility (Model X11) was the dominant forecasting signal." (Generation 1, Exit Forecasting Results)
    active = get_bookings_active_as_of(bookings=bookings, cutoff_timestamp=cutoff_timestamp)
    count = active[active["exitDate"].dt.normalize().eq(pd.Timestamp(target_date).normalize())]["bookingId"].nunique()
    return float(count)

def calculate_same_weekday_component(daily_actuals, target_date, cutoff_timestamp, target_col, history_length):
    """Forecast using previous matching weekdays."""
    # EVIDENCE: "Medium Horizons (T-7 to T-21): Booking visibility alone became sub-optimal. Winning models combined Booking Visibility with Historical Weekday Behavior (Model E12)." (Generation 1, Entry Forecasting Results)
    # EVIDENCE: `same_weekday_history` parameter from `forecast_parameters.csv` is calibrated for optimal lookback.
    target_date = pd.Timestamp(target_date).normalize()
    cutoff_date = pd.Timestamp(cutoff_timestamp).normalize()
    weekday = target_date.weekday()
    history = daily_actuals[
        daily_actuals["date"].lt(cutoff_date) & daily_actuals["date"].dt.weekday.eq(weekday)
    ].sort_values("date").tail(history_length)
    if len(history) < history_length: return np.nan
    return float(history[target_col].mean())

def calculate_weekday_month_component(daily_actuals, target_date, cutoff_timestamp, target_col, max_history):
    """Same weekday + same month forecast."""
    # EVIDENCE: "Long Horizons (T-28 to T-56): As visibility further weakened, seasonality became the dominant factor. Models like E14 (Weekday-Month Historical) and E15 (Weekday-Month + Booking Curve) performed best." (Generation 1, Entry Forecasting Results)
    # EVIDENCE: G2 entry weights show increasing contribution of Month and Weekday at longer horizons.
    target_date = pd.Timestamp(target_date).normalize()
    cutoff_date = pd.Timestamp(cutoff_timestamp).normalize()
    target_month = target_date.month
    target_weekday = target_date.weekday()
    history = daily_actuals[
        daily_actuals["date"].lt(cutoff_date) & daily_actuals["month"].eq(target_month) & daily_actuals["weekday_num"].eq(target_weekday)
    ].sort_values("date").tail(max_history)
    if history.empty: return np.nan
    return float(history[target_col].mean())

def calculate_trend_factor(daily_actuals, cutoff_timestamp, target_col, config):
    """Calculates the trend adjustment factor."""
    # EVIDENCE: G2 entry weights show Trend becoming the largest signal at T-42 and T-49.
    # EVIDENCE: G2 exit weights show Trend becoming the largest signal at T-42.
    # These are global config settings, not per-horizon parameters.
    cutoff_date = pd.Timestamp(cutoff_timestamp).normalize()
    recent_start = cutoff_date - pd.Timedelta(days=config["trend_recent_days"])
    comparison_start = recent_start - pd.Timedelta(days=config["trend_comparison_days"])
    recent = daily_actuals[daily_actuals["date"].between(recent_start, cutoff_date, inclusive="left")][target_col]
    comparison = daily_actuals[daily_actuals["date"].between(comparison_start, recent_start, inclusive="left")][target_col]
    if len(recent) == 0 or len(comparison) == 0: return 1.0
    comparison_mean = comparison.mean()
    if comparison_mean <= 0: return 1.0
    factor = recent.mean() / comparison_mean
    return float(np.clip(factor, config["trend_factor_min"], config["trend_factor_max"]))

def calculate_entry_trend_component(daily_actuals, target_date, cutoff_timestamp, config):
    """Trend-adjusted seasonal entry forecast."""
    # Combines `weekday_month_component` with `trend_factor`
    month_component = calculate_weekday_month_component(daily_actuals=daily_actuals, target_date=target_date, cutoff_timestamp=cutoff_timestamp, target_col="entries", max_history=config["weekday_month_max_history"]) # Renamed for clarity
    if pd.isna(month_component): return np.nan
    trend_factor = calculate_trend_factor(daily_actuals=daily_actuals, cutoff_timestamp=cutoff_timestamp, target_col="entries", config=config)
    return month_component * trend_factor

def calculate_exit_trend_component(daily_actuals, target_date, cutoff_timestamp, config):
    """Trend-adjusted seasonal exit forecast."""
    # Combines `weekday_month_component` with `trend_factor`
    month_component = calculate_weekday_month_component(daily_actuals=daily_actuals, target_date=target_date, cutoff_timestamp=cutoff_timestamp, target_col="exits", max_history=config["weekday_month_max_history"]) # Renamed for clarity
    if pd.isna(month_component): return np.nan
    trend_factor = calculate_trend_factor(daily_actuals=daily_actuals, cutoff_timestamp=cutoff_timestamp, target_col="exits", config=config)
    return month_component * trend_factor

def calculate_exit_duration_component(duration_distribution, bookings, target_date, cutoff_timestamp):
    """
    Expected exits generated from historical duration behaviour.
    Uses bookings active as of cutoff, and planned duration.
    """
    # EVIDENCE: "Duration is both observable before the event and predictive of the event, making it one of the strongest forecasting candidates." (Historical Analysis, Finding 4 Interpretation)
    # EVIDENCE: "Once horizons extended beyond T-2, the dominant signal became a combination of Exit Visibility + Duration (Model X12)." (Generation 1, Exit Forecasting Results)
    # EVIDENCE: G2 exit weights show Duration being the dominant signal at many short and medium horizons, and re-emerging at long horizons.
    target_date = pd.Timestamp(target_date).normalize()
    cutoff_timestamp = pd.Timestamp(cutoff_timestamp)

    active_bookings = get_bookings_active_as_of(bookings, cutoff_timestamp)

    # Calculate planned duration for active bookings
    active_bookings["planned_duration_days"] = (
        (active_bookings["exitDate"] - active_bookings["entryDate"]).dt.total_seconds() / 86400
    ).round().astype("Int64") # Use Int64 for nullable integer

    forecast_value = 0.0
    # Iterate through active bookings for the current target date
    # Sum the probability of each booking's planned duration resulting in an exit on target_date
    for _, booking_row in active_bookings.iterrows():
        entry_date = booking_row["entryDate"].normalize()
        planned_duration = booking_row["planned_duration_days"]

        # Check if this booking's planned exit date matches the target_date
        if (entry_date + pd.Timedelta(days=planned_duration)).normalize() == target_date:
            # If the planned duration matches, this booking contributes to the forecast
            # In a simplified model, each such booking contributes 1 to the count.
            # A more advanced model might use the `duration_distribution` to weight this.
            # For now, let's just count them as a direct match.
            forecast_value += 1

    return float(forecast_value)



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
    Applies booking shrinkage to blend booking and historical forecasts.
    """
    if pd.isna(booking_forecast):
        return historical_forecast
    if pd.isna(historical_forecast):
        return booking_forecast

    known_bookings = max(0.0, float(known_bookings))
    shrinkage_strength = max(0.0, float(shrinkage_strength))

    if shrinkage_strength == 0:
        return clip_forecast(booking_forecast)

    reliability = known_bookings / (known_bookings + shrinkage_strength)

    return clip_forecast(
        reliability * booking_forecast + (1.0 - reliability) * historical_forecast
    )


def combine_entry_components(
    components,
    weights,
):
    """
    Combines entry forecasting components using calibrated weights.
    """
    component_values = {
        "entry_booking_shrunk": components.get("entry_booking_shrunk"),
        "entry_weekday": components.get("entry_weekday"),
        "entry_month": components.get("entry_month"),
        "entry_trend_month": components.get("entry_trend_month"),
    }

    weighted_values = []
    weighted_weights = []

    for component_name, value in component_values.items():
        if pd.isna(value):
            continue
        weight = weights.get(component_name, 0)
        weighted_values.append(value)
        weighted_weights.append(weight)

    if not weighted_values:
        return np.nan

    return float(np.average(weighted_values, weights=weighted_weights))


def combine_exit_components(
    components,
    weights,
):
    """
    Combines exit forecasting components using calibrated weights.
    """
    component_values = {
        "exit_booking_shrunk": components.get("exit_booking_shrunk"),
        "exit_duration": components.get("exit_duration"),
        "exit_weekday": components.get("exit_weekday"),
        "exit_trend_month": components.get("exit_trend_month"),
    }

    weighted_values = []
    weighted_weights = []

    for component_name, value in component_values.items():
        if pd.isna(value):
            continue
        weight = weights.get(component_name, 0)
        weighted_values.append(value)
        weighted_weights.append(weight)

    if not weighted_values:
        return np.nan

    return float(np.average(weighted_values, weights=weighted_weights))


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
    Creates entry and exit forecasts for one target date and one forecast horizon
    using the calibrated ensemble model.
    """
    target_date = pd.Timestamp(target_date).normalize()
    cutoff_timestamp = target_date - pd.Timedelta(days=horizon_days) + pd.Timedelta(hours=7)

    entry_params = get_horizon_parameters(parameters_df, "entry", horizon_days)
    exit_params = get_horizon_parameters(parameters_df, "exit", horizon_days)
    entry_weights = get_horizon_weights(weights_df, "entry", horizon_days)
    exit_weights = get_horizon_weights(weights_df, "exit", horizon_days)

    # --- ENTRY COMPONENTS ---
    # Raw booking count for shrinkage input
    raw_entry_booking_count = calculate_entry_booking_component(bookings, target_date, cutoff_timestamp)

    entry_weekday = calculate_same_weekday_component(daily_actuals, target_date, cutoff_timestamp, "entries", int(entry_params["same_weekday_history"]))
    entry_month = calculate_weekday_month_component(daily_actuals, target_date, cutoff_timestamp, "entries", config["weekday_month_max_history"])
    entry_trend_historical = calculate_entry_trend_component(daily_actuals, target_date, cutoff_timestamp, config) # This is the "historical_forecast" for shrinkage

    entry_booking_shrunk = apply_booking_shrinkage(
        booking_forecast=raw_entry_booking_count,
        historical_forecast=entry_trend_historical,
        known_bookings=raw_entry_booking_count,
        shrinkage_strength=entry_params["booking_shrinkage_strength"]
    )

    # --- EXIT COMPONENTS ---
    # Raw booking count for shrinkage input
    raw_exit_booking_count = calculate_exit_booking_component(bookings, target_date, cutoff_timestamp)

    exit_duration = calculate_exit_duration_component(duration_distribution, bookings, target_date)
    exit_weekday = calculate_same_weekday_component(daily_actuals, target_date, cutoff_timestamp, "exits", int(exit_params["same_weekday_history"]))
    exit_month = calculate_weekday_month_component(daily_actuals, target_date, cutoff_timestamp, "exits", config["weekday_month_max_history"])
    exit_trend_historical = calculate_exit_trend_component(daily_actuals, target_date, cutoff_timestamp, config)

    exit_booking_shrunk = apply_booking_shrinkage(
        booking_forecast=raw_exit_booking_count,
        historical_forecast=exit_trend_historical,
        known_bookings=raw_exit_booking_count,
        shrinkage_strength=exit_params["booking_shrinkage_strength"]
    )

    entry_components = {
        "entry_booking_shrunk": entry_booking_shrunk,
        "entry_weekday": entry_weekday,
        "entry_month": entry_month,
        "entry_trend_month": entry_trend_historical,
    }

    exit_components = {
        "exit_booking_shrunk": exit_booking_shrunk,
        "exit_duration": exit_duration,
        "exit_weekday": exit_weekday,
        "exit_trend_month": exit_trend_historical,
    }

    entry_forecast = combine_entry_components(entry_components, entry_weights)
    exit_forecast = combine_exit_components(exit_components, exit_weights)

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
    Creates daily forecasts for the next N days using the calibrated ensemble.
    """
    forecast_rows = []
    today = pd.Timestamp.today().normalize()

    for horizon_days in range(config["forecast_days"] + 1):
        target_date = today + pd.Timedelta(days=horizon_days)

        forecast_row = create_forecast_for_date(
            target_date=target_date,
            horizon_days=horizon_days,
            bookings=bookings,
            daily_actuals=daily_actuals,
            duration_distribution=duration_distribution,
            weights_df=weights_df,
            parameters_df=parameters_df,
            config=config,
        )
        forecast_rows.append(forecast_row)

    return pd.DataFrame(forecast_rows)


# =============================================================================
# 9. HOURLY FORECAST GENERATION
# =============================================================================

def create_entry_hourly_forecast_from_bookings(
    bookings,
    target_date,
    cutoff_timestamp,
):
    """Creates hourly entry forecast from visible bookings."""
    active = get_bookings_active_as_of(bookings, cutoff_timestamp)
    target_date = pd.Timestamp(target_date).normalize()
    entries = active[active["entryDate"].dt.normalize().eq(target_date)].copy()
    if entries.empty: return pd.DataFrame()
    entries["hour"] = entries["entryDate"].dt.hour
    forecast = entries.groupby("hour").agg(entries=("bookingId", "nunique")).reset_index()
    return forecast

def create_exit_hourly_forecast_from_expected_returns(
    operations,
    target_date,
):
    """Creates hourly exit forecast from ExpectedReturnDate."""
    target_date = pd.Timestamp(target_date).normalize()
    ops = operations.copy()
    ops = ops[ops["ExpectedReturnDate"].notna() & ops["ExpectedReturnDate"].dt.normalize().eq(target_date)]
    if ops.empty: return pd.DataFrame()
    ops["hour"] = ops["ExpectedReturnDate"].dt.hour
    forecast = ops.groupby("hour").agg(exits=("BookingReference", "nunique")).reset_index()
    return forecast

def build_hourly_profile(
    hourly_actuals,
    target_date,
    flow,
    history_weeks,
):
    """
    Historical hourly profile based on same weekday demand.
    """
    target_date = pd.Timestamp(target_date).normalize()
    weekday = target_date.weekday()
    demand_col = "entries" if flow == "entry" else "exits"
    history = hourly_actuals[
        hourly_actuals["date"].lt(target_date) & hourly_actuals["weekday_num"].eq(weekday)
    ].sort_values("date")
    dates = history["date"].drop_duplicates().sort_values().tail(history_weeks)
    if len(dates) < history_weeks: return None
    history = history[history["date"].isin(dates)]
    hourly_totals = history.groupby("hour")[demand_col].sum().reindex(range(24), fill_value=0).astype(float)
    if hourly_totals.sum() <= 0: return None
    profile = (hourly_totals / hourly_totals.sum())
    if not np.isclose(profile.sum(), 1.0, atol=1e-10): raise ValueError("Hourly profile does not sum to 1.")
    return profile.to_dict()

def distribute_daily_to_hourly(daily_forecast, hourly_profile):
    """Distributes a daily forecast across hours using the hourly profile."""
    if hourly_profile is None or pd.isna(daily_forecast): return None
    hourly_forecast = {}
    for hour in range(24):
        proportion = hourly_profile.get(hour, 0.0)
        hourly_forecast[hour] = daily_forecast * proportion
    return hourly_forecast

def create_hourly_forecast_for_date(
    target_date,
    horizon_days,
    bookings,
    daily_actuals,
    hourly_actuals,
    daily_forecast_entry,
    daily_forecast_exit,
    config,
):
    """Creates complete hourly forecasts for entries and exits on a target date."""
    target_date = pd.Timestamp(target_date).normalize()
    cutoff_timestamp = target_date - pd.Timedelta(days=horizon_days) + pd.Timedelta(hours=7)

    # Calculate hourly profiles
    entry_profile = build_hourly_profile(hourly_actuals, target_date, "entry", config["hourly_profile_windows"][0]) # Assuming first window is default
    exit_profile = build_hourly_profile(hourly_actuals, target_date, "exit", config["hourly_profile_windows"][0]) # Assuming first window is default

    # Distribute daily forecasts
    hourly_entries = distribute_daily_to_hourly(daily_forecast_entry, entry_profile)
    hourly_exits = distribute_daily_to_hourly(daily_forecast_exit, exit_profile)

    # Create output rows
    rows = []
    for hour in range(24):
        hour_datetime = target_date + pd.Timedelta(hours=hour)
        rows.append({
            "target_date": target_date,
            "hour": hour,
            "datetime": hour_datetime,
            "horizon_days": horizon_days,
            "profile_weeks": config["hourly_profile_windows"][0], # Storing the default used
            "forecast_entries": (hourly_entries.get(hour) if hourly_entries else np.nan),
            "forecast_exits": (hourly_exits.get(hour) if hourly_exits else np.nan),
            "entry_profile_pct": (entry_profile.get(hour, 0.0) * 100 if entry_profile else np.nan),
            "exit_profile_pct": (exit_profile.get(hour, 0.0) * 100 if exit_profile else np.nan),
        })
    return pd.DataFrame(rows)

# =============================================================================
# 10. EXPORT FINAL OUTPUTS
# =============================================================================

def export_forecast_outputs(
    daily_forecast_df,
    hourly_forecast_df,
    config,
):
    """Exports daily and hourly forecasts to CSV files."""
    output_folder = Path(config["output_folder"])
    output_folder.mkdir(parents=True, exist_ok=True)

    daily_path = output_folder / "FastPark_Daily_Forecast.csv"
    hourly_path = output_folder / "FastPark_Hourly_Forecast.csv"

    daily_forecast_df.to_csv(daily_path, index=False)
    hourly_forecast_df.to_csv(hourly_path, index=False)

    print(f"\nDaily forecast exported to: {daily_path}")
    print(f"Hourly forecast exported to: {hourly_path}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    t0 = time.perf_counter()
    print("=" * 80)
    print("FASTPARK PRODUCTION FORECAST")
    print("=" * 80)

    config = get_forecast_config()
    engine = get_engine()

    # Load calibrated weights and parameters
    weights_df = load_forecast_weights(config["weights_file"])
    parameters_df = load_forecast_parameters(config["parameters_file"])

    t1 = step(t0, "Loaded weights and parameters")

    # Load necessary historical data
    historical_data = load_forecast_data(config)
    bookings = historical_data["bookings"]
    operations = historical_data["operations"]
    flights = historical_data["flights"]

    t2 = step(t1, "Loaded historical data")

    # Prepare historical datasets for component calculation
    daily_actuals = create_daily_actuals(operations)
    hourly_actuals = create_hourly_actuals(operations)
    duration_distribution = create_duration_distribution(bookings, config)

    t3 = step(t2, "Prepared historical datasets")

    # Generate daily forecasts
    print("\nGenerating daily forecasts...")
    daily_forecasts = build_daily_forecast(
        bookings=bookings,
        daily_actuals=daily_actuals,
        duration_distribution=duration_distribution,
        weights_df=weights_df,
        parameters_df=parameters_df,
        config=config,
    )
    t4 = step(t3, f"Generated daily forecasts ({len(daily_forecasts):,} rows)")

    # Generate hourly forecasts
    print("\nGenerating hourly forecasts...")
    all_horizons = range(config["forecast_days"] + 1) # 0 to 56 days

    hourly_forecast_rows = []
    for _, daily_row in daily_forecasts.iterrows():
        target_date = daily_row['target_date']
        horizon_days = daily_row['horizon_days']

        # Get parameters and weights for the specific horizon and flow
        entry_params = get_horizon_parameters(parameters_df, "entry", horizon_days)
        exit_params = get_horizon_parameters(parameters_df, "exit", horizon_days)
        entry_weights = get_horizon_weights(weights_df, "entry", horizon_days)
        exit_weights = get_horizon_weights(weights_df, "exit", horizon_days)

        # Retrieve components needed for hourly forecasting (simplified here, real implementation would use more)
        # For simplicity, we'll use the daily forecast values as proxies for component contribution for hourly split
        daily_entry_forecast = daily_row['entry_forecast']
        daily_exit_forecast = daily_row['exit_forecast']

        # Use the default hourly profile window from parameters if available, otherwise a default
        hourly_profile_window = entry_params.get('hourly_profile_window', 6) # Default to 6 weeks

        hourly_entries = create_entry_hourly_forecast_from_bookings(
            bookings=bookings,
            target_date=target_date,
            cutoff_timestamp=target_date - pd.Timedelta(days=horizon_days) + pd.Timedelta(hours=7)
        )
        # Note: For production, `create_entry_hourly_forecast_from_bookings` and `create_exit_hourly_forecast_from_expected_returns`
        # might be used as components *within* the hourly forecast, rather than the sole source.
        # The current logic uses hourly profiles to distribute daily forecasts.

        hourly_exits = create_exit_hourly_forecast_from_expected_returns(
            operations=operations,
            target_date=target_date,
        )
        # If we need to distribute the combined daily forecast using profiles:
        hourly_entries_distributed = distribute_daily_to_hourly(daily_entry_forecast, build_hourly_profile(hourly_actuals, target_date, "entry", hourly_profile_window))
        hourly_exits_distributed = distribute_daily_to_hourly(daily_forecast_exit, build_hourly_profile(hourly_actuals, target_date, "exit", hourly_profile_windows=6)) # Assuming default window if not specified

        for hour in range(24):
            hourly_forecast_rows.append({
                "target_date": target_date,
                "horizon_days": horizon_days,
                "hour": hour,
                "datetime": target_date + pd.Timedelta(hours=hour),
                "entry_forecast": hourly_entries_distributed.get(hour) if hourly_entries_distributed else np.nan,
                "exit_forecast": hourly_exits_distributed.get(hour) if hourly_exits_distributed else np.nan,
                "entry_profile_pct": (build_hourly_profile(hourly_actuals, target_date, "entry", hourly_profile_window).get(hour, 0.0) * 100 if build_hourly_profile(hourly_actuals, target_date, "entry", hourly_profile_window) else np.nan),
                "exit_profile_pct": (build_hourly_profile(hourly_actuals, target_date, "exit", hourly_profile_window).get(hour, 0.0) * 100 if build_hourly_profile(hourly_actuals, target_date, "exit", hourly_profile_window) else np.nan),
            })

    hourly_forecast_df = pd.DataFrame(hourly_forecast_rows)
    hourly_forecast_df["datetime"] = pd.to_datetime(hourly_forecast_df["datetime"])
    hourly_forecast_df = hourly_forecast_df.set_index("datetime")

    t5 = step(t4, f"Generated hourly forecasts ({len(hourly_forecast_df):,} rows)")

    # =============================================================================
    # 10. EXPORT FINAL OUTPUTS
    # =============================================================================
    export_forecast_outputs(
        daily_forecast_df=daily_forecasts,
        hourly_forecast_df=hourly_forecast_df,
        config=config,
    )

    t6 = step(t5, "Exported final forecast files")

    print("\nProduction forecast generation complete.")
    print(f"Total runtime: {t6 - t0:.2f} seconds")

