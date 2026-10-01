"""
FASTPARK SHORT TERM FORECAST 2026
=================================

Production Forecast Engine

Methodology Source
------------------
Historical Analysis
Generation 1
Generation 2
Generation 2.1

This script operationalises:

    - Booking Visibility
    - Booking Pace
    - Cancellation Behaviour
    - Same Weekday Demand
    - Weekday-Month Seasonality
    - Trend-Adjusted Seasonality
    - Duration Modelling
    - Booking Shrinkage
    - Horizon-Specific Ensemble Weights
    - Horizon-Specific Parameters
    - Horizon-Specific Hourly Profiles

The script does NOT:

    - optimise weights
    - tune parameters
    - backtest models

Those tasks belong to:

    create_forecast_weights.py
    create_forecast_parameters.py

Outputs
-------

Daily Forecast
Hourly Forecast
Component Audit

"""

# =============================================================================
# IMPORTS
# =============================================================================

from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

from modules.utils.db import get_engine

# =============================================================================
# CONFIGURATION
# =============================================================================

def get_forecast_config():

    base_dir = Path(__file__).resolve().parent

    return {

        "history_start":
            "2024-01-01",

        "forecast_days":
            56,

        "asset_name":
            "FastPark",

        "maximum_duration_days":
            35,

        "trend_recent_days":
            28,

        "trend_comparison_days":
            84,

        "trend_factor_min":
            0.70,

        "trend_factor_max":
            1.30,

        "weights_file":
            base_dir / "forecast_weights.csv",

        "parameters_file":
            base_dir / "forecast_parameters.csv",

        "output_folder":
            base_dir / "Forecast Outputs",

        "export_mode":
            "legacy"      # legacy / modern

    }

# =============================================================================
# WEIGHTS
# =============================================================================

def load_forecast_weights(path):

    return (
        pd.read_csv(path)
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

    row = weights_df[
        weights_df["demand_type"].eq(demand_type)
        &
        weights_df["horizon_days"].eq(horizon_days)
    ]

    if row.empty:

        raise ValueError(
            f"No weights found for "
            f"{demand_type} T-{horizon_days}"
        )

    return row.iloc[0].to_dict()

# =============================================================================
# PARAMETERS
# =============================================================================

def load_forecast_parameters(path):

    return (
        pd.read_csv(path)
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

    row = parameters_df[
        parameters_df["flow"].eq(flow)
        &
        parameters_df["horizon_days"].eq(horizon_days)
    ]

    if row.empty:

        raise ValueError(
            f"No parameters found "
            f"for {flow} T-{horizon_days}"
        )

    return row.iloc[0].to_dict()

# =============================================================================
# DATA
# =============================================================================

def get_bookings(
    engine,
    config,
):

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

    df = pd.read_sql(
        sql,
        engine,
        params=[
            config["asset_name"]
        ]
    )

    date_cols = [
        "createdAt",
        "cancelledAt",
        "entryDate",
        "exitDate",
    ]

    for col in date_cols:

        df[col] = pd.to_datetime(
            df[col],
            errors="coerce"
        )

    return df

def get_operations(engine):

    sql = """
    SELECT
        BookingReference,
        CheckInStarted,
        CheckInEnded,
        ExpectedReturnDate,
        ActualCheckedOutDate
    FROM FastPark.v_EntryAndExits
    """

    df = pd.read_sql(
        sql,
        engine,
    )

    date_cols = [
        "CheckInStarted",
        "CheckInEnded",
        "ExpectedReturnDate",
        "ActualCheckedOutDate",
    ]

    for col in date_cols:

        df[col] = pd.to_datetime(
            df[col],
            errors="coerce"
        )

    return df

# =============================================================================
# ACTUALS
# =============================================================================

def create_daily_actuals(operations):

    entries = (
        operations
        .dropna(
            subset=["CheckInEnded"]
        )
        .assign(
            date=lambda x:
            x["CheckInEnded"]
            .dt.normalize()
        )
        .groupby(
            "date"
        )[
            "BookingReference"
        ]
        .nunique()
        .rename("entries")
    )

    exits = (
        operations
        .dropna(
            subset=["ActualCheckedOutDate"]
        )
        .assign(
            date=lambda x:
            x["ActualCheckedOutDate"]
            .dt.normalize()
        )
        .groupby(
            "date"
        )[
            "BookingReference"
        ]
        .nunique()
        .rename("exits")
    )

    daily = (
        pd.concat(
            [
                entries,
                exits,
            ],
            axis=1,
        )
        .fillna(0)
        .reset_index()
    )

    daily["weekday_num"] = (
        daily["date"]
        .dt.weekday
    )

    daily["month"] = (
        daily["date"]
        .dt.month
    )

    return daily

# =============================================================================
# VISIBILITY
# =============================================================================

def get_bookings_active_as_of(
    bookings,
    cutoff_timestamp,
):

    mask = (

        bookings["createdAt"]
        .le(cutoff_timestamp)

        &

        (
            bookings["cancelledAt"]
            .isna()

            |

            bookings["cancelledAt"]
            .gt(cutoff_timestamp)
        )
    )

    return bookings.loc[mask].copy()

def entry_visibility(
    bookings,
    target_date,
    cutoff_timestamp,
):

    active = (
        get_bookings_active_as_of(
            bookings,
            cutoff_timestamp,
        )
    )

    return float(

        active[
            active["entryDate"]
            .dt.normalize()
            .eq(target_date)
        ]["bookingId"]
        .nunique()
    )

def exit_visibility(
    bookings,
    target_date,
    cutoff_timestamp,
):

    active = (
        get_bookings_active_as_of(
            bookings,
            cutoff_timestamp,
        )
    )

    return float(

        active[
            active["exitDate"]
            .dt.normalize()
            .eq(target_date)
        ]["bookingId"]
        .nunique()
    )

def expected_return_daily_component(
    operations,
    target_date,
):
    """
    Near-horizon exit signal.

    Uses ExpectedReturnDate from operational data.

    Generation 1 and legacy production logic showed that
    ExpectedReturnDate contains useful information for
    short-dated exit forecasting.
    """

    target_date = pd.Timestamp(
        target_date
    ).normalize()

    returns = operations[
        operations["ExpectedReturnDate"]
        .notna()
    ]

    returns = returns[
        returns["ExpectedReturnDate"]
        .dt.normalize()
        .eq(target_date)
    ]

    return float(
        returns["BookingReference"]
        .nunique()
    )

# =============================================================================
# BOOKING PACE
# =============================================================================

def booking_pace_factor(
    bookings,
    target_date,
    cutoff_timestamp,
):
    """
    Generation 2.1 pace logic.

    Compares:

        current booking position

    against

        historical booking position at
        the same forecast horizon.
    """

    target_date = pd.Timestamp(
        target_date
    ).normalize()

    cutoff_timestamp = pd.Timestamp(
        cutoff_timestamp
    )

    horizon_days = (
        target_date
        -
        cutoff_timestamp.normalize()
    ).days

    current_visible = (
        get_bookings_active_as_of(
            bookings,
            cutoff_timestamp,
        )
    )

    current_visible = (
        current_visible[
            current_visible["entryDate"]
            .dt.normalize()
            .eq(target_date)
        ]["bookingId"]
        .nunique()
    )

    if horizon_days <= 0:
        return 1.0

    historical_positions = []

    history = bookings.copy()

    history = history[
        history["entryDate"].notna()
    ]

    for _, booking in history.iterrows():

        lead = (
            booking["entryDate"]
            -
            booking["createdAt"]
        ).days

        if lead >= horizon_days:

            historical_positions.append(1)

    expected_position = len(
        historical_positions
    )

    if expected_position <= 0:
        return 1.0

    pace_factor = (
        current_visible
        / expected_position
    )

    return float(
        np.clip(
            pace_factor,
            0.85,
            1.15,
        )
    )

# =============================================================================
# CANCELLATIONS
# =============================================================================

def cancellation_factor(
    bookings,
    cutoff_timestamp,
):

    recent_start = (
        cutoff_timestamp
        - pd.Timedelta(days=30)
    )

    recent = bookings[
        bookings["createdAt"]
        .between(
            recent_start,
            cutoff_timestamp,
        )
    ]

    if recent.empty:
        return 1.0

    recent_rate = (
        recent["cancelledAt"]
        .notna()
        .mean()
    )

    all_rate = (
        bookings["cancelledAt"]
        .notna()
        .mean()
    )

    if all_rate <= 0:
        return 1.0

    factor = (
        all_rate
        /
        max(
            recent_rate,
            0.01,
        )
    )

    return float(
        np.clip(
            factor,
            0.85,
            1.15,
        )
    )

# =============================================================================
# SAME WEEKDAY
# =============================================================================

def same_weekday_forecast(
    daily_actuals,
    target_date,
    cutoff_date,
    target_col,
    history_length,
):

    weekday = target_date.weekday()

    history = (
        daily_actuals[
            daily_actuals["date"]
            .lt(cutoff_date)
            &
            daily_actuals["weekday_num"]
            .eq(weekday)
        ]
        .sort_values("date")
        .tail(history_length)
    )

    if history.empty:
        return np.nan

    return float(
        history[target_col]
        .mean()
    )

# =============================================================================
# WEEKDAY MONTH
# =============================================================================

def weekday_month_forecast(
    daily_actuals,
    target_date,
    cutoff_date,
    target_col,
):

    history = (
        daily_actuals[
            daily_actuals["date"]
            .lt(cutoff_date)
            &
            daily_actuals["month"]
            .eq(target_date.month)
            &
            daily_actuals["weekday_num"]
            .eq(target_date.weekday())
        ]
    )

    if history.empty:
        return np.nan

    return float(
        history[target_col]
        .mean()
    )

# =============================================================================
# TREND
# =============================================================================

def trend_factor(
    daily_actuals,
    cutoff_date,
    target_col,
    config,
):

    recent = daily_actuals[
        daily_actuals["date"]
        .between(
            cutoff_date
            -
            pd.Timedelta(
                days=config[
                    "trend_recent_days"
                ]
            ),

            cutoff_date,

            inclusive="left",
        )
    ][target_col]

    previous = daily_actuals[
        daily_actuals["date"]
        .between(
            cutoff_date
            -
            pd.Timedelta(
                days=
                config["trend_recent_days"]
                +
                config["trend_comparison_days"]
            ),

            cutoff_date
            -
            pd.Timedelta(
                days=config[
                    "trend_recent_days"
                ]
            ),

            inclusive="left",
        )
    ][target_col]

    if (
        recent.empty
        or
        previous.empty
    ):
        return 1.0

    base = previous.mean()

    if base <= 0:
        return 1.0

    return float(
        np.clip(
            recent.mean() / base,
            config["trend_factor_min"],
            config["trend_factor_max"],
        )
    )

# =============================================================================
# DURATION MODEL
# =============================================================================

def create_duration_distribution(
    bookings,
    config,
):

    df = bookings.copy()

    df["duration_days"] = (
        (
            df["exitDate"]
            -
            df["entryDate"]
        )
        .dt.days
    )

    df = df[
        df["duration_days"]
        .between(
            0,
            config[
                "maximum_duration_days"
            ]
        )
    ]

    output = (
        df.groupby(
            "duration_days"
        )
        .size()
        .rename("count")
        .reset_index()
    )

    output["probability"] = (
        output["count"]
        /
        output["count"].sum()
    )

    return output

def duration_exit_forecast(
    bookings,
    duration_distribution,
    target_date,
    cutoff_timestamp,
):

    active = get_bookings_active_as_of(
        bookings,
        cutoff_timestamp,
    )

    active = active[
        active["entryDate"]
        .notna()
    ]

    forecast = 0.0

    for _, booking in active.iterrows():

        entry_day = (
            booking["entryDate"]
            .normalize()
        )

        for _, row in (
            duration_distribution
            .iterrows()
        ):

            exit_day = (
                entry_day
                +
                pd.Timedelta(
                    days=int(
                        row[
                            "duration_days"
                        ]
                    )
                )
            )

            if exit_day == target_date:

                forecast += (
                    row[
                        "probability"
                    ]
                )

    return float(forecast)

# =============================================================================
# SHRINKAGE
# =============================================================================

def apply_shrinkage(
    booking_signal,
    historical_signal,
    strength,
):

    if pd.isna(historical_signal):
        return booking_signal

    reliability = (
        booking_signal
        /
        (
            booking_signal
            + strength
        )
    )

    return (
        reliability
        * booking_signal
        +
        (
            1
            -
            reliability
        )
        * historical_signal
    )

# =============================================================================
# ENSEMBLE
# =============================================================================

def weighted_ensemble(
    components,
    weights,
):

    values = []
    component_weights = []

    for name, value in (
        components.items()
    ):

        if pd.isna(value):
            continue

        values.append(value)

        component_weights.append(
            weights.get(
                name,
                0,
            )
        )

    if not values:
        return np.nan

    return float(
        np.average(
            values,
            weights=
            component_weights,
        )
    )

# =============================================================================
# DAILY FORECAST
# =============================================================================

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

    pace = booking_pace_factor(
        bookings,
        target_date,
        cutoff_timestamp,
    )

    cancel = cancellation_factor(
        bookings,
        cutoff_timestamp,
    )

    entry_booking = (
        entry_visibility(
            bookings,
            target_date,
            cutoff_timestamp,
        )
        *
        pace
        *
        cancel
    )

    entry_weekday = (
        same_weekday_forecast(
            daily_actuals,
            target_date,
            cutoff_timestamp.normalize(),
            "entries",
            int(
                entry_params[
                    "same_weekday_history"
                ]
            ),
        )
    )

    entry_month = (
        weekday_month_forecast(
            daily_actuals,
            target_date,
            cutoff_timestamp.normalize(),
            "entries",
        )
    )

    entry_trend = (
        entry_month
        *
        trend_factor(
            daily_actuals,
            cutoff_timestamp.normalize(),
            "entries",
            config,
        )
    )

    entry_booking_shrunk = (
        apply_shrinkage(
            entry_booking,
            entry_trend,
            entry_params[
                "booking_shrinkage_strength"
            ],
        )
    )

    exit_visibility_signal = (
        exit_visibility(
            bookings,
            target_date,
            cutoff_timestamp,
        )
    )

    expected_return_signal = (
        expected_return_daily_component(
            operations,
            target_date,
        )
    )

    if horizon_days <= 2:

        exit_booking = (
            0.50
            *
            exit_visibility_signal
            +
            0.50
            *
            expected_return_signal
        )

    elif horizon_days <= 7:

        exit_booking = (
            0.75
            *
            exit_visibility_signal
            +
            0.25
            *
            expected_return_signal
        )

    else:

        exit_booking = (
            exit_visibility_signal
        )

    exit_duration = (
        duration_exit_forecast(
            bookings,
            duration_distribution,
            target_date,
            cutoff_timestamp,
        )
    )

    exit_weekday = (
        same_weekday_forecast(
            daily_actuals,
            target_date,
            cutoff_timestamp.normalize(),
            "exits",
            int(
                exit_params[
                    "same_weekday_history"
                ]
            ),
        )
    )

    exit_trend = (
        weekday_month_forecast(
            daily_actuals,
            target_date,
            cutoff_timestamp.normalize(),
            "exits",
        )
        *
        trend_factor(
            daily_actuals,
            cutoff_timestamp.normalize(),
            "exits",
            config,
        )
    )

    exit_booking_shrunk = (
        apply_shrinkage(
            exit_booking,
            exit_trend,
            exit_params[
                "booking_shrinkage_strength"
            ],
        )
    )

    entry_forecast = weighted_ensemble(

        {
            "entry_booking_shrunk":
            entry_booking_shrunk,

            "entry_weekday":
            entry_weekday,

            "entry_month":
            entry_month,

            "entry_trend_month":
            entry_trend,
        },

        entry_weights,
    )

    exit_forecast = weighted_ensemble(

        {
            "exit_booking_shrunk":
            exit_booking_shrunk,

            "exit_duration":
            exit_duration,

            "exit_weekday":
            exit_weekday,

            "exit_trend_month":
            exit_trend,
        },

        exit_weights,
    )

    return {

        "target_date":
            target_date,

        "horizon_days":
            horizon_days,

        "entry_forecast":
            round(entry_forecast),

        "exit_forecast":
            round(exit_forecast),

        "booking_pace_factor":
            pace,

        "cancellation_factor":
            cancel,

        "entry_booking_shrunk":
            entry_booking_shrunk,

        "entry_weekday":
            entry_weekday,

        "entry_month":
            entry_month,

        "entry_trend_month":
            entry_trend,

        "exit_booking_shrunk":
            exit_booking_shrunk,

        "exit_duration":
            exit_duration,

        "exit_weekday":
            exit_weekday,

        "exit_trend_month":
            exit_trend,

    }

# =============================================================================
# HORIZON SPECIFIC HOURLY FORECAST
# =============================================================================

def create_hourly_actuals(
    operations,
):

    entries = (
        operations
        .dropna(
            subset=["CheckInEnded"]
        )
        .assign(
            datetime=lambda x:
            x["CheckInEnded"]
            .dt.floor("h")
        )
        .groupby("datetime")
        .agg(
            entries=(
                "BookingReference",
                "nunique",
            )
        )
        .reset_index()
    )

    exits = (
        operations
        .dropna(
            subset=["ActualCheckedOutDate"]
        )
        .assign(
            datetime=lambda x:
            x["ActualCheckedOutDate"]
            .dt.floor("h")
        )
        .groupby("datetime")
        .agg(
            exits=(
                "BookingReference",
                "nunique",
            )
        )
        .reset_index()
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

    hourly["date"] = (
        hourly["datetime"]
        .dt.normalize()
    )

    hourly["hour"] = (
        hourly["datetime"]
        .dt.hour
    )

    hourly["weekday_num"] = (
        hourly["datetime"]
        .dt.weekday
    )

    return hourly

def build_hourly_profile(
    hourly_actuals,
    target_date,
    flow,
    history_weeks,
):

    demand_col = (
        "entries"
        if flow == "entry"
        else "exits"
    )

    weekday = target_date.weekday()

    history = (
        hourly_actuals[
            hourly_actuals["weekday_num"]
            .eq(weekday)
            &
            hourly_actuals["date"]
            .lt(target_date)
        ]
    )

    dates = (
        history["date"]
        .drop_duplicates()
        .sort_values()
        .tail(history_weeks)
    )

    history = (
        history[
            history["date"]
            .isin(dates)
        ]
    )

    hourly = (
        history
        .groupby("hour")
        [demand_col]
        .sum()
        .reindex(
            range(24),
            fill_value=0,
        )
    )

    if hourly.sum() <= 0:
        return None

    return (
        hourly
        / hourly.sum()
    ).to_dict()

def booked_entry_hour_profile(
    bookings,
    target_date,
    cutoff_timestamp,
):

    active = (
        get_bookings_active_as_of(
            bookings,
            cutoff_timestamp,
        )
    )

    active = active[
        active["entryDate"]
        .dt.normalize()
        .eq(target_date)
    ]

    if active.empty:
        return None

    hourly = (
        active
        .groupby(
            active["entryDate"]
            .dt.hour
        )["bookingId"]
        .nunique()
        .reindex(
            range(24),
            fill_value=0,
        )
    )

    if hourly.sum() <= 0:
        return None

    return (
        hourly
        / hourly.sum()
    ).to_dict()

def expected_return_profile(
    operations,
    target_date,
):

    returns = operations[
        operations["ExpectedReturnDate"]
        .notna()
    ]

    returns = returns[
        returns["ExpectedReturnDate"]
        .dt.normalize()
        .eq(target_date)
    ]

    if returns.empty:
        return None

    hourly = (
        returns
        .groupby(
            returns["ExpectedReturnDate"]
            .dt.hour
        )["BookingReference"]
        .nunique()
        .reindex(
            range(24),
            fill_value=0,
        )
    )

    if hourly.sum() <= 0:
        return None

    return (
        hourly
        / hourly.sum()
    ).to_dict()

def distribute_daily_forecast(
    total,
    profile,
):

    rows = []

    for hour in range(24):

        rows.append(

            {
                "hour":
                    hour,

                "forecast":
                    total
                    *
                    profile.get(
                        hour,
                        0,
                    ),
            }
        )

    return pd.DataFrame(rows)

def build_hourly_forecast(
    daily_forecasts,
    bookings,
    operations,
    hourly_actuals,
    parameters_df,
):

    rows = []

    for _, forecast_row in (
        daily_forecasts.iterrows()
    ):

        target_date = (
            forecast_row[
                "target_date"
            ]
        )

        horizon_days = int(
            forecast_row[
                "horizon_days"
            ]
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

        cutoff = (
            target_date
            -
            pd.Timedelta(
                days=horizon_days
            )
            +
            pd.Timedelta(
                hours=7
            )
        )

        # ----------------------------------------------------
        # Entry Profiles
        # ----------------------------------------------------

        if horizon_days <= 2:

            entry_profile = (
                booked_entry_hour_profile(
                    bookings,
                    target_date,
                    cutoff,
                )
            )

        else:

            entry_profile = (
                build_hourly_profile(
                    hourly_actuals,
                    target_date,
                    "entry",
                    int(
                        entry_params[
                            "hourly_profile_window"
                        ]
                    ),
                )
            )

        # ----------------------------------------------------
        # Exit Profiles
        # ----------------------------------------------------

        if horizon_days <= 2:

            exit_profile = (
                expected_return_profile(
                    operations,
                    target_date,
                )
            )

        else:

            exit_profile = (
                build_hourly_profile(
                    hourly_actuals,
                    target_date,
                    "exit",
                    int(
                        exit_params[
                            "hourly_profile_window"
                        ]
                    ),
                )
            )

        if entry_profile is None:
            continue

        if exit_profile is None:
            continue

        for hour in range(24):

            rows.append(

                {
                    "datetime":
                        target_date
                        +
                        pd.Timedelta(
                            hours=hour
                        ),

                    "target_date":
                        target_date,

                    "horizon_days":
                        horizon_days,

                    "entry_forecast":
                        forecast_row[
                            "entry_forecast"
                        ]
                        *
                        entry_profile[
                            hour
                        ],

                    "exit_forecast":
                        forecast_row[
                            "exit_forecast"
                        ]
                        *
                        exit_profile[
                            hour
                        ],

                    "entry_profile_pct":
                        entry_profile[
                            hour
                        ]
                        * 100,

                    "exit_profile_pct":
                        exit_profile[
                            hour
                        ]
                        * 100,
                }
            )

    return pd.DataFrame(rows)

# =============================================================================
# EXPORT MODES
# =============================================================================

def export_legacy_outputs(
    daily_df,
    hourly_df,
    config,
):
    today_str = (
        pd.Timestamp.today()
        .strftime("%Y-%m-%d")
    )

    daily_df.to_csv(

        config["output_folder"]
        /
        f"{today_str}_daily_forecast.csv",

        index=False,
    )

    output = (
        hourly_df.copy()
    )

    output["Date"] = (
        output["datetime"]
        .dt.date
    )

    output[
        "intervalStartTime_Local"
    ] = (
        output["datetime"]
        .dt.time
    )

    output = output.rename(
        columns={
            "entry_forecast":
                "Entries",

            "exit_forecast":
                "Exits",
        }
    )

    output = output[
        [
            "Date",
            "intervalStartTime_Local",
            "Entries",
            "Exits",
        ]
    ]

    output.to_csv(
        config["output_folder"]
        /
        "FastPark_Hourly_Forecast_Output.csv",
        index=False,
    )

    dwh_output = (
        hourly_df.copy()
    )

    dwh_output["Date"] = (
        dwh_output["datetime"]
        .dt.date
    )

    dwh_output[
        "intervalStartTime_Local"
    ] = (
        hourly_df["datetime"]
        .dt.time
    )

    dwh_output = (
        dwh_output.rename(
            columns={
                "entry_forecast":
                    "Entries",

                "exit_forecast":
                    "Exits",
            }
        )
    )

    dwh_output.to_csv(

        config["output_folder"]
        /
        f"FP_Forecast_output_{datetime.now().strftime('%Y%m%d')}.csv",

        index=False,
    )

    workforce_output = (
        dwh_output.copy()
    )

    workforce_output.to_csv(

        config["output_folder"]
        /
        "FastPark Hourly Forecast Output.csv",

        index=False,
    )

def export_modern_outputs(
        daily_df,
        hourly_df,
        config,
    ):

        daily_df.to_csv(

            config[
                "output_folder"
            ]
            /
            "Forecast_Daily.csv",

            index=False,
        )

        hourly_df.to_csv(

            config[
                "output_folder"
            ]
            /
            "Forecast_Hourly.csv",

            index=False,
        )

def export_audit_output(
    daily_df,
    config,
):

    audit_cols = [

        "target_date",
        "horizon_days",

        "booking_pace_factor",
        "cancellation_factor",

        "entry_booking_shrunk",
        "entry_weekday",
        "entry_month",
        "entry_trend_month",

        "exit_booking_shrunk",
        "exit_duration",
        "exit_weekday",
        "exit_trend_month",

        "entry_forecast",
        "exit_forecast",
    ]

    daily_df[
        audit_cols
    ].to_csv(

        config["output_folder"]
        /
        "Forecast_Audit.csv",

        index=False,
    )

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    config = get_forecast_config()

    engine = get_engine()

    bookings = get_bookings(
        engine,
        config,
    )

    operations = get_operations(
        engine,
    )

    daily_actuals = (
        create_daily_actuals(
            operations
        )
    )

    duration_distribution = (
        create_duration_distribution(
            bookings,
            config,
        )
    )

    weights_df = (
        load_forecast_weights(
            config[
                "weights_file"
            ]
        )
    )

    parameters_df = (
        load_forecast_parameters(
            config[
                "parameters_file"
            ]
        )
    )

    rows = []

    today = (
        pd.Timestamp
        .today()
        .normalize()
    )

    for horizon in range(
        config["forecast_days"]
        + 1
    ):

        target_date = (
            today
            +
            pd.Timedelta(
                days=horizon
            )
        )

        rows.append(

            create_forecast_for_date(
                target_date,
                horizon,
                bookings,
                daily_actuals,
                duration_distribution,
                weights_df,
                parameters_df,
                config,
            )
        )

    output = pd.DataFrame(rows)

    hourly_actuals = (
        create_hourly_actuals(
            operations
        )
    )

    hourly_forecasts = (
        build_hourly_forecast(
            output,
            bookings,
            operations,
            hourly_actuals,
            parameters_df,
        )
    )

    export_audit_output(
        output,
        config,
    )

    if (
        config["export_mode"]
        == "legacy"
    ):

        export_legacy_outputs(
            hourly_forecasts,
            config,
        )

    else:

        export_modern_outputs(
            output,
            hourly_forecasts,
            config,
        )

        config[
            "output_folder"
        ].mkdir(
            parents=True,
            exist_ok=True,
        )

    output.to_csv(

        config[
            "output_folder"
        ]
        /
        "FastPark_Daily_Forecast.csv",

        index=False,
    )

    print()
    print(
        "Forecast complete."
    )