"""
FASTPARK FORECAST PARAMETER CREATION
====================================

Purpose
-------
Create the production forecasting parameter table from the historical
simulation outputs.

Unlike forecast_weights.csv, this file contains operational forecasting
settings rather than component weights.

Examples
--------
    - Same-weekday history window
    - Hourly profile lookback window
    - Booking shrinkage strength

These parameters are derived from the simulation outputs and can be
re-generated whenever the forecasting framework is recalibrated.

Inputs
------

refined_selected_weights_backup.csv

    Used to determine the preferred booking-shrinkage strength.

fastpark_hourly_forecast_simulation.xlsx

    Used to determine the preferred hourly profile window.

Outputs
-------

forecast_parameters.csv

Used by:

    fastpark_short_term_forecast_2026.py

Notes
-----

This file should ONLY contain parameters that are genuinely calibrated.

Structural model settings such as:

    trend_recent_days
    trend_comparison_days
    maximum_duration_days

remain inside the forecast script configuration because they were not
optimised during the simulation process.
"""

from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================================
# FILE LOCATIONS
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent

SELECTED_WEIGHTS_FILE = (
    BASE_DIR
    / "inputs" / "refined_selected_weights_backup_all_dates.csv"
)

HOURLY_SIMULATION_FILE = (
    BASE_DIR
    / "inputs" / "fastpark_hourly_forecast_simulation.xlsx"
)

NON_PRICE_SIMULATION_FILE = (
    BASE_DIR
    / "inputs" / "fastpark_forecast_simulation_non_price.xlsx"
)

OUTPUT_FILE = (
    BASE_DIR
    / "forecast_parameters.csv"
)


# ============================================================================
# HELPERS
# ============================================================================

def weighted_average(series, weights):
    """
    Calculate weighted average while ignoring missing values.
    """

    valid = (
        series.notna()
        &
        weights.notna()
    )

    if not valid.any():
        return np.nan

    return np.average(
        series.loc[valid],
        weights=weights.loc[valid],
    )

def build_adjustment_parameters():
    """
    Extracts pace and cancellation adjustment factors from the Non-Price simulation.
    """
    # Assuming 'Selected Weights' or the main results export contains the adjustment factors
    df = pd.read_excel(NON_PRICE_SIMULATION_FILE, sheet_name="Selected Weights")
    
    # We filter to the latest fold to get the most recent learned adjustments
    latest_month = df["test_month"].max()
    df_latest = df[df["test_month"] == latest_month].copy()
    
    # Map the columns to what the production script expects
    # You need the 'adjustment' for Pace and Cancellation
    # This assumes your simulation export has columns like 'pace_adjustment' and 'cancellation_adjustment'
    cols = ["flow", "horizon_days", "pace_adjustment", "cancellation_adjustment"]
    
    return df_latest[cols].drop_duplicates()

# ============================================================================
# SHRINKAGE PARAMETERS
# ============================================================================

def build_shrinkage_parameters():
    """
    Derive the preferred booking-shrinkage strength.

    Source:
        refined_selected_weights_backup.csv

    Logic:
        Better validation folds contribute more heavily.
    """

    df = pd.read_csv(
        SELECTED_WEIGHTS_FILE
    )

    required = {
        "demand_type",
        "horizon_days",
        "shrinkage_strength",
        "validation_wape_pct",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing columns in selected weights:\n"
            f"{sorted(missing)}"
        )

    df["quality_weight"] = (
        1
        /
        df["validation_wape_pct"]
        .replace(0, np.nan)
    )

    rows = []

    for (
        demand_type,
        horizon_days,
    ), group in df.groupby(
        [
            "demand_type",
            "horizon_days",
        ]
    ):

        shrinkage = weighted_average(
            group["shrinkage_strength"],
            group["quality_weight"],
        )

        rows.append(
            {
                "flow": demand_type,
                "horizon_days": int(horizon_days),
                "booking_shrinkage_strength": round(shrinkage),
            }
        )

    return pd.DataFrame(rows)


# ============================================================================
# HOURLY PROFILE PARAMETERS
# ============================================================================

def build_hourly_profile_parameters():
    """
    Extract preferred hourly profile windows.

    Source:
        fastpark_hourly_forecast_simulation.xlsx

    Sheet:
        Best Profile Windows
    """

    df = pd.read_excel(
        HOURLY_SIMULATION_FILE,
        sheet_name="Best Profile Windows",
    )

    required = {
        "flow_type",
        "horizon_days",
        "profile_weeks",
    }

    missing = required - set(df.columns)

    if missing:

        raise ValueError(
            f"Missing columns in hourly workbook:\n"
            f"{sorted(missing)}"
        )

    output = df[
        [
            "flow_type",
            "horizon_days",
            "profile_weeks",
        ]
    ].copy()

    output = output.rename(
        columns={
            "flow_type":
                "flow",
            "profile_weeks":
                "hourly_profile_window",
        }
    )

    output["flow"] = (
        output["flow"]
        .astype(str)
        .str.lower()
    )

    return output


# ============================================================================
# SAME-WEEKDAY HISTORY PARAMETERS
# ============================================================================

def build_same_weekday_parameters():
    """
    Derive the preferred same-weekday lookback window directly from
    the Generation 2.1 simulation outputs.

    Source
    ------
    fastpark_forecast_simulation_non_price.xlsx

    Sheet
    -----
    Historical Window Tests

    Purpose
    -------
    Generation 2.1 tested multiple same-weekday history windows:

        1
        2
        4
        6
        8

    for every:

        flow
        forecast horizon

    The lowest validation WAPE is selected as the production setting.

    This removes the need for hard-coded assumptions and ensures the
    forecasting engine uses the historically best-performing weekday
    lookback period.

    Output
    ------
    flow
    horizon_days
    same_weekday_history
    """

    df = pd.read_excel(
        NON_PRICE_SIMULATION_FILE,
        sheet_name="Historical Window Tests",
    )

    required_columns = {
        "flow",
        "horizon_days",
        "window",
        "validation_wape_pct",
    }

    missing_columns = (
        required_columns
        - set(df.columns)
    )

    if missing_columns:

        raise ValueError(
            "Missing required columns in "
            "'Historical Window Tests':\n"
            f"{sorted(missing_columns)}"
        )

    # ------------------------------------------------------------------------
    # Select the best same-weekday history window.
    #
    # Lowest validation WAPE wins.
    #
    # The simulation already tested:
    #
    #     1
    #     2
    #     4
    #     6
    #     8
    #
    # weekday history windows.
    #
    # We simply use the historical winner.
    # ------------------------------------------------------------------------

    best_windows = (
        df
        .sort_values(
            [
                "flow",
                "horizon_days",
                "validation_wape_pct",
                "window",
            ]
        )
        .groupby(
            [
                "flow",
                "horizon_days",
            ],
            as_index=False,
        )
        .first()
    )

    output = best_windows[
        [
            "flow",
            "horizon_days",
            "window",
        ]
    ].copy()

    output = output.rename(
        columns={
            "window":
                "same_weekday_history",
        }
    )

    output["same_weekday_history"] = (
        output["same_weekday_history"]
        .astype(int)
    )

    return output


# ============================================================================
# WORKBOOK-BASED HORIZON SELECTION
# ============================================================================

def build_horizon_component_map():
    """
    Return the exact workbook winner for each flow and horizon.

    This is the production translation of the G2.1 experiment performance table.
    We do not use rough bands like 'early' or 'late'; the value is set per exact
    horizon day 0..56 so the production forecast mirrors the tested logic.
    """

    component_map = {
        "entry": {
            0: "ADD_CALENDAR",
            1: "ALL_NON_PRICE",
            2: "ALL_NON_PRICE",
            3: "ADD_BOOKING_PACE",
            4: "ADD_BOOKING_PACE",
            5: "ALL_NON_PRICE",
            6: "ALL_NON_PRICE",
            7: "ADD_BOOKING_REGIME",
            8: "ALL_NON_PRICE",
            9: "ADD_BOOKING_REGIME",
            10: "ALL_NON_PRICE",
            11: "ALL_NON_PRICE",
            12: "ALL_NON_PRICE",
            13: "ADD_BOOKING_PACE",
            14: "ADD_BOOKING_PACE",
            15: "ADD_BOOKING_REGIME",
            16: "ADD_CANCELLATION",
            17: "ALL_NON_PRICE",
            18: "ALL_NON_PRICE",
            19: "ADD_BOOKING_PACE",
            20: "ADD_CANCELLATION",
            21: "ADD_WEEKDAY_TREND",
            22: "ADD_WEEKDAY_TREND",
            23: "ADD_BOOKING_PACE",
            24: "ADD_CANCELLATION",
            25: "ADD_WEEKDAY_TREND",
            26: "ADD_BOOKING_PACE",
            27: "ADD_BOOKING_PACE",
            28: "ADD_WEEKDAY_TREND",
            29: "ADD_BOOKING_PACE",
            30: "ADD_BOOKING_PACE",
            31: "ADD_BOOKING_PACE",
            32: "ADD_BOOKING_PACE",
            33: "ADD_BOOKING_PACE",
            34: "ADD_CANCELLATION",
            35: "ALL_NON_PRICE",
            36: "ADD_CANCELLATION",
            37: "ADD_CANCELLATION",
            38: "ADD_CANCELLATION",
            39: "ADD_CANCELLATION",
            40: "ADD_WEEKDAY_TREND",
            41: "ADD_BOOKING_PACE",
            42: "ADD_BOOKING_PACE",
            43: "ADD_BOOKING_PACE",
            44: "ADD_BOOKING_PACE",
            45: "ADD_BOOKING_PACE",
            46: "ADD_BOOKING_PACE",
            47: "ADD_CANCELLATION",
            48: "ADD_CANCELLATION",
            49: "ADD_WEEKDAY_TREND",
            50: "ADD_BOOKING_PACE",
            51: "ADD_BOOKING_PACE",
            52: "ADD_BOOKING_PACE",
            53: "ADD_BOOKING_PACE",
            54: "ADD_BOOKING_PACE",
            55: "ADD_BOOKING_PACE",
            56: "ADD_BOOKING_PACE",
        },
        "exit": {
            0: "ALL_NON_PRICE",
            1: "ALL_NON_PRICE",
            2: "ADD_BOOKING_PACE",
            3: "BASELINE",
            4: "ALL_NON_PRICE",
            5: "ALL_NON_PRICE",
            6: "ALL_NON_PRICE",
            7: "ADD_WEEKDAY_TREND",
            8: "ADD_WEEKDAY_TREND",
            9: "ADD_WEEKDAY_TREND",
            10: "ADD_CANCELLATION",
            11: "ADD_CANCELLATION",
            12: "ADD_WEEKDAY_TREND",
            13: "ADD_BOOKING_REGIME",
            14: "ALL_NON_PRICE",
            15: "ADD_BOOKING_REGIME",
            16: "ADD_BOOKING_REGIME",
            17: "ADD_BOOKING_REGIME",
            18: "ADD_BOOKING_REGIME",
            19: "ADD_BOOKING_REGIME",
            20: "ALL_NON_PRICE",
            21: "ADD_BOOKING_REGIME",
            22: "ADD_BOOKING_REGIME",
            23: "ADD_BOOKING_REGIME",
            24: "ADD_BOOKING_REGIME",
            25: "ALL_NON_PRICE",
            26: "ALL_NON_PRICE",
            27: "ADD_CALENDAR",
            28: "ALL_NON_PRICE",
            29: "ALL_NON_PRICE",
            30: "ALL_NON_PRICE",
            31: "ALL_NON_PRICE",
            32: "ALL_NON_PRICE",
            33: "ALL_NON_PRICE",
            34: "ALL_NON_PRICE",
            35: "ALL_NON_PRICE",
            36: "ALL_NON_PRICE",
            37: "ALL_NON_PRICE",
            38: "ADD_CALENDAR",
            39: "ALL_NON_PRICE",
            40: "ALL_NON_PRICE",
            41: "ALL_NON_PRICE",
            42: "ALL_NON_PRICE",
            43: "ALL_NON_PRICE",
            44: "ALL_NON_PRICE",
            45: "ALL_NON_PRICE",
            46: "ALL_NON_PRICE",
            47: "ALL_NON_PRICE",
            48: "ALL_NON_PRICE",
            49: "ADD_CALENDAR",
            50: "ALL_NON_PRICE",
            51: "ALL_NON_PRICE",
            52: "ALL_NON_PRICE",
            53: "ADD_CALENDAR",
            54: "ADD_CALENDAR",
            55: "ADD_CALENDAR",
            56: "ADD_CALENDAR",
        },
    }

    rows = []
    for flow, horizon_map in component_map.items():
        for horizon_days, experiment_name in horizon_map.items():
            rows.append(
                {
                    "flow": flow,
                    "horizon_days": int(horizon_days),
                    "winner_experiment": experiment_name,
                }
            )

    return pd.DataFrame(rows)


# ============================================================================
# BUILD FORECAST PARAMETERS
# ============================================================================

def build_forecast_parameters():
    """
    Create production parameter table.
    """

    shrinkage = (
        build_shrinkage_parameters()
    )

    hourly_profiles = (
        build_hourly_profile_parameters()
    )

    weekday_history = (
        build_same_weekday_parameters()
    )

    adjustments = (
        build_adjustment_parameters()
    )

    workbook_horizon_map = build_horizon_component_map()

    parameters = (
        shrinkage
        .merge(
            hourly_profiles,
            on=[
                "flow",
                "horizon_days",
            ],
            how="outer",
        )
        .merge(
            weekday_history,
            on=[
                "flow",
                "horizon_days",
            ],
            how="outer",
        )
        .merge(
            adjustments,
            on=[
                "flow",
                "horizon_days",
            ],
            how="outer",
        )
        .merge(
            workbook_horizon_map,
            on=[
                "flow",
                "horizon_days",
            ],
            how="left",
        )
    )

    parameters = (
        parameters
        .sort_values(
            [
                "flow",
                "horizon_days",
            ]
        )
        .reset_index(drop=True)
    )

    parameters["booking_shrinkage_strength"] = parameters["booking_shrinkage_strength"].fillna(0).round().astype(int)
    parameters["hourly_profile_window"] = parameters["hourly_profile_window"].fillna(6).astype(int)
    parameters["same_weekday_history"] = parameters["same_weekday_history"].fillna(4).astype(int)
    parameters["pace_adjustment"] = parameters["pace_adjustment"].fillna(1.0) # 1.0 = no adjustment
    parameters["cancellation_adjustment"] = parameters["cancellation_adjustment"].fillna(1.0) # 1.0 = no adjustment
    parameters["winner_experiment"] = parameters["winner_experiment"].fillna("ALL_NON_PRICE")

    # ------------------------------------------------------------------------
    # Sheet structure validation
    #
    # Historical Window Tests was generated by
    # fastpark_forecast_simulation_non_price.py.
    #
    # If the simulation export changes in future, fail loudly rather than
    # silently producing incorrect production parameters.
    # ------------------------------------------------------------------------
    required_columns = [
        "flow", "horizon_days", "booking_shrinkage_strength",
        "hourly_profile_window", "same_weekday_history",
        "pace_adjustment", "cancellation_adjustment", "winner_experiment"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in parameters.columns
    ]

    if missing_columns:

        raise ValueError(
            "Missing parameter columns:\n"
            f"{missing_columns}"
        )

    parameters.to_csv(
        OUTPUT_FILE,
        index=False,
    )

    print()
    print("=" * 80)
    print("FORECAST PARAMETERS CREATED")
    print("=" * 80)

    print()
    print(
        f"Output:\n{OUTPUT_FILE}"
    )

    print()
    print(
        parameters.head(30)
        .to_string(index=False)
    )

    return parameters


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":

    build_forecast_parameters()
