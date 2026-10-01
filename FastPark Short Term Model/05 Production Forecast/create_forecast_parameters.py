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
    / "refined_selected_weights_backup.csv"
)

HOURLY_SIMULATION_FILE = (
    BASE_DIR
    / "fastpark_hourly_forecast_simulation.xlsx"
)

NON_PRICE_SIMULATION_FILE = (
    BASE_DIR
    / "fastpark_forecast_simulation_non_price.xlsx"
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

    parameters["booking_shrinkage_strength"] = (
        parameters["booking_shrinkage_strength"]
        .fillna(0)
        .round()
        .astype(int)
    )

    parameters["hourly_profile_window"] = (
        parameters["hourly_profile_window"]
        .astype(int)
    )

    parameters["same_weekday_history"] = (
        parameters["same_weekday_history"]
        .astype(int)
    )

    # ------------------------------------------------------------------------
    # Sheet structure validation
    #
    # Historical Window Tests was generated by
    # fastpark_forecast_simulation_non_price.py.
    #
    # Expected columns:
    #
    #     flow
    #     horizon_days
    #     window
    #     validation_wape_pct
    #
    # If the simulation export changes in future, fail loudly rather than
    # silently producing incorrect production parameters.
    # ------------------------------------------------------------------------

    required_columns = [
        "flow",
        "horizon_days",
        "booking_shrinkage_strength",
        "hourly_profile_window",
        "same_weekday_history",
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