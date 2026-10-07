"""
FASTPARK FORECAST WEIGHT CREATION
================================

Purpose
-------
Create the production forecast weight table from the historical
Generation 2 optimisation results.

The production forecasting script should NEVER contain hard-coded
weights.

Instead, weights are derived from the historical rolling-origin
backtests contained in:

    refined_selected_weights_backup.csv

This ensures the operational forecast always reflects the most
successful combinations discovered during model calibration.

------
For each:

    demand_type
    horizon_days

the script:

1. Loads all selected weight solutions across rolling folds.
2. Scores each fold according to validation quality.
3. Gives greater importance to stronger folds.
4. Calculates weighted-average component weights.
5. Re-normalises weights so they sum to 100%.
6. Exports a clean production-ready weight file.

Output
------

forecast_weights.csv

This file is read by:

    fastpark_short_term_forecast_2026.py

and should be treated as the production forecast configuration.
"""

from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================================
# FILE LOCATIONS
# ============================================================================
#
# Keep all paths relative to this script so that the forecasting package can
# be moved between environments without modification.
#
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent

INPUT_FILE = (
    BASE_DIR
    / "inputs" / "refined_selected_weights_backup_all_dates.csv"
)

OUTPUT_FILE = (
    BASE_DIR
    / "forecast_weights.csv"
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def weighted_average(series, weights):
    """
    Calculate a weighted average while safely ignoring missing values.

    Parameters
    ----------
    series : pd.Series
        Values to average.

    weights : pd.Series
        Relative importance of each observation.

    Returns
    -------
    float
        Weighted average value.
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
# MAIN WEIGHT BUILD PROCESS
# ============================================================================

def build_forecast_weights():
    """
    Create the production weight table.

    The Generation 2 optimisation selected a preferred set of weights for
    every rolling fold.

    Rather than choosing a single fold, this function combines all folds
    together.

    Better-performing folds contribute more heavily than weaker folds.
    """

    # ------------------------------------------------------------------------
    # Load calibration results
    # ------------------------------------------------------------------------

    df = pd.read_csv(INPUT_FILE)

    print(
        f"Loaded {len(df):,} selected weight rows."
    )

    # ------------------------------------------------------------------------
    # Identify weight columns automatically.
    #
    # This allows new components to be added later without rewriting the
    # script.
    # ------------------------------------------------------------------------

    weight_columns = [
        column
        for column in df.columns
        if (
            column.startswith("entry_")
            or column.startswith("exit_")
        )
    ]

    if not weight_columns:

        raise RuntimeError(
            "No forecast weight columns were found.\n"
            "Check the selected-weights export."
        )

    # ------------------------------------------------------------------------
    # Create calibration quality score.
    #
    # Lower validation WAPE means better out-of-sample performance.
    #
    # Example:
    #
    # Validation WAPE = 5%
    # Score = 0.20
    #
    # Validation WAPE = 10%
    # Score = 0.10
    #
    # Therefore stronger folds have greater influence.
    # ------------------------------------------------------------------------

    df["weight_quality"] = (
        1
        /
        df["validation_wape_pct"]
        .replace(
            0,
            np.nan,
        )
    )

    rows = []

    # ------------------------------------------------------------------------
    # Create one production row for each:
    #
    #     demand_type
    #     horizon_days
    #
    # ------------------------------------------------------------------------

    for (
        demand_type,
        horizon_days,
    ), group in df.groupby(
        [
            "demand_type",
            "horizon_days",
        ]
    ):

        output = {
            "demand_type":
                demand_type,

            "horizon_days":
                int(horizon_days),
        }

        for column in weight_columns:

            output[column] = weighted_average(
                group[column],
                group["weight_quality"],
            )

        rows.append(output)

    weights = pd.DataFrame(rows)

    # ------------------------------------------------------------------------
    # Re-normalise weights.
    #
    # Floating-point averages can occasionally cause weights to sum to
    # slightly more or less than 1.
    #
    # The production script expects weights to sum exactly to 100%.
    # ------------------------------------------------------------------------

    for idx, row in weights.iterrows():

        if row["demand_type"] == "entry":

            component_columns = [
                column
                for column in weights.columns
                if column.startswith("entry_")
            ]

        else:

            component_columns = [
                column
                for column in weights.columns
                if column.startswith("exit_")
            ]

        total_weight = row[
            component_columns
        ].sum()

        if total_weight > 0:

            weights.loc[
                idx,
                component_columns,
            ] = (
                row[component_columns]
                / total_weight
            )

    # ------------------------------------------------------------------------
    # Sort output for easier auditing.
    # ------------------------------------------------------------------------

    weights = (
        weights
        .sort_values(
            [
                "demand_type",
                "horizon_days",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    # ------------------------------------------------------------------------
    # Export production weight table.
    # ------------------------------------------------------------------------

    weights.to_csv(
        OUTPUT_FILE,
        index=False,
    )

    # ------------------------------------------------------------------------
    # Console summary.
    # ------------------------------------------------------------------------

    print()
    print("=" * 80)
    print("PRODUCTION FORECAST WEIGHTS CREATED")
    print("=" * 80)

    print()
    print(
        f"Output file:\n{OUTPUT_FILE}"
    )

    print()
    print(
        weights.head(20)
        .to_string(index=False)
    )


# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================

if __name__ == "__main__":

    build_forecast_weights()