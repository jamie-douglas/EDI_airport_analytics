# =============================================================================
# FASTPARK LIVE FORECAST COMPARISON
# OLD vs NEW
# =============================================================================

import pandas as pd
import numpy as np
from pathlib import Path

from simplified_fastpark_shortterm import (
    PRODUCTION_CONFIG,
    get_engine,
    load_and_clean_production_data,
)

# =============================================================================
# CONFIG
# =============================================================================

START_RUN_DATE = pd.Timestamp("2026-10-20")

MAX_HORIZON = 56

NEW_FOLDER = Path(
    r"C:\Users\jamie_douglas\Edinburgh Airport Limited\Business Planning - Documents\02. Capacity Areas\09. Car Parking\01. FastPark\FastPark Model Rebuild 2025\FastPark Short-Term Model Rebuild\Daily Forecast Output"
)

OLD_FOLDER = Path(
    r"C:\Users\jamie_douglas\Edinburgh Airport Limited\Business Planning - Documents\02. Capacity Areas\09. Car Parking\01. FastPark\FastPark Model Rebuild 2025\FastPark Short-Term Model Rebuild\Prev\Daily Forecast Output"
)

OUTPUT_FILE = Path(
    "outputs/comparison/FastPark_Live_Model_Comparison.xlsx"
)

OUTPUT_FILE.parent.mkdir(
    parents=True,
    exist_ok=True
)

# =============================================================================
# METRICS
# =============================================================================


def mape(actual, forecast):

    if pd.isna(actual):
        return np.nan

    if actual == 0:
        return np.nan

    return abs(actual - forecast) / actual


# =============================================================================
# LOAD ACTUALS
# =============================================================================

print("Loading actuals...")

engine = get_engine(
    dsn=PRODUCTION_CONFIG["db_dsn"],
    username=PRODUCTION_CONFIG["db_username"],
)

actual_config = PRODUCTION_CONFIG.copy()
actual_config["current_run_date"] = pd.Timestamp.today().normalize()

actual_data = load_and_clean_production_data(
    actual_config,
    engine,
)

daily_actuals = (
    actual_data["daily_actuals_df"]
    .rename(
        columns={
            "date": "target_date",
            "entries": "actual_entries",
            "exits": "actual_exits",
        }
    )
)

daily_actuals["target_date"] = pd.to_datetime(
    daily_actuals["target_date"]
).dt.normalize()

# Remove today's partial actuals
today = pd.Timestamp.today().normalize()

daily_actuals = daily_actuals[
    daily_actuals["target_date"] < today
]

# =============================================================================
# FORECAST LOADER
# =============================================================================


def load_forecasts(folder, model_name):

    results = []

    files = sorted(
        folder.glob("*_daily_forecast.csv")
    )

    print(
        f"{model_name}: found {len(files)} files"
    )

    for forecast_file in files:

        try:

            run_date = pd.to_datetime(
                forecast_file.name[:10]
            ).normalize()

        except Exception:
            continue

        if run_date < START_RUN_DATE:
            continue

        try:

            df = pd.read_csv(
                forecast_file
            )

            df.columns = [
                str(c).strip()
                for c in df.columns
            ]

            if "Date" not in df.columns:

                df.rename(
                    columns={
                        df.columns[0]: "Date"
                    },
                    inplace=True
                )

            entry_col = None
            exit_col = None

            for c in [
                "Entries",
                "entry_forecast"
            ]:
                if c in df.columns:
                    entry_col = c

            for c in [
                "Exits",
                "exit_forecast"
            ]:
                if c in df.columns:
                    exit_col = c

            if (
                entry_col is None
                or exit_col is None
            ):
                continue

            df["target_date"] = pd.to_datetime(
                df["Date"],
                errors="coerce"
            ).dt.normalize()

            df["forecast_entries"] = pd.to_numeric(
                df[entry_col],
                errors="coerce"
            )

            df["forecast_exits"] = pd.to_numeric(
                df[exit_col],
                errors="coerce"
            )

            df["run_date"] = run_date

            df["horizon"] = (
                df["target_date"]
                - run_date
            ).dt.days

            df["model"] = model_name

            results.append(
                df[
                    [
                        "model",
                        "run_date",
                        "target_date",
                        "horizon",
                        "forecast_entries",
                        "forecast_exits"
                    ]
                ]
            )

        except Exception as e:

            print(
                f"Failed: {forecast_file.name}"
            )

            print(e)

    if len(results) == 0:
        return pd.DataFrame()

    return pd.concat(
        results,
        ignore_index=True
    )


# =============================================================================
# LOAD FORECASTS
# =============================================================================

print("Loading new model forecasts...")

new_results = load_forecasts(
    NEW_FOLDER,
    "New"
)

print("Loading old model forecasts...")

old_results = load_forecasts(
    OLD_FOLDER,
    "Old"
)

results = pd.concat(
    [
        old_results,
        new_results
    ],
    ignore_index=True
)

# =============================================================================
# MERGE ACTUALS
# =============================================================================

results = results.merge(
    daily_actuals,
    on="target_date",
    how="left"
)

results = results[
    results["horizon"].between(
        0,
        MAX_HORIZON
    )
]


results = results.dropna(
    subset=[
        "forecast_entries",
        "forecast_exits",
        "actual_entries",
        "actual_exits"
    ]
)

# =============================================================================
# TOTALS
# =============================================================================

results["forecast_total"] = (
    results["forecast_entries"]
    + results["forecast_exits"]
)

results["actual_total"] = (
    results["actual_entries"]
    + results["actual_exits"]
)

# =============================================================================
# ACCURACY
# =============================================================================

results["total_mape"] = results.apply(
    lambda x: mape(
        x["actual_total"],
        x["forecast_total"]
    ),
    axis=1
)

results["total_accuracy"] = (
    1 - results["total_mape"]
)

results["entry_mape"] = results.apply(
    lambda x: mape(
        x["actual_entries"],
        x["forecast_entries"]
    ),
    axis=1
)

results["entry_accuracy"] = (
    1 - results["entry_mape"]
)

results["exit_mape"] = results.apply(
    lambda x: mape(
        x["actual_exits"],
        x["forecast_exits"]
    ),
    axis=1
)

results["exit_accuracy"] = (
    1 - results["exit_mape"]
)

# =============================================================================
# HORIZON COMPARISON
# =============================================================================

horizon_summary = (
    results
    .groupby(
        [
            "model",
            "horizon"
        ]
    )
    [
        [
            "total_accuracy",
            "entry_accuracy",
            "exit_accuracy"
        ]
    ]
    .mean()
    .reset_index()
)

# =============================================================================
# DAILY MATRICES
# =============================================================================

old_matrix = (
    results[
        results["model"] == "Old"
    ]
    .pivot_table(
        index="run_date",
        columns="horizon",
        values="total_accuracy",
        aggfunc="mean"
    )
    .reset_index()
)

old_matrix.columns.name = None

new_matrix = (
    results[
        results["model"] == "New"
    ]
    .pivot_table(
        index="run_date",
        columns="horizon",
        values="total_accuracy",
        aggfunc="mean"
    )
    .reset_index()
)

new_matrix.columns.name = None


# =============================================================================
# HORIZON COMPARISON
# =============================================================================

comparison = (
    results
    .groupby(
        ["horizon", "model"]
    )["total_accuracy"]
    .mean()
    .unstack()
)

comparison["Improvement"] = (
    comparison["New"]
    - comparison["Old"]
)

comparison["Improvement_%"] = (
    comparison["Improvement"] * 100
)

comparison = comparison.reset_index()


# =============================================================================
# DAILY COMPARISON
# =============================================================================

daily_comparison = (
    results
    .groupby(
        [
            "run_date",
            "model"
        ]
    )["total_accuracy"]
    .mean()
    .unstack()
)

daily_comparison["Difference"] = (
    daily_comparison["New"]
    - daily_comparison["Old"]
)

daily_comparison["Winner"] = np.where(
    daily_comparison["Difference"] > 0,
    "New",
    np.where(
        daily_comparison["Difference"] < 0,
        "Old",
        "Tie"
    )
)

daily_comparison = (
    daily_comparison
    .reset_index()
)

# =============================================================================
# OVERALL SUMMARY
# =============================================================================

overall_summary = (
    results
    .groupby("model")
    [
        [
            "entry_accuracy",
            "exit_accuracy",
            "total_accuracy"
        ]
    ]
    .mean()
)

# =============================================================================
# SAVE
# =============================================================================

print("Writing workbook...")

with pd.ExcelWriter(
    OUTPUT_FILE,
    engine="openpyxl"
) as writer:

    results.to_excel(
        writer,
        sheet_name="Raw_Data",
        index=False
    )

    overall_summary.to_excel(
        writer,
        sheet_name="Overall"
    )

    horizon_summary.to_excel(
        writer,
        sheet_name="By_Horizon",
        index=False
    )

    comparison.to_excel(
        writer,
        sheet_name="Horizon_Comparison",
        index=False
    )

    old_matrix.to_excel(
        writer,
        sheet_name="Old_Matrix",
        index=False
    )

    new_matrix.to_excel(
        writer,
        sheet_name="New_Matrix",
        index=False
    )

    daily_comparison.to_excel(
        writer,
        sheet_name="Daily_Comparison",
        index=False
    )

print()
print("Done")
print(OUTPUT_FILE)