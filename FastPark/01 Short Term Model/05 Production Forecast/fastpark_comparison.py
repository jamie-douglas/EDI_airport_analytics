# =============================================================================
# FASTPARK OLD vs NEW FORECAST BENCHMARK
# =============================================================================

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# =============================================================================
# IMPORT NEW MODEL
# =============================================================================

from simplified_fastpark_shortterm import (
    PRODUCTION_CONFIG,
    run_production_forecast,
    get_engine,
    load_and_clean_production_data,
)

# =============================================================================
# CONFIG
# =============================================================================

MAX_HORIZON = 56

START_DATE = "2026-08-16"
END_DATE = pd.Timestamp.today().normalize() - pd.Timedelta(days=MAX_HORIZON+1)


FORECAST_FOLDER = Path(
    r"C:\Users\jamie_douglas\Edinburgh Airport Limited\Business Planning - Documents\02. Capacity Areas\09. Car Parking\01. FastPark\FastPark Model Rebuild 2025\FastPark Short-Term Model Rebuild\Daily Forecast Output"
)

OUTPUT_FILE = (
    Path("outputs")
    / "comparison"
    / "FastPark_Old_vs_New_Benchmark_last56.xlsx"
)

OUTPUT_FILE.parent.mkdir(
    parents=True,
    exist_ok=True
)

# =============================================================================
# HELPERS
# =============================================================================

def mape(actual, forecast):

    if pd.isna(actual):
        return np.nan

    if actual == 0:
        return np.nan

    return abs(actual - forecast) / actual


def accuracy(actual, forecast):

    value = mape(actual, forecast)

    if pd.isna(value):
        return np.nan

    return 1 - value


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

hourly_actuals = (
    actual_data["hourly_actuals_df"]
    .rename(
        columns={
            "datetime_hour": "datetime_hour",
            "entries": "actual_entries",
            "exits": "actual_exits",
        }
    )
)

daily_actuals["target_date"] = pd.to_datetime(
    daily_actuals["target_date"]
).dt.normalize()

hourly_actuals["datetime_hour"] = pd.to_datetime(
    hourly_actuals["datetime_hour"]
)

# =============================================================================
# OLD MODEL RESULTS
# =============================================================================

print("Processing archived forecasts...")

old_daily_results = []

old_hourly_results = []

for forecast_file in sorted(FORECAST_FOLDER.glob("*_daily_forecast.csv")):

    try:

        run_date = pd.to_datetime(
            forecast_file.name[:10]
        ).normalize()

        if run_date < pd.Timestamp(START_DATE):
            continue

        if run_date > END_DATE:
            continue

    except:
        continue

    forecast_df = pd.read_csv(forecast_file)

    forecast_df.columns = [
        str(c).strip()
        for c in forecast_df.columns
    ]

    if "Date" not in forecast_df.columns:
        forecast_df.rename(
            columns={
                forecast_df.columns[0]: "Date"
            },
            inplace=True,
        )

    entry_col = None
    exit_col = None

    for c in ["Entries", "entry_forecast"]:
        if c in forecast_df.columns:
            entry_col = c

    for c in ["Exits", "exit_forecast"]:
        if c in forecast_df.columns:
            exit_col = c

    if entry_col is None or exit_col is None:
        continue

    forecast_df["target_date"] = pd.to_datetime(
        forecast_df["Date"],
        errors="coerce",
    ).dt.normalize()

    forecast_df["forecast_entries"] = pd.to_numeric(
        forecast_df[entry_col],
        errors="coerce",
    )

    forecast_df["forecast_exits"] = pd.to_numeric(
        forecast_df[exit_col],
        errors="coerce",
    )

    merged = forecast_df.merge(
        daily_actuals,
        on="target_date",
        how="left",
    )

    merged["run_date"] = run_date
    merged["model"] = "Old"

    merged["horizon"] = (
        merged["target_date"] - merged["run_date"]
    ).dt.days

    old_daily_results.append(
        merged[
            [
                "model",
                "run_date",
                "target_date",
                "horizon",
                "forecast_entries",
                "forecast_exits",
                "actual_entries",
                "actual_exits",
            ]
        ]
    )

    # ============================================================================
    # OLD HOURLY FORECAST
    # ============================================================================

    hourly_file = Path(
        str(forecast_file).replace(
            "_daily_forecast.csv",
            "_daily_forecast_DWH.csv"
        )
    )

    if hourly_file.exists():

        try:

            hourly_df = pd.read_csv(hourly_file)

            hourly_df["Date"] = pd.to_datetime(
                hourly_df["Date"],
                errors="coerce"
            )

            hourly_df["datetime_hour"] = (
                hourly_df["Date"] +
                pd.to_timedelta(
                    hourly_df["intervalStartTime_Local"].astype(str)
                )
            )

            hourly_df.rename(
                columns={
                    "Entries": "forecast_entries",
                    "Exits": "forecast_exits"
                },
                inplace=True
            )

            hourly_df = hourly_df.merge(
                hourly_actuals,
                on="datetime_hour",
                how="left"
            )

            hourly_df["run_date"] = run_date
            hourly_df["model"] = "Old"

            hourly_df["target_date"] = (
                hourly_df["datetime_hour"]
                .dt.normalize()
            )

            hourly_df["horizon"] = (
                hourly_df["target_date"]
                - hourly_df["run_date"]
            ).dt.days

            old_hourly_results.append(
                hourly_df[
                    [
                        "model",
                        "run_date",
                        "target_date",
                        "datetime_hour",
                        "horizon",
                        "forecast_entries",
                        "forecast_exits",
                        "actual_entries",
                        "actual_exits"
                    ]
                ]
            )

        except Exception as e:

            print(
                f"Hourly load failed: {hourly_file.name}"
            )

            print(e)

if old_daily_results:
    old_daily = pd.concat(
        old_daily_results,
        ignore_index=True
    )
else:
    old_daily = pd.DataFrame()


if old_hourly_results:
    old_hourly = pd.concat(
        old_hourly_results,
        ignore_index=True
    )
else:
    old_hourly = pd.DataFrame()

print(
    f"Old daily rows: {len(old_daily):,}"
)

print(
    f"Old hourly rows: {len(old_hourly):,}"
)

# =============================================================================
# NEW MODEL BACKTEST
# =============================================================================

print("Running new model backtest...")

new_daily_results = []

new_hourly_results = []

run_dates = pd.date_range(
    START_DATE,
    END_DATE,
    freq="D",
)

print(
    f"Benchmarking {len(run_dates)} run dates "
    f"from {run_dates.min().date()} "
    f"to {run_dates.max().date()}"
)

for i, run_date in enumerate(run_dates):

    print(
        f"\n[{i+1}/{len(run_dates)}] "
        f"Running {run_date.date()}"
    )

    try:

        config = PRODUCTION_CONFIG.copy()
        config["current_run_date"] = run_date

        daily_fcst, hourly_fcst = run_production_forecast(
            config, save_outputs=False
        )

        temp = daily_fcst.copy()

        temp["target_date"] = pd.to_datetime(
            temp["target_date"]
        ).dt.normalize()

        temp = temp.merge(
            daily_actuals,
            on="target_date",
            how="left",
        )

        temp["run_date"] = run_date
        temp["model"] = "New"

        temp.rename(
            columns={
                "entry_forecast": "forecast_entries",
                "exit_forecast": "forecast_exits",
                "horizon_days": "horizon",
            },
            inplace=True,
        )

        new_daily_results.append(
            temp[
                [
                    "model",
                    "run_date",
                    "target_date",
                    "horizon",
                    "forecast_entries",
                    "forecast_exits",
                    "actual_entries",
                    "actual_exits",
                ]
            ]
        )

        print(run_date.date())

        # ============================================================================
        # NEW HOURLY FORECAST
        # ============================================================================

        hourly_temp = hourly_fcst.copy()

        hourly_temp["datetime_hour"] = pd.to_datetime(
            hourly_temp["datetime_hour"]
        )

        hourly_temp["target_date"] = (
            hourly_temp["datetime_hour"]
            .dt.normalize()
        )

        hourly_temp["horizon"] = (
            hourly_temp["target_date"]
            - run_date
        ).dt.days


        hourly_temp = hourly_temp.merge(
            hourly_actuals,
            on="datetime_hour",
            how="left"
        )

        hourly_temp["run_date"] = run_date
        hourly_temp["model"] = "New"

        hourly_temp.rename(
            columns={
                "entry_forecast_hourly": "forecast_entries",
                "exit_forecast_hourly": "forecast_exits"
            },
            inplace=True,
        )

        new_hourly_results.append(
            hourly_temp[
                [
                    "model",
                    "run_date",
                    "target_date",
                    "datetime_hour",
                    "horizon",
                    "forecast_entries",
                    "forecast_exits",
                    "actual_entries",
                    "actual_exits"
                ]
            ]
        )

        # =====================================================
        # CHECKPOINT SAVE EVERY 5 RUNS
        # =====================================================

        if (i + 1) % 5 == 0:

            try:

                print(
                    f"Saving checkpoint ({i+1}/{len(run_dates)})..."
                )

                pd.concat(
                    new_daily_results,
                    ignore_index=True
                ).to_csv(
                    "outputs/comparison/temp_daily.csv",
                    index=False
                )

                pd.concat(
                    new_hourly_results,
                    ignore_index=True
                ).to_csv(
                    "outputs/comparison/temp_hourly.csv",
                    index=False
                )

            except Exception as e:

                print(
                    f"Checkpoint save failed: {e}"
                )
    except Exception as e:
        
        print(
            f"Run failed for {run_date.date()}: {e}"
        )



if new_daily_results:
    new_daily = pd.concat(
        new_daily_results,
        ignore_index=True
    )
else:
    new_daily = pd.DataFrame()

if new_hourly_results:
    new_hourly = pd.concat(
        new_hourly_results,
        ignore_index=True
    )
else:
    new_hourly = pd.DataFrame()

print(
    f"New daily rows: {len(new_daily):,}"
)

print(
    f"New hourly rows: {len(new_hourly):,}"
)

# =============================================================================
# COMBINE
# =============================================================================



results = pd.concat(
    [
        old_daily,
        new_daily,
    ],
    ignore_index=True,
)

hourly_results = pd.concat(
    [
        old_hourly,
        new_hourly
    ],
    ignore_index=True
)

# remove forecasts for dates already in the past
results = results[
    results["horizon"] >= 0
].copy()


hourly_results = hourly_results[
    hourly_results["horizon"] >= 0
].copy()

results = results[
    results["horizon"] <= 56
]

hourly_results = hourly_results[
    hourly_results["horizon"] <= 56
]

print(
    f"Benchmark daily rows: {len(results):,}"
)

print(
    f"Benchmark hourly rows: {len(hourly_results):,}"
)

results = results.dropna(
    subset=[
        "forecast_entries",
        "forecast_exits"
    ]
)

results = results.dropna(
    subset=[
        "actual_entries",
        "actual_exits"
    ]
)

hourly_results = hourly_results.dropna(
    subset=[
        "forecast_entries",
        "forecast_exits"
    ]
)

hourly_results = hourly_results.dropna(
    subset=[
        "actual_entries",
        "actual_exits"
    ]
)

results["forecast_total"] = (
    results["forecast_entries"]
    + results["forecast_exits"]
)

results["actual_total"] = (
    results["actual_entries"]
    + results["actual_exits"]
)

hourly_results["forecast_total"] = (
    hourly_results["forecast_entries"]
    + hourly_results["forecast_exits"]
)

hourly_results["actual_total"] = (
    hourly_results["actual_entries"]
    + hourly_results["actual_exits"]
)


# =============================================================================
# ERROR METRICS
# =============================================================================

results["entry_mape"] = results.apply(
    lambda x: mape(
        x["actual_entries"],
        x["forecast_entries"],
    ),
    axis=1,
)

results["exit_mape"] = results.apply(
    lambda x: mape(
        x["actual_exits"],
        x["forecast_exits"],
    ),
    axis=1,
)

results["total_mape"] = results.apply(
    lambda x: mape(
        x["actual_total"],
        x["forecast_total"],
    ),
    axis=1,
)

results["entry_accuracy"] = (
    1 - results["entry_mape"]
)

results["exit_accuracy"] = (
    1 - results["exit_mape"]
)

results["total_accuracy"] = (
    1 - results["total_mape"]
)

hourly_results["entry_mape"] = hourly_results.apply(
    lambda x: mape(
        x["actual_entries"],
        x["forecast_entries"]
    ),
    axis=1
)

hourly_results["exit_mape"] = hourly_results.apply(
    lambda x: mape(
        x["actual_exits"],
        x["forecast_exits"]
    ),
    axis=1
)

hourly_results["total_mape"] = hourly_results.apply(
    lambda x: mape(
        x["actual_total"],
        x["forecast_total"]
    ),
    axis=1
)

hourly_results.replace(
    [np.inf, -np.inf],
    np.nan,
    inplace=True
)

hourly_results["hour"] = (
    hourly_results["datetime_hour"]
    .dt.hour
)

hourly_results["weekday"] = (
    hourly_results["datetime_hour"]
    .dt.day_name()
)

hourly_results["month"] = (
    hourly_results["datetime_hour"]
    .dt.to_period("M")
    .astype(str)
)

hourly_results["week"] = (
    hourly_results["datetime_hour"]
    .dt.isocalendar()
    .week
)

# =============================================================================
# DATE FEATURES
# =============================================================================

results["weekday"] = (
    pd.to_datetime(
        results["target_date"]
    ).dt.day_name()
)

results["month"] = (
    pd.to_datetime(
        results["target_date"]
    ).dt.to_period("M")
    .astype(str)
)

results["week"] = (
    pd.to_datetime(
        results["target_date"]
    ).dt.isocalendar().week
)

# =============================================================================
# SUMMARIES
# =============================================================================

overall_summary = (
    results
    .groupby("model")
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape",
            "entry_accuracy",
            "exit_accuracy",
            "total_accuracy",
        ]
    ]
    .mean()
)

comparison_summary = (
    results
    .groupby("model")
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape"
        ]
    ]
    .mean()
    .round(4)
)

horizon_summary = (
    results
    .groupby(
        [
            "model",
            "horizon",
        ]
    )
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape",
        ]
    ]
    .mean()
    .reset_index()
)

weekday_summary = (
    results
    .groupby(
        [
            "model",
            "weekday",
        ]
    )
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape",
        ]
    ]
    .mean()
    .reset_index()
)

month_summary = (
    results
    .groupby(
        [
            "model",
            "month",
        ]
    )
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape",
        ]
    ]
    .mean()
    .reset_index()
)

week_summary = (
    results
    .groupby(
        [
            "model",
            "week",
        ]
    )
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape",
        ]
    ]
    .mean()
    .reset_index()
)

overall_hourly = (
    hourly_results
    .groupby("model")
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape"
        ]
    ]
    .mean()
)

hourly_comparison_summary = (
    hourly_results
    .groupby("model")
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape"
        ]
    ]
    .mean()
    .round(4)
)

hourly_by_horizon = (
    hourly_results
    .groupby(
        [
            "model",
            "horizon"
        ]
    )
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape"
        ]
    ]
    .mean()
    .reset_index()
)

hourly_by_hour = (
    hourly_results
    .groupby(
        [
            "model",
            "hour"
        ]
    )
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape"
        ]
    ]
    .mean()
    .reset_index()
)

hourly_weekday_hour = (
    hourly_results
    .groupby(
        [
            "model",
            "weekday",
            "hour"
        ]
    )
    [
        [
            "entry_mape",
            "exit_mape",
            "total_mape"
        ]
    ]
    .mean()
    .reset_index()
)

# =============================================================================
# SAVE
# =============================================================================

print("Writing workbook...")

with pd.ExcelWriter(
    OUTPUT_FILE,
    engine="openpyxl",
) as writer:

    results.to_excel(
        writer,
        sheet_name="Raw_Daily",
        index=False,
    )

    overall_summary.to_excel(
        writer,
        sheet_name="Overall",
    )

    comparison_summary.to_excel(
        writer,
        sheet_name="Daily_Comparison",
    )

    horizon_summary.to_excel(
        writer,
        sheet_name="By_Horizon",
        index=False,
    )

    weekday_summary.to_excel(
        writer,
        sheet_name="By_Weekday",
        index=False,
    )

    month_summary.to_excel(
        writer,
        sheet_name="By_Month",
        index=False,
    )

    week_summary.to_excel(
        writer,
        sheet_name="By_Week",
        index=False,
    )

    hourly_results.to_excel(
    writer,
    sheet_name="Raw_Hourly",
    index=False
    )

    overall_hourly.to_excel(
        writer,
        sheet_name="Hourly_Overall"
    )

    hourly_comparison_summary.to_excel(
        writer,
        sheet_name="Hourly_Comparison"
    )

    hourly_by_horizon.to_excel(
        writer,
        sheet_name="Hourly_Horizon",
        index=False
    )

    hourly_by_hour.to_excel(
        writer,
        sheet_name="Hourly_By_Hour",
        index=False
    )

    hourly_weekday_hour.to_excel(
        writer,
        sheet_name="Hourly_Weekday_Hour",
        index=False
    )

print("")
print("Done")
print(OUTPUT_FILE)