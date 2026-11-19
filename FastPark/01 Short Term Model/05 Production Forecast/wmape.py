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

FORECAST_FOLDER = Path(
    r"C:\Users\jamie_douglas\Edinburgh Airport Limited\Business Planning - Documents\02. Capacity Areas\09. Car Parking\01. FastPark\FastPark Model Rebuild 2025\FastPark Short-Term Model Rebuild\Daily Forecast Output"
)

OUTPUT_FILE = (
    Path("outputs")
    / "comparison"
    / "FastPark_wMAPE_Comparison.xlsx"
)

# =============================================================================
# LOAD NEW MODEL DATE RANGE
# =============================================================================

new_hourly_dates = pd.read_csv(
    "outputs/comparison/temp_hourly.csv",
    usecols=["run_date"]
)

new_hourly_dates["run_date"] = pd.to_datetime(
    new_hourly_dates["run_date"]
)

START_DATE = (
    new_hourly_dates["run_date"]
    .min()
    .normalize()
)

END_DATE = (
    new_hourly_dates["run_date"]
    .max()
    .normalize()
)

MAX_HORIZON = 56

print(f"Using run dates from {START_DATE.date()} to {END_DATE.date()}")

# =============================================================================
# HELPERS
# =============================================================================

def wmape(actual, forecast):

    actual = pd.Series(actual)
    forecast = pd.Series(forecast)

    valid = (
        actual.notna()
        & forecast.notna()
    )

    actual = actual[valid]
    forecast = forecast[valid]

    if actual.sum() == 0:
        return np.nan

    return (
        (actual - forecast).abs().sum()
        / actual.sum()
    )

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
# OLD MODEL
# =============================================================================

print("Loading old forecasts...")

old_daily_results = []
old_hourly_results = []

for forecast_file in sorted(
    FORECAST_FOLDER.glob("*_daily_forecast.csv")
):

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

    entry_col = next(
        (
            c for c in ["Entries", "entry_forecast"]
            if c in forecast_df.columns
        ),
        None
    )

    exit_col = next(
        (
            c for c in ["Exits", "exit_forecast"]
            if c in forecast_df.columns
        ),
        None
    )

    if entry_col is None or exit_col is None:
        continue

    forecast_df["target_date"] = pd.to_datetime(
        forecast_df["Date"], dayfirst=True, errors="coerce"
    ).dt.normalize()

    forecast_df["forecast_entries"] = forecast_df[
        entry_col
    ]

    forecast_df["forecast_exits"] = forecast_df[
        exit_col
    ]

    merged = forecast_df.merge(
        daily_actuals,
        on="target_date",
        how="left"
    )

    merged["run_date"] = run_date
    merged["model"] = "Old"

    merged["horizon"] = (
        merged["target_date"]
        - merged["run_date"]
    ).dt.days

    merged = merged[merged["horizon"] >= 0].copy()

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
                "actual_exits"
            ]
        ]
    )

    hourly_file = Path(
        str(forecast_file).replace(
            "_daily_forecast.csv",
            "_daily_forecast_DWH.csv"
        )
    )

    if hourly_file.exists():

        hourly_df = pd.read_csv(hourly_file)

        hourly_df["Date"] = pd.to_datetime(
            hourly_df["Date"], dayfirst=True, errors="coerce"
        )

        hourly_df["datetime_hour"] = (
            hourly_df["Date"]
            + pd.to_timedelta(
                hourly_df[
                    "intervalStartTime_Local"
                ].astype(str)
            )
        )

        hourly_df.rename(
            columns={
                "Entries": "forecast_entries",
                "Exits": "forecast_exits"
            },
            inplace=True,
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

        hourly_df = hourly_df[hourly_df["horizon"] >= 0].copy()

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
                    "actual_exits",
                ]
            ]
        )

old_daily = pd.concat(
    old_daily_results,
    ignore_index=True
)

old_hourly = pd.concat(
    old_hourly_results,
    ignore_index=True
)

print(
    f"Old run dates: {old_hourly['run_date'].nunique()}"
)

print(
    old_hourly['run_date'].min(),
    old_hourly['run_date'].max()
)

print(old_daily.head(20))
print(old_hourly.head(20))

# =============================================================================
# NEW MODEL
# =============================================================================

print("Loading temp files...")

new_daily = pd.read_csv(
    "outputs/comparison/temp_daily.csv"
)

new_hourly = pd.read_csv(
    "outputs/comparison/temp_hourly.csv"
)

new_daily["model"] = "New"
new_hourly["model"] = "New"

new_daily["target_date"] = pd.to_datetime(
    new_daily["target_date"]
)

new_hourly["datetime_hour"] = pd.to_datetime(
    new_hourly["datetime_hour"]
)

new_hourly["run_date"] = pd.to_datetime(
    new_hourly["run_date"]
)

print(
    f"New run dates: {new_hourly['run_date'].nunique()}"
)

print(
    new_hourly['run_date'].min(),
    new_hourly['run_date'].max()
)

old_runs = set(
    pd.to_datetime(old_hourly["run_date"]).dt.normalize()
)

new_runs = set(
    pd.to_datetime(new_hourly["run_date"]).dt.normalize()
)

common_runs = old_runs.intersection(new_runs)

old_hourly = old_hourly[
    pd.to_datetime(old_hourly["run_date"])
    .dt.normalize()
    .isin(common_runs)
]

new_hourly = new_hourly[
    pd.to_datetime(new_hourly["run_date"])
    .dt.normalize()
    .isin(common_runs)
]

print(
    "Common run dates:",
    len(common_runs)
)

# =============================================================================
# COMBINE
# =============================================================================

results = pd.concat(
    [old_daily, new_daily],
    ignore_index=True
)

hourly_results = pd.concat(
    [old_hourly, new_hourly],
    ignore_index=True
)

for df in [results, hourly_results]:

    df["forecast_total"] = (
        df["forecast_entries"]
        + df["forecast_exits"]
    )

    df["actual_total"] = (
        df["actual_entries"]
        + df["actual_exits"]
    )

# =============================================================================
# DAILY WMAPE
# =============================================================================

daily_summary = (
    results
    .groupby("model")
    .apply(
        lambda g: pd.Series({
            "entry_wmape": wmape(
                g["actual_entries"],
                g["forecast_entries"]
            ),
            "exit_wmape": wmape(
                g["actual_exits"],
                g["forecast_exits"]
            ),
            "total_wmape": wmape(
                g["actual_total"],
                g["forecast_total"]
            )
        })
    )
)

daily_horizon = (
    results
    .groupby(
        ["model", "horizon"]
    )
    .apply(
        lambda g: pd.Series({
            "entry_wmape": wmape(
                g["actual_entries"],
                g["forecast_entries"]
            ),
            "exit_wmape": wmape(
                g["actual_exits"],
                g["forecast_exits"]
            ),
            "total_wmape": wmape(
                g["actual_total"],
                g["forecast_total"]
            )
        })
    )
    .reset_index()
)

# =============================================================================
# HOURLY WMAPE
# =============================================================================

hourly_results["hour"] = (
    hourly_results["datetime_hour"]
    .dt.hour
)

hourly_summary = (
    hourly_results
    .groupby("model")
    .apply(
        lambda g: pd.Series({
            "entry_wmape": wmape(
                g["actual_entries"],
                g["forecast_entries"]
            ),
            "exit_wmape": wmape(
                g["actual_exits"],
                g["forecast_exits"]
            ),
            "total_wmape": wmape(
                g["actual_total"],
                g["forecast_total"]
            )
        })
    )
)

hourly_horizon = (
    hourly_results
    .groupby(
        ["model", "horizon"]
    )
    .apply(
        lambda g: pd.Series({
            "entry_wmape": wmape(
                g["actual_entries"],
                g["forecast_entries"]
            ),
            "exit_wmape": wmape(
                g["actual_exits"],
                g["forecast_exits"]
            ),
            "total_wmape": wmape(
                g["actual_total"],
                g["forecast_total"]
            )
        })
    )
    .reset_index()
)

hourly_by_hour = (
    hourly_results
    .groupby(
        ["model", "hour"]
    )
    .apply(
        lambda g: pd.Series({
            "entry_wmape": wmape(
                g["actual_entries"],
                g["forecast_entries"]
            ),
            "exit_wmape": wmape(
                g["actual_exits"],
                g["forecast_exits"]
            ),
            "total_wmape": wmape(
                g["actual_total"],
                g["forecast_total"]
            )
        })
    )
    .reset_index()
)

# 1. Determine the maximum common horizon available in both models
# (This ensures we aren't comparing a model that can see 56 days to one that can't)
max_h_old = hourly_results[hourly_results['model'] == 'Old']['horizon'].max()
max_h_new = hourly_results[hourly_results['model'] == 'New']['horizon'].max()
common_max_horizon = min(max_h_old, max_h_new)

# 3. Align the datasets (The "Common Days" logic)
# Ensure both are Datetime objects so they can be compared/sorted
hourly_results['target_date'] = pd.to_datetime(hourly_results['target_date'])

old_dates = hourly_results[hourly_results['model'] == 'Old']['target_date'].dt.normalize().unique()
new_dates = hourly_results[hourly_results['model'] == 'New']['target_date'].dt.normalize().unique()

# Find the intersection using numpy
common_dates = np.intersect1d(old_dates, new_dates)

# 4. Final filter: Keep only data points that fall within these common dates
hourly_results = hourly_results[hourly_results['target_date'].dt.normalize().isin(common_dates)].copy()

print(f"Post-filter row count: {len(hourly_results)}")

print(hourly_results.head(20))


# =============================================================================
# STAFFING IMPACT ANALYSIS
# =============================================================================

# Productivity constant
MOVEMENTS_PER_STAFF_HOUR = 3.5

# Calculate Required Staff for both Forecast and Actuals
# Note: We group by the unique time-point for every forecast run
hourly_results["forecast_staff_needed"] = hourly_results["forecast_total"] / MOVEMENTS_PER_STAFF_HOUR
hourly_results["actual_staff_needed"] = hourly_results["actual_total"] / MOVEMENTS_PER_STAFF_HOUR

# Calculate the Delta
# Positive = Overstaffing (Wasted Cost), Negative = Understaffing (Service Risk)
hourly_results["staff_delta"] = (
    hourly_results["forecast_staff_needed"] - hourly_results["actual_staff_needed"]
)

# Categorize the variance
hourly_results["overstaffed_fte"] = np.where(hourly_results["staff_delta"] > 0, hourly_results["staff_delta"], 0)
hourly_results["understaffed_fte"] = np.where(hourly_results["staff_delta"] < 0, hourly_results["staff_delta"].abs(), 0)

# =============================================================================
# STAFFING IMPACT SUMMARY
# =============================================================================

hourly_results["is_overstaffed"] = hourly_results["staff_delta"] > 0
hourly_results["is_understaffed"] = hourly_results["staff_delta"] < 0
hourly_results["abs_staff_error"] = hourly_results["staff_delta"].abs()

staffing_summary = (
    hourly_results
    .groupby("model")
    .agg(
        observation_count=("staff_delta", "count"),

        overstaffed_hours=("is_overstaffed", "sum"),
        understaffed_hours=("is_understaffed", "sum"),

        avg_overstaff_fte=(
            "staff_delta",
            lambda x: x[x > 0].mean()
        ),

        avg_understaff_fte=(
            "staff_delta",
            lambda x: abs(x[x < 0].mean())
        ),

        avg_abs_staff_error=(
            "abs_staff_error",
            "mean"
        )
    )
)

# Proportion of hours

staffing_summary["pct_overstaffed"] = (
    staffing_summary["overstaffed_hours"]
    / staffing_summary["observation_count"]
)

staffing_summary["pct_understaffed"] = (
    staffing_summary["understaffed_hours"]
    / staffing_summary["observation_count"]
)

# Typical week = 168 hours

staffing_summary["overstaffed_hours_per_week"] = (
    staffing_summary["pct_overstaffed"] * 168
)

staffing_summary["understaffed_hours_per_week"] = (
    staffing_summary["pct_understaffed"] * 168
)

# Weekly FTE-hours impact

staffing_summary["weekly_overstaff_fte_hours"] = (
    staffing_summary["overstaffed_hours_per_week"]
    * staffing_summary["avg_overstaff_fte"]
)

staffing_summary["weekly_understaff_fte_hours"] = (
    staffing_summary["understaffed_hours_per_week"]
    * staffing_summary["avg_understaff_fte"]
)

# Convert 40 hours = 1 FTE

staffing_summary["weekly_overstaff_fte"] = (
    staffing_summary["weekly_overstaff_fte_hours"] / 40
)

staffing_summary["weekly_understaff_fte"] = (
    staffing_summary["weekly_understaff_fte_hours"] / 40
)

# =============================================================================
# SAVE
# =============================================================================

with pd.ExcelWriter(
    OUTPUT_FILE,
    engine="openpyxl"
) as writer:

    # 1. Existing Sheets
    daily_summary.to_excel(writer, sheet_name="Daily_wMAPE")
    daily_horizon.to_excel(writer, sheet_name="Daily_Horizon", index=False)
    hourly_summary.to_excel(writer, sheet_name="Hourly_wMAPE")
    hourly_horizon.to_excel(writer, sheet_name="Hourly_Horizon", index=False)
    hourly_by_hour.to_excel(writer, sheet_name="Hourly_By_Hour", index=False)

    # 2. New Staffing Impact Sheets
    # The summary table for the high-level comparison
    staffing_summary.to_excel(writer, sheet_name="Staffing_Impact_Summary")

    hourly_results[['model', 'run_date', 'target_date', 'horizon', 'datetime_hour', 
                'forecast_total', 'actual_total', 'overstaffed_fte', 'understaffed_fte']
               ].to_excel(writer, sheet_name="Staffing_Audit_Trail", index=False)

print("Done. Results saved to:", OUTPUT_FILE)