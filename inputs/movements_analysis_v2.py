import os
import pandas as pd
import numpy as np

# ============================================================
# VERSION 2 - W24 VS W25 PRODUCTIVITY COMPARISON
# SIMPLIFIED OUTPUT WORKBOOK
# ============================================================
#
# INPUTS:
#   inputs/movements_W24.csv
#   inputs/movements_W25.csv
#
# OUTPUT:
#   inputs/winter_productivity_comparison_v3.xlsx
#
# OUTPUT SHEETS:
#   1. Notes
#   2. Overall Comparison
#   3. Hourly - Driver Level
#   4. Hourly - Adj Driver Level
#   5. Hourly - System Level
#
# METRICS:
#
# 1. Driver Level
#    Average raw movements per active driver-hour.
#    For each driver/date/hour, count movements.
#    Then average those driver-hour rows.
#
# 2. Adjusted Driver Level
#    Average adjusted movements per active driver-hour.
#    Same as Driver Level, but same-location movements are excluded.
#
# 3. System Level
#    Total adjusted movements divided by total active driver-hours.
#    This is the most operational/stakeholder-friendly metric.
#
# IMPORTANT CLEANING LOGIC:
#   - Same-location movements are excluded from adjusted metrics.
#   - Zero-duration rows are NOT removed from productivity metrics.
#   - This is because W24 records many movements as a single timestamp,
#     e.g. "9:59", which creates start_time = end_time.
#
# ============================================================


# =========================
# CONFIG
# =========================

INPUT_FILES = {
    "2024": "inputs/movements_W24.csv",
    "2025": "inputs/movements_W25.csv"
}

OUTPUT_EXCEL = "inputs/winter_productivity_comparison_v2.xlsx"

HOUR_COLUMNS = [f"{h:02d}:00" for h in range(24)]


# =========================
# HELPER FUNCTIONS
# =========================

def extract_location(x):
    """
    Cleans a location by taking the text before '/'.
    Example:
        'Terminal / Zone A' -> 'Terminal'
    """
    if pd.isna(x):
        return x
    return str(x).split("/")[0].strip()


def normalise_time_string(x):
    """
    Converts:
        '9:59'  -> '09:59'
        '0:54'  -> '00:54'
        '23:56' -> '23:56'
    """
    if pd.isna(x):
        return x

    x = str(x).strip()

    if ":" not in x:
        return np.nan

    parts = x.split(":")

    if len(parts) != 2:
        return np.nan

    hour = parts[0]
    minute = parts[1]

    try:
        return f"{int(hour):02d}:{int(minute):02d}"
    except:
        return np.nan


def load_and_prepare_file(comparison_year, filepath):
    """
    Loads one movement file and applies:
      - column standardisation
      - location cleaning
      - same-location flag
      - robust time parsing
      - date/hour fields
    """

    df = pd.read_csv(filepath)

    # Standardise column names
    df.columns = df.columns.str.strip().str.lower()

    # Add comparison year
    df["comparison_year"] = comparison_year

    # Required columns
    required_cols = ["date", "time", "driver", "from", "to"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(
            f"{filepath} is missing required columns: {missing_cols}"
        )

    # =========================
    # CLEAN FROM / TO
    # =========================

    df["from_clean"] = df["from"].apply(extract_location)
    df["to_clean"] = df["to"].apply(extract_location)

    df["same_location"] = df["from_clean"] == df["to_clean"]

    # =========================
    # PARSE TIME
    # =========================

    time_text = df["time"].astype(str).str.strip()

    # Handles ranges:
    # "23:55 - 23:57 (2 mins)"
    # "9:55 - 10:02"
    range_times = time_text.str.extract(
        r"(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})"
    )

    df["start_time"] = range_times[0]
    df["end_time"] = range_times[1]

    # Handles single times:
    # "23:56"
    # "9:59"
    # "0:54"
    single_times = time_text.str.extract(
        r"^(\d{1,2}:\d{2})$"
    )[0]

    df["start_time"] = df["start_time"].fillna(single_times)
    df["end_time"] = df["end_time"].fillna(single_times)

    # Normalise to HH:MM
    df["start_time"] = df["start_time"].apply(normalise_time_string)
    df["end_time"] = df["end_time"].apply(normalise_time_string)

    # Combine date and time
    df["start_dt"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["start_time"],
        dayfirst=True,
        errors="coerce"
    )

    df["end_dt"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["end_time"],
        dayfirst=True,
        errors="coerce"
    )

    # Fix overnight movement ranges
    df.loc[df["end_dt"] < df["start_dt"], "end_dt"] += pd.Timedelta(days=1)

    # Duration is calculated but NOT used to exclude productivity rows
    df["duration_hours"] = (
        (df["end_dt"] - df["start_dt"]).dt.total_seconds() / 3600
    )

    # Date/hour fields
    df["date_only"] = df["start_dt"].dt.date
    df["hour"] = df["start_dt"].dt.hour

    df["hour_label"] = df["hour"].apply(
        lambda x: f"{int(x):02d}:00" if pd.notna(x) else np.nan
    )

    return df


def safe_yoy(current_value, previous_value):
    """
    YoY formula:
        current / previous - 1
    """
    if pd.isna(previous_value) or previous_value == 0:
        return np.nan
    return (current_value / previous_value) - 1


def format_yoy(x):
    """
    Formats decimal YoY as percentage text.
    """
    if pd.isna(x):
        return ""
    return f"{round(x * 100, 1)}%"


def create_overall_row(metric_name, value_2024, value_2025):
    """
    Creates one overall comparison row.
    """
    yoy = safe_yoy(value_2025, value_2024)

    return {
        "metric": metric_name,
        "2024": round(value_2024, 2) if pd.notna(value_2024) else np.nan,
        "2025": round(value_2025, 2) if pd.notna(value_2025) else np.nan,
        "YoY Change": format_yoy(yoy)
    }


def create_hourly_comparison(hourly_source, value_col):
    """
    Creates a heatmap-style hourly comparison table:

        M/Hr | 00:00 | 01:00 | ... | 23:00
        2025 | x.xx
        2024 | x.xx
        Diff | x%
    """

    pivot = hourly_source.pivot(
        index="comparison_year",
        columns="hour_label",
        values=value_col
    )

    # Ensure all hour columns exist
    for col in HOUR_COLUMNS:
        if col not in pivot.columns:
            pivot[col] = np.nan

    pivot = pivot[HOUR_COLUMNS]

    if "2024" in pivot.index:
        values_2024 = pivot.loc["2024"]
    else:
        values_2024 = pd.Series(index=HOUR_COLUMNS, dtype=float)

    if "2025" in pivot.index:
        values_2025 = pivot.loc["2025"]
    else:
        values_2025 = pd.Series(index=HOUR_COLUMNS, dtype=float)

    diff = (values_2025 / values_2024) - 1
    diff = diff.replace([np.inf, -np.inf], np.nan)

    output = pd.DataFrame(
        [
            ["2025"] + list(values_2025.round(2)),
            ["2024"] + list(values_2024.round(2)),
            ["Diff"] + list(diff.apply(format_yoy))
        ],
        columns=["M/Hr"] + HOUR_COLUMNS
    )

    return output


def add_excel_formatting(writer, sheet_name, df):
    """
    Light formatting:
      - freeze top row
      - filter
      - sensible column widths
    """
    worksheet = writer.sheets[sheet_name]

    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions

    for col_idx, col_name in enumerate(df.columns, start=1):
        max_length = max(
            len(str(col_name)),
            df[col_name].astype(str).map(len).max() if len(df) > 0 else 0
        )
        adjusted_width = min(max(max_length + 2, 10), 35)
        worksheet.column_dimensions[
            worksheet.cell(row=1, column=col_idx).column_letter
        ].width = adjusted_width


# =========================
# 1. LOAD BOTH FILES
# =========================

all_dfs = []

for comparison_year, filepath in INPUT_FILES.items():
    print(f"Loading {comparison_year}: {filepath}")
    period_df = load_and_prepare_file(comparison_year, filepath)
    all_dfs.append(period_df)

df = pd.concat(all_dfs, ignore_index=True)


# =========================
# 2. PARSING CHECK
# =========================

parse_check = df.groupby("comparison_year").agg(
    total_rows=("time", "count"),
    parsed_rows=("start_dt", lambda x: x.notna().sum()),
    unparsed_rows=("start_dt", lambda x: x.isna().sum())
).reset_index()

print("\n=== TIME PARSING CHECK ===")
print(parse_check)

if df["start_dt"].isna().any():
    print("\n=== SAMPLE UNPARSED ROWS ===")
    print(
        df.loc[
            df["start_dt"].isna(),
            ["comparison_year", "date", "time"]
        ].head(30)
    )


# Drop rows where no usable datetime could be parsed
df = df[df["start_dt"].notna()].copy()


# =========================
# 3. ADJUSTED MOVEMENT BASE
# =========================
#
# Adjusted = exclude same-location only.
# Zero-duration rows are retained.
# =========================

df_adj = df[~df["same_location"]].copy()


# =========================
# 4. SYSTEM-LEVEL DATE/HOUR BASE
# =========================
#
# This is used for System Level:
#
#   total adjusted movements / total active driver-hours
#
# Active driver-hours means:
#   for each date/hour, count distinct active drivers,
#   then sum those driver counts across the period.
# =========================

system_hourly_by_date = df.groupby(
    ["comparison_year", "date_only", "hour", "hour_label"]
).agg(
    raw_movements=("time", "count"),
    active_drivers=("driver", "nunique")
).reset_index()

system_hourly_adj_by_date = df_adj.groupby(
    ["comparison_year", "date_only", "hour", "hour_label"]
).agg(
    adjusted_movements=("time", "count")
).reset_index()

system_hourly_by_date = system_hourly_by_date.merge(
    system_hourly_adj_by_date,
    on=["comparison_year", "date_only", "hour", "hour_label"],
    how="left"
)

system_hourly_by_date["adjusted_movements"] = (
    system_hourly_by_date["adjusted_movements"].fillna(0)
)

system_hourly_by_date["system_level_metric"] = (
    system_hourly_by_date["adjusted_movements"] /
    system_hourly_by_date["active_drivers"]
)


# =========================
# 5. SYSTEM-LEVEL HOURLY COMPARISON BASE
# =========================

system_hourly = system_hourly_by_date.groupby(
    ["comparison_year", "hour", "hour_label"]
).agg(
    adjusted_movements=("adjusted_movements", "sum"),
    active_driver_hours=("active_drivers", "sum")
).reset_index()

system_hourly["system_level_metric"] = (
    system_hourly["adjusted_movements"] /
    system_hourly["active_driver_hours"]
)


# =========================
# 6. DRIVER-LEVEL DATE/HOUR BASE
# =========================
#
# One row per:
#   comparison_year / driver / date / hour
#
# raw_movements:
#   all movements
#
# adjusted_movements:
#   same-location excluded
# =========================

driver_hour_detail = df.groupby(
    ["comparison_year", "driver", "date_only", "hour", "hour_label"]
).agg(
    raw_movements=("time", "count")
).reset_index()

driver_hour_adj = df_adj.groupby(
    ["comparison_year", "driver", "date_only", "hour", "hour_label"]
).agg(
    adjusted_movements=("time", "count")
).reset_index()

driver_hour_detail = driver_hour_detail.merge(
    driver_hour_adj,
    on=["comparison_year", "driver", "date_only", "hour", "hour_label"],
    how="left"
)

driver_hour_detail["adjusted_movements"] = (
    driver_hour_detail["adjusted_movements"].fillna(0)
)


# =========================
# 7. DRIVER-LEVEL HOURLY COMPARISON BASE
# =========================
#
# Driver Level:
#   average raw movements across driver/date/hour rows
#
# Adjusted Driver Level:
#   average adjusted movements across driver/date/hour rows
# =========================

driver_hourly = driver_hour_detail.groupby(
    ["comparison_year", "hour", "hour_label"]
).agg(
    driver_level_metric=("raw_movements", "mean"),
    adjusted_driver_level_metric=("adjusted_movements", "mean")
).reset_index()


# =========================
# 8. OVERALL COMPARISON
# =========================

# Driver-level overall
driver_overall = driver_hour_detail.groupby("comparison_year").agg(
    driver_level_metric=("raw_movements", "mean"),
    adjusted_driver_level_metric=("adjusted_movements", "mean")
).reset_index()

# System-level overall
system_overall = system_hourly_by_date.groupby("comparison_year").agg(
    adjusted_movements=("adjusted_movements", "sum"),
    active_driver_hours=("active_drivers", "sum")
).reset_index()

system_overall["system_level_metric"] = (
    system_overall["adjusted_movements"] /
    system_overall["active_driver_hours"]
)


def get_metric_value(source_df, year, metric_col):
    values = source_df.loc[
        source_df["comparison_year"] == year,
        metric_col
    ]

    if len(values) == 0:
        return np.nan

    return values.iloc[0]


driver_2024 = get_metric_value(driver_overall, "2024", "driver_level_metric")
driver_2025 = get_metric_value(driver_overall, "2025", "driver_level_metric")

adj_driver_2024 = get_metric_value(driver_overall, "2024", "adjusted_driver_level_metric")
adj_driver_2025 = get_metric_value(driver_overall, "2025", "adjusted_driver_level_metric")

system_2024 = get_metric_value(system_overall, "2024", "system_level_metric")
system_2025 = get_metric_value(system_overall, "2025", "system_level_metric")

overall_comparison = pd.DataFrame([
    create_overall_row(
        "Driver Level",
        driver_2024,
        driver_2025
    ),
    create_overall_row(
        "Adjusted Driver Level",
        adj_driver_2024,
        adj_driver_2025
    ),
    create_overall_row(
        "System Level",
        system_2024,
        system_2025
    )
])


# =========================
# 9. HOURLY OUTPUT SHEETS
# =========================

hourly_driver_level = create_hourly_comparison(
    hourly_source=driver_hourly,
    value_col="driver_level_metric"
)

hourly_adjusted_driver_level = create_hourly_comparison(
    hourly_source=driver_hourly,
    value_col="adjusted_driver_level_metric"
)

hourly_system_level = create_hourly_comparison(
    hourly_source=system_hourly,
    value_col="system_level_metric"
)


# =========================
# 10. NOTES SHEET
# =========================

notes = pd.DataFrame({
    "metric": [
        "Driver Level",
        "Adjusted Driver Level",
        "System Level",
        "Cleaning rule",
        "Zero-duration rule",
        "Recommended use"
    ],
    "definition": [
        "Average raw movements per active driver-hour. For each driver/date/hour, movements are counted and then averaged across driver-hour rows.",
        "Average adjusted movements per active driver-hour. Same as Driver Level, but same-location movements are excluded before counting movements.",
        "Total adjusted movements divided by total active driver-hours. This is calculated at operation/date/hour level and is the most suitable stakeholder-facing productivity metric.",
        "Adjusted metrics exclude movements where cleaned from and to locations are identical.",
        "Zero-duration rows are retained because W24 records many movements as single timestamps. Removing zero-duration rows would unfairly remove valid W24 records.",
        "Use System Level as the main overall productivity comparison. Use Driver Level and Adjusted Driver Level to explain/validate whether the pattern is also visible when looking at individual driver-hour behaviour."
    ]
})


# =========================
# 11. WRITE EXCEL WORKBOOK
# =========================

with pd.ExcelWriter(OUTPUT_EXCEL, engine="openpyxl") as writer:

    notes.to_excel(
        writer,
        sheet_name="Notes",
        index=False
    )

    overall_comparison.to_excel(
        writer,
        sheet_name="Overall Comparison",
        index=False
    )

    hourly_driver_level.to_excel(
        writer,
        sheet_name="Hourly - Driver Level",
        index=False
    )

    hourly_adjusted_driver_level.to_excel(
        writer,
        sheet_name="Hourly - Adj Driver",
        index=False
    )

    hourly_system_level.to_excel(
        writer,
        sheet_name="Hourly - System Level",
        index=False
    )

    sheets_to_format = {
        "Notes": notes,
        "Overall Comparison": overall_comparison,
        "Hourly - Driver Level": hourly_driver_level,
        "Hourly - Adj Driver": hourly_adjusted_driver_level,
        "Hourly - System Level": hourly_system_level
    }

    for sheet_name, sheet_df in sheets_to_format.items():
        add_excel_formatting(writer, sheet_name, sheet_df)


# =========================
# 12. FINAL PRINTS
# =========================

print("\n============================================================")
print("VERSION 3 COMPLETE")
print("============================================================")
print(f"Excel workbook created: {OUTPUT_EXCEL}")
print(f"Full path: {os.path.abspath(OUTPUT_EXCEL)}")
print("")
print("Workbook sheets:")
print("- Notes")
print("- Overall Comparison")
print("- Hourly - Driver Level")
print("- Hourly - Adj Driver")
print("- Hourly - System Level")
print("============================================================")

print("\n=== OVERALL COMPARISON ===")
print(overall_comparison)

print("\n=== HOURLY - DRIVER LEVEL ===")
print(hourly_driver_level)

print("\n=== HOURLY - ADJUSTED DRIVER LEVEL ===")
print(hourly_adjusted_driver_level)

print("\n=== HOURLY - SYSTEM LEVEL ===")
print(hourly_system_level)