import os
import pandas as pd
import numpy as np
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

# ============================================================
# AUGUST EVENT MOVEMENT ANALYSIS
# EVENT DATES: 22, 26, 27 AUGUST 2025
# BASELINE: FULL AUGUST 2025 HOURLY AVERAGE
# ============================================================
#
# INPUTS:
#   inputs/Aug_22.xlsx
#   inputs/Aug_2627.xlsx
#   inputs/Aug2025.xlsx
#
# OUTPUT:
#   inputs/august_event_movement_hourly_comparison.xlsx
#
# PURPOSE:
#   Compare hourly movements on the 2025 event dates against the
#   normal August 2025 hourly average.
#
# IMPORTANT TIME LOGIC:
#   These files are treated like the W24 movement file.
#   The time column is a single timestamp, not a HH:MM - HH:MM range.
#
#   Example:
#       9:59
#       13:41
#       0:54
#       23:56
#
#   Therefore:
#       start_dt = timestamp
#       end_dt   = timestamp
#       duration_hours = 0
#
#   Zero-duration rows are retained.
#
# SHIFT LOGIC:
#   For each driver, movements are sorted by time.
#   A new shift is created where the gap between movements is > 6 hours.
#
# ADJUSTED MOVEMENTS:
#   Adjusted movements exclude same-location movements only.
#   They do NOT exclude zero-duration rows.
# ============================================================


# =========================
# CONFIG
# =========================

INPUT_FILES = {
    "Aug_22": {
        "filepath": "inputs/Aug_22.csv",
        "analysis_group": "event"
    },
    "Aug_2627": {
        "filepath": "inputs/Aug_2627.csv",
        "analysis_group": "event"
    },
    "Aug2025": {
        "filepath": "inputs/Aug2025.csv",
        "analysis_group": "baseline"
    }
}

OUTPUT_EXCEL = "inputs/august_event_movement_hourly_comparison.xlsx"

EVENT_DATES = pd.to_datetime([
    "2025-08-22",
    "2025-08-26",
    "2025-08-27"
]).date

HOUR_COLUMNS = [f"{h:02d}:00" for h in range(24)]

NEW_SHIFT_GAP_HOURS = 6

# Because the event is over nights, this lets rows after midnight
# be assigned back to the previous event date.
#
# Example:
#   2025-08-23 01:30 becomes operational_event_date = 2025-08-22
#
# Set to 0 if you do NOT want this.
EVENT_NIGHT_END_HOUR = 0

# You said compare against August 2025 as a whole average.
# Keep this False.
#
# If later you want to exclude 22, 26 and 27 Aug from the baseline,
# change this to True.
EXCLUDE_EVENT_DATES_FROM_BASELINE = False


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

    These files are treated as W24-style single timestamp files.
    """
    if pd.isna(x):
        return np.nan

    x = str(x).strip()

    if ":" not in x:
        return np.nan

    parts = x.split(":")

    if len(parts) != 2:
        return np.nan

    try:
        hour = int(parts[0])
        minute = int(parts[1])

        if hour < 0 or hour > 23:
            return np.nan

        if minute < 0 or minute > 59:
            return np.nan

        return f"{hour:02d}:{minute:02d}"

    except:
        return np.nan


def safe_pct_diff(event_value, baseline_value):
    """
    Percentage difference:
        event / baseline - 1
    """
    if pd.isna(baseline_value) or baseline_value == 0:
        return np.nan

    return (event_value / baseline_value) - 1


def format_pct(x):
    """
    Formats decimal percentage as text.
    """
    if pd.isna(x):
        return ""

    return f"{round(x * 100, 1)}%"


def load_file(filepath):
    """
    Loads CSV movement file.
    """

    return pd.read_csv(filepath)


def add_operational_event_date(df):
    """
    Adds an operational_event_date so that after-midnight rows can be
    associated with the previous event night.

    Example:
        2025-08-23 01:30 with EVENT_NIGHT_END_HOUR = 4
        becomes operational_event_date = 2025-08-22

    This is only used for identifying event-night rows.
    The actual date_only and hour remain unchanged.
    """

    df["operational_event_date"] = df["date_only"]

    if EVENT_NIGHT_END_HOUR > 0:
        after_midnight_mask = (
            df["hour"].notna() &
            (df["hour"] < EVENT_NIGHT_END_HOUR)
        )

        df.loc[
            after_midnight_mask,
            "operational_event_date"
        ] = (
            pd.to_datetime(df.loc[after_midnight_mask, "date_only"])
            - pd.Timedelta(days=1)
        ).dt.date

    return df


def load_and_prepare_file(source_name, filepath, analysis_group):
    """
    Loads one movement Excel file and applies:
      - column standardisation
      - location cleaning
      - same-location flag
      - W24-style single timestamp parsing
      - date/hour fields
      - event-night operational date logic
    """

    df = load_file(filepath)

    # Standardise column names
    df.columns = df.columns.str.strip().str.lower()

    df["source_file"] = source_name
    df["analysis_group"] = analysis_group

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

    df["from_match"] = (
        df["from_clean"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    df["to_match"] = (
        df["to_clean"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # =========================
    # PARSE SINGLE TIMESTAMP
    # W24-STYLE LOGIC
    # =========================

    df["start_time"] = df["time"].apply(normalise_time_string)

    # Single timestamp rows: end_time equals start_time
    df["end_time"] = df["start_time"]

    df["start_dt"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["start_time"],
        dayfirst=True,
        errors="coerce"
    )

    df["end_dt"] = df["start_dt"]

    # Duration is zero for valid W24-style rows.
    # Do not use this to remove rows.
    df["duration_hours"] = 0

    df["date_only"] = df["start_dt"].dt.date
    df["hour"] = df["start_dt"].dt.hour

    df["hour_label"] = df["hour"].apply(
        lambda x: f"{int(x):02d}:00" if pd.notna(x) else np.nan
    )

    df["weekday"] = df["start_dt"].dt.day_name()

    df = add_operational_event_date(df)

    df["is_event_date"] = df["operational_event_date"].isin(EVENT_DATES)

    return df


def add_excel_formatting(writer, sheet_name, df):
    """
    Light Excel formatting:
      - freeze top row
      - auto filter
      - sensible column widths
    """

    worksheet = writer.sheets[sheet_name]

    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions

    for col_idx, col_name in enumerate(df.columns, start=1):

        if len(df) > 0:
            max_length = max(
                len(str(col_name)),
                df[col_name].astype(str).map(len).max()
            )
        else:
            max_length = len(str(col_name))

        adjusted_width = min(max(max_length + 2, 10), 40)

        worksheet.column_dimensions[
            worksheet.cell(row=1, column=col_idx).column_letter
        ].width = adjusted_width


# =========================
# 1. LOAD FILES
# =========================

all_dfs = []

for source_name, config in INPUT_FILES.items():

    print(f"Loading {source_name}: {config['filepath']}")

    period_df = load_and_prepare_file(
        source_name=source_name,
        filepath=config["filepath"],
        analysis_group=config["analysis_group"]
    )

    all_dfs.append(period_df)

df = pd.concat(all_dfs, ignore_index=True)


# =========================
# 2. PARSING CHECK
# =========================

parse_check = df.groupby([
    "source_file",
    "analysis_group"
]).agg(
    total_rows=("time", "count"),
    parsed_rows=("start_dt", lambda x: x.notna().sum()),
    unparsed_rows=("start_dt", lambda x: x.isna().sum()),
    min_start=("start_dt", "min"),
    max_start=("start_dt", "max")
).reset_index()

print("\n=== TIME PARSING CHECK ===")
print(parse_check)

if df["start_dt"].isna().any():
    print("\n=== SAMPLE UNPARSED ROWS ===")
    print(
        df.loc[
            df["start_dt"].isna(),
            ["source_file", "date", "time"]
        ].head(30)
    )

# Drop rows where no usable datetime could be parsed
df = df[df["start_dt"].notna()].copy()


# =========================
# 3. CREATE DRIVER SHIFTS
# =========================
#
# This is the key shift logic from your old scripts.
#
# Each driver's rows are sorted in timestamp order.
# A new shift starts if the gap since the previous movement is > 6 hours.
#
# Grouping by analysis_group prevents the event files and the Aug2025
# baseline file from being mixed together.
# =========================

df = df.sort_values([
    "analysis_group",
    "driver",
    "start_dt",
    "end_dt"
]).copy()

df["prev_end_dt"] = df.groupby([
    "analysis_group",
    "driver"
])["end_dt"].shift(1)

df["gap_from_previous_hours"] = (
    (df["start_dt"] - df["prev_end_dt"]).dt.total_seconds() / 3600
)

df["new_shift"] = (
    df["gap_from_previous_hours"].isna() |
    (df["gap_from_previous_hours"] > NEW_SHIFT_GAP_HOURS)
)

df["shift_id"] = df.groupby([
    "analysis_group",
    "driver"
])["new_shift"].cumsum()

df["driver_shift_key"] = (
    df["analysis_group"].astype(str)
    + "_"
    + df["driver"].astype(str)
    + "_"
    + df["shift_id"].astype(str)
)

shift_creation_check = df.groupby([
    "analysis_group"
]).agg(
    rows=("time", "count"),
    drivers=("driver", "nunique"),
    shifts=("driver_shift_key", "nunique"),
    min_start=("start_dt", "min"),
    max_start=("start_dt", "max")
).reset_index()

print("\n=== SHIFT CREATION CHECK ===")
print(shift_creation_check)


# =========================
# 4. SPLIT EVENT AND BASELINE
# =========================

event_df = df[
    (df["analysis_group"] == "event") &
    (df["is_event_date"])
].copy()

baseline_df = df[
    df["analysis_group"] == "baseline"
].copy()

if EXCLUDE_EVENT_DATES_FROM_BASELINE:
    baseline_df = baseline_df[
        ~baseline_df["operational_event_date"].isin(EVENT_DATES)
    ].copy()


# =========================
# 5. ADJUSTED BASES
# =========================
#
# Adjusted = exclude same-location only.
# Do NOT remove duration_hours == 0.
# These files are W24-style, so zero duration is expected.
# =========================

event_adj_df = event_df[
    ~event_df["same_location"]
].copy()

baseline_adj_df = baseline_df[
    ~baseline_df["same_location"]
].copy()


# =========================
# 6. DRIVER SHIFT SUMMARY
# =========================

shift_summary_raw = df.groupby([
    "analysis_group",
    "driver",
    "shift_id",
    "driver_shift_key"
]).agg(
    shift_start=("start_dt", "min"),
    shift_end=("start_dt", "max"),
    raw_movements=("time", "count")
).reset_index()

shift_summary_adj = df[
    ~df["same_location"]
].groupby([
    "analysis_group",
    "driver",
    "shift_id",
    "driver_shift_key"
]).agg(
    adjusted_movements=("time", "count")
).reset_index()

shift_summary = shift_summary_raw.merge(
    shift_summary_adj,
    on=[
        "analysis_group",
        "driver",
        "shift_id",
        "driver_shift_key"
    ],
    how="left"
)

shift_summary["adjusted_movements"] = (
    shift_summary["adjusted_movements"].fillna(0)
)

shift_summary["shift_hours_observed"] = (
    (shift_summary["shift_end"] - shift_summary["shift_start"])
    .dt.total_seconds() / 3600
)

shift_summary["shift_hours_observed"] = (
    shift_summary["shift_hours_observed"].replace(0, np.nan)
)

shift_summary["raw_movements_per_observed_shift_hour"] = (
    shift_summary["raw_movements"] /
    shift_summary["shift_hours_observed"]
)

shift_summary["adjusted_movements_per_observed_shift_hour"] = (
    shift_summary["adjusted_movements"] /
    shift_summary["shift_hours_observed"]
)

shift_summary[[
    "raw_movements_per_observed_shift_hour",
    "adjusted_movements_per_observed_shift_hour"
]] = shift_summary[[
    "raw_movements_per_observed_shift_hour",
    "adjusted_movements_per_observed_shift_hour"
]].replace([np.inf, -np.inf], np.nan)

print("\n=== DRIVER SHIFT SUMMARY CHECK ===")
print(shift_summary.head())


# ============================================================
# SIMPLE DRIVER-LEVEL SUMMARY OUTPUT ONLY
# ============================================================
#
# Output:
#   Summary
#   Weekday Comparison
#
# Metrics:
#   movements_per_driver_hour:
#       For each driver/date/hour, count adjusted movements.
#       Then average those driver/date/hour rows.
#
#   average_shift_length:
#       Average observed shift length from shift_summary.
#
#   hours_with_increased_movements_per_driver_hour:
#       List of hours where event day driver-level MPH is above
#       the August average driver-level MPH for that hour.
#
#   hours_with_decreased_movements_per_driver_hour:
#       List of hours where event day driver-level MPH is below
#       the August average driver-level MPH for that hour.
#
# Important:
#   Adjusted movements = excludes same-location only.
#   Zero-duration rows are retained.
# ============================================================


# =========================
# HELPER FOR HOUR LISTS
# =========================

def hour_list_from_comparison(compare_df, diff_col, direction):
    """
    Returns a comma-separated list of hour labels.

    direction:
        "increased" means diff > 0
        "decreased" means diff < 0
    """

    if direction == "increased":
        hours = compare_df.loc[
            compare_df[diff_col] > 0,
            "hour_label"
        ].tolist()

    elif direction == "decreased":
        hours = compare_df.loc[
            compare_df[diff_col] < 0,
            "hour_label"
        ].tolist()

    else:
        hours = []

    if len(hours) == 0:
        return ""

    return ", ".join(hours)


def build_driver_hour_detail(source_df):
    """
    Creates driver/date/hour adjusted movement rows.

    One row means:
        one driver
        one date
        one hour

    This is the driver-level denominator you wanted.
    """

    detail = source_df.groupby([
        "driver",
        "date_only",
        "hour",
        "hour_label"
    ]).agg(
        adjusted_movements=("time", "count")
    ).reset_index()

    return detail


# =========================
# 1. DRIVER-HOUR DETAIL
# =========================

event_driver_hour_detail = build_driver_hour_detail(event_adj_df)

baseline_driver_hour_detail = build_driver_hour_detail(baseline_adj_df)


# =========================
# 2. AUGUST DRIVER-LEVEL HOURLY AVERAGE
# =========================
#
# For each hour:
#   average adjusted movements across driver/date/hour rows.
# This should be around 3-ish.
# =========================

august_driver_hourly_avg = baseline_driver_hour_detail.groupby([
    "hour",
    "hour_label"
]).agg(
    august_avg_movements_per_driver_hour=("adjusted_movements", "mean")
).reset_index()

# Force all hours to appear
all_hours = pd.DataFrame({
    "hour": list(range(24)),
    "hour_label": HOUR_COLUMNS
})

august_driver_hourly_avg = all_hours.merge(
    august_driver_hourly_avg,
    on=["hour", "hour_label"],
    how="left"
)


# =========================
# 3. AUGUST SUMMARY ROW
# =========================

august_movements_per_driver_hour = (
    baseline_driver_hour_detail["adjusted_movements"].mean()
)

august_average_shift_length = (
    shift_summary.loc[
        shift_summary["analysis_group"] == "baseline",
        "shift_hours_observed"
    ].mean()
)

august_avg_distinct_staff_per_day = (
    baseline_df.groupby("date_only")["driver"]
    .nunique()
    .mean()
)


# =========================
# 4. EVENT DAY SUMMARY ROWS
# =========================

summary_rows = []

event_dates_sorted = sorted(EVENT_DATES)

for i, event_date in enumerate(event_dates_sorted, start=1):

    event_day_adj = event_adj_df[
        event_adj_df["date_only"] == event_date
    ].copy()

    event_day_staff_count = (
        event_df.loc[
            event_df["date_only"] == event_date,
            "driver"
        ].nunique()
    )

    event_day_driver_hour_detail = event_driver_hour_detail[
        event_driver_hour_detail["date_only"] == event_date
    ].copy()

    # Driver-level MPH for the day
    event_day_movements_per_driver_hour = (
        event_day_driver_hour_detail["adjusted_movements"].mean()
    )

    # Average shift length for that event date
    event_day_shift_summary = shift_summary[
        (shift_summary["analysis_group"] == "event") &
        (shift_summary["shift_start"].dt.date == event_date)
    ].copy()

    event_day_average_shift_length = (
        event_day_shift_summary["shift_hours_observed"].mean()
    )

    # Hourly driver-level average for this event date
    event_day_hourly_avg = event_day_driver_hour_detail.groupby([
        "hour",
        "hour_label"
    ]).agg(
        event_day_movements_per_driver_hour=("adjusted_movements", "mean")
    ).reset_index()

    event_day_hourly_avg = all_hours.merge(
        event_day_hourly_avg,
        on=["hour", "hour_label"],
        how="left"
    )

    # If an event day has no rows in an hour, treat that as 0
    event_day_hourly_avg["event_day_movements_per_driver_hour"] = (
        event_day_hourly_avg["event_day_movements_per_driver_hour"].fillna(0)
    )

    # Compare event day hour vs August average hour
    compare_to_august = event_day_hourly_avg.merge(
        august_driver_hourly_avg,
        on=["hour", "hour_label"],
        how="left"
    )

    compare_to_august["movement_per_driver_hour_diff"] = (
        compare_to_august["event_day_movements_per_driver_hour"] -
        compare_to_august["august_avg_movements_per_driver_hour"]
    )

    increased_hours = hour_list_from_comparison(
        compare_df=compare_to_august,
        diff_col="movement_per_driver_hour_diff",
        direction="increased"
    )

    decreased_hours = hour_list_from_comparison(
        compare_df=compare_to_august,
        diff_col="movement_per_driver_hour_diff",
        direction="decreased"
    )

    summary_rows.append({
        "Date": event_date,
        "Row": f"Event Day {i}",
        "Weekday": pd.Timestamp(event_date).day_name(),
        "Distinct Staff": event_day_staff_count,
        "Movements per Driver Hour": event_day_movements_per_driver_hour,
        "Average Shift Length": event_day_average_shift_length,
        "Hours with Increased Movements per Driver Hour vs August Avg": increased_hours,
        "Hours with Decreased Movements per Driver Hour vs August Avg": decreased_hours
    })


# =========================
# 5. ADD AUGUST ROW
# =========================

summary_rows.append({
    "Date": "August Average",
    "Row": "August",
    "Weekday": "All August days",
    "Movements per Driver Hour": august_movements_per_driver_hour,
    "Distinct Staff": august_avg_distinct_staff_per_day,
    "Average Shift Length": august_average_shift_length,
    "Hours with Increased Movements per Driver Hour vs August Avg": "",
    "Hours with Decreased Movements per Driver Hour vs August Avg": ""
})

summary_table = pd.DataFrame(summary_rows)

summary_table = summary_table[[
    "Row",
    "Date",
    "Weekday",
    "Distinct Staff",
    "Movements per Driver Hour",
    "Average Shift Length",
    "Hours with Increased Movements per Driver Hour vs August Avg",
    "Hours with Decreased Movements per Driver Hour vs August Avg"
]]

for col in [
    "Distinct Staff",
    "Movements per Driver Hour",
    "Average Shift Length"
]:
    summary_table[col] = summary_table[col].round(2)


# ============================================================
# DAY-OF-WEEK MATCHED COMPARISON
# ============================================================
#
# Compares:
#   Event Day 1 Friday    vs average August Friday
#   Event Day 2 Tuesday   vs average August Tuesday
#   Event Day 3 Wednesday vs average August Wednesday
#
# Still uses driver-level movements per active driver-hour.
# ============================================================

weekday_rows = []

baseline_adj_df["weekday"] = pd.to_datetime(
    baseline_adj_df["date_only"]
).dt.day_name()

for i, event_date in enumerate(event_dates_sorted, start=1):

    event_weekday = pd.Timestamp(event_date).day_name()

    # Event day driver-hour detail
    event_day_driver_hour_detail = event_driver_hour_detail[
        event_driver_hour_detail["date_only"] == event_date
    ].copy()

    event_day_mph = (
        event_day_driver_hour_detail["adjusted_movements"].mean()
    )

    # Same weekday baseline
    same_weekday_baseline_adj = baseline_adj_df[
        baseline_adj_df["weekday"] == event_weekday
    ].copy()

    same_weekday_driver_hour_detail = build_driver_hour_detail(
        same_weekday_baseline_adj
    )

    same_weekday_mph = (
        same_weekday_driver_hour_detail["adjusted_movements"].mean()
    )

    # Same weekday hourly average
    same_weekday_hourly_avg = same_weekday_driver_hour_detail.groupby([
        "hour",
        "hour_label"
    ]).agg(
        same_weekday_avg_movements_per_driver_hour=("adjusted_movements", "mean")
    ).reset_index()

    same_weekday_hourly_avg = all_hours.merge(
        same_weekday_hourly_avg,
        on=["hour", "hour_label"],
        how="left"
    )

    # Event day hourly average
    event_day_hourly_avg = event_day_driver_hour_detail.groupby([
        "hour",
        "hour_label"
    ]).agg(
        event_day_movements_per_driver_hour=("adjusted_movements", "mean")
    ).reset_index()

    event_day_hourly_avg = all_hours.merge(
        event_day_hourly_avg,
        on=["hour", "hour_label"],
        how="left"
    )

    event_day_hourly_avg["event_day_movements_per_driver_hour"] = (
        event_day_hourly_avg["event_day_movements_per_driver_hour"].fillna(0)
    )

    weekday_compare = event_day_hourly_avg.merge(
        same_weekday_hourly_avg,
        on=["hour", "hour_label"],
        how="left"
    )

    weekday_compare["movement_per_driver_hour_diff"] = (
        weekday_compare["event_day_movements_per_driver_hour"] -
        weekday_compare["same_weekday_avg_movements_per_driver_hour"]
    )

    increased_hours_same_weekday = hour_list_from_comparison(
        compare_df=weekday_compare,
        diff_col="movement_per_driver_hour_diff",
        direction="increased"
    )

    decreased_hours_same_weekday = hour_list_from_comparison(
        compare_df=weekday_compare,
        diff_col="movement_per_driver_hour_diff",
        direction="decreased"
    )

    # Shift lengths
    event_day_shift_length = shift_summary[
        (shift_summary["analysis_group"] == "event") &
        (shift_summary["shift_start"].dt.date == event_date)
    ]["shift_hours_observed"].mean()

    same_weekday_shift_length = shift_summary[
        (shift_summary["analysis_group"] == "baseline") &
        (shift_summary["shift_start"].dt.day_name() == event_weekday)
    ]["shift_hours_observed"].mean()

    weekday_rows.append({
        "Row": f"Event Day {i}",
        "Date": event_date,
        "Weekday": event_weekday,
        "Event Movements per Driver Hour": event_day_mph,
        "Same Weekday August Movements per Driver Hour": same_weekday_mph,
        "Difference": event_day_mph - same_weekday_mph,
        "% Difference": safe_pct_diff(
            event_day_mph,
            same_weekday_mph
        ),
        "Event Average Shift Length": event_day_shift_length,
        "Same Weekday August Average Shift Length": same_weekday_shift_length,
        "Hours with Increased Movements per Driver Hour vs Same Weekday Avg": increased_hours_same_weekday,
        "Hours with Decreased Movements per Driver Hour vs Same Weekday Avg": decreased_hours_same_weekday
    })

weekday_summary = pd.DataFrame(weekday_rows)

weekday_summary["% Difference Text"] = (
    weekday_summary["% Difference"].apply(format_pct)
)

weekday_summary[[
    "Event Movements per Driver Hour",
    "Same Weekday August Movements per Driver Hour",
    "Difference",
    "Event Average Shift Length",
    "Same Weekday August Average Shift Length"
]] = weekday_summary[[
    "Event Movements per Driver Hour",
    "Same Weekday August Movements per Driver Hour",
    "Difference",
    "Event Average Shift Length",
    "Same Weekday August Average Shift Length"
]].round(2)

weekday_summary = weekday_summary[[
    "Row",
    "Date",
    "Weekday",
    "Event Movements per Driver Hour",
    "Same Weekday August Movements per Driver Hour",
    "Difference",
    "% Difference Text",
    "Event Average Shift Length",
    "Same Weekday August Average Shift Length",
    "Hours with Increased Movements per Driver Hour vs Same Weekday Avg",
    "Hours with Decreased Movements per Driver Hour vs Same Weekday Avg"
]]

# ============================================================
# HOURLY MOVEMENTS PER DRIVER TABLE
# ============================================================
#
# Output format:
#   rows = Event Day 1, Event Day 2, Event Day 3, August
#   columns = 00:00 to 23:00
#
# Cell value:
#   average adjusted movements per active driver-hour
#
# Logic:
#   For each driver/date/hour, count adjusted movements.
#   Then average those driver-hour rows for the selected date/hour.
# ============================================================

hourly_wide_rows = []

event_dates_sorted = sorted(EVENT_DATES)

for i, event_date in enumerate(event_dates_sorted, start=1):

    event_day_driver_hour_detail = event_driver_hour_detail[
        event_driver_hour_detail["date_only"] == event_date
    ].copy()

    event_day_hourly = event_day_driver_hour_detail.groupby([
        "hour",
        "hour_label"
    ]).agg(
        movements_per_driver_hour=("adjusted_movements", "mean")
    ).reset_index()

    event_day_hourly = all_hours.merge(
        event_day_hourly,
        on=["hour", "hour_label"],
        how="left"
    )

    # If there were no movements for that hour, use 0
    event_day_hourly["movements_per_driver_hour"] = (
        event_day_hourly["movements_per_driver_hour"].fillna(0)
    )

    row = {
        "Row": f"Event Day {i}",
        "Date": event_date,
        "Weekday": pd.Timestamp(event_date).day_name()
    }

    for _, r in event_day_hourly.iterrows():
        row[r["hour_label"]] = r["movements_per_driver_hour"]

    hourly_wide_rows.append(row)


# August average row
august_hourly = baseline_driver_hour_detail.groupby([
    "hour",
    "hour_label"
]).agg(
    movements_per_driver_hour=("adjusted_movements", "mean")
).reset_index()

august_hourly = all_hours.merge(
    august_hourly,
    on=["hour", "hour_label"],
    how="left"
)

august_hourly["movements_per_driver_hour"] = (
    august_hourly["movements_per_driver_hour"].fillna(0)
)

august_row = {
    "Row": "August",
    "Date": "August Average",
    "Weekday": "All August days"
}

for _, r in august_hourly.iterrows():
    august_row[r["hour_label"]] = r["movements_per_driver_hour"]

hourly_wide_rows.append(august_row)

hourly_wide_table = pd.DataFrame(hourly_wide_rows)

hourly_wide_table = hourly_wide_table[
    ["Row", "Date", "Weekday"] + HOUR_COLUMNS
]

hourly_wide_table[HOUR_COLUMNS] = hourly_wide_table[HOUR_COLUMNS].round(2)


# ============================================================
# WRITE ONLY SIMPLE OUTPUT WORKBOOK
# ============================================================

with pd.ExcelWriter(
    OUTPUT_EXCEL,
    engine="openpyxl"
) as writer:

    summary_table.to_excel(
        writer,
        sheet_name="Summary",
        index=False
    )

    weekday_summary.to_excel(
        writer,
        sheet_name="Weekday Comparison",
        index=False
    )

    hourly_wide_table.to_excel(
        writer,
        sheet_name="Hourly Driver MPH",
        index=False
    )

    sheets_to_format = {
        "Summary": summary_table,
        "Weekday Comparison": weekday_summary,
        "Hourly Driver MPH": hourly_wide_table
    }

    for sheet_name, sheet_df in sheets_to_format.items():
        add_excel_formatting(writer, sheet_name, sheet_df)


# ============================================================
# FINAL PRINTS
# ============================================================

print("\n============================================================")
print("AUGUST EVENT DRIVER-LEVEL SUMMARY COMPLETE")
print("============================================================")
print(f"Excel workbook created: {OUTPUT_EXCEL}")
print(f"Full path: {os.path.abspath(OUTPUT_EXCEL)}")

print("\n=== SUMMARY TABLE ===")
print(summary_table)

print("\n=== WEEKDAY COMPARISON ===")
print(weekday_summary)

print("\n=== HOURLY DRIVER MPH TABLE ===")
print(hourly_wide_table)

print("============================================================")
