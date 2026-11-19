import os
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ============================================================
# BLOCK A-Y + MOUND ARRIVAL ANALYSIS
# ============================================================
#
# PURPOSE
# -------
# Analyse arrivals into:
#
#   Block A-Y
#   Mound*
#
# Excludes:
#
#   Block AA+
#   Returns*
#   Customer
#   Temp Lane
#   Red
#   Any other destinations
#
# TIME LOGIC
# ----------
#
# If Time contains:
#
#   09:55 - 10:02 (7 mins)
#
# use:
#
#   10:02
#
# If Time contains:
#
#   13:41
#
# use:
#
#   13:41
#
# This represents ARRIVAL time into destination.
#
# OUTPUTS
# -------
#
# Excel Workbook:
#
#   Summary
#   Monthly Profile
#   Weekday Profile
#   Hourly Profile
#   Top 20 Days
#   Weekday Hour Profile
#
# PNG Charts:
#
#   block_area_weekday_profile.png
#
# ============================================================

INPUT_FILE = (
    r"C:\Users\jamie_douglas\OneDrive - Edinburgh Airport Limited"
    r"\Documents\GitHub\EDI_airport_analytics\inputs"
    r"\movements_sep 25_to_26.csv"
)

OUTPUT_EXCEL = (
    r"C:\Users\jamie_douglas\OneDrive - Edinburgh Airport Limited"
    r"\Documents\GitHub\EDI_airport_analytics\outputs"
    r"\block_area_analysis.xlsx"
)

OUTPUT_FOLDER = os.path.dirname(OUTPUT_EXCEL)

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

WEEKDAY_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday"
]

# ============================================================
# HELPERS
# ============================================================

def extract_location(x):

    if pd.isna(x):
        return np.nan

    return str(x).split("/")[0].strip()


def normalise_time_string(x):

    if pd.isna(x):
        return np.nan

    x = str(x).strip()

    if ":" not in x:
        return np.nan

    parts = x.split(":")

    if len(parts) != 2:
        return np.nan

    try:
        return f"{int(parts[0]):02d}:{int(parts[1]):02d}"
    except:
        return np.nan


def extract_arrival_time(time_value):
    """
    Examples

    09:55 - 10:02 (7 mins)
        -> 10:02

    9:55 - 10:02
        -> 10:02

    13:41
        -> 13:41
    """

    if pd.isna(time_value):
        return np.nan

    text = str(time_value).strip()

    range_match = re.search(
        r"(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})",
        text
    )

    if range_match:
        return normalise_time_string(range_match.group(2))

    single_match = re.search(
        r"^(\d{1,2}:\d{2})$",
        text
    )

    if single_match:
        return normalise_time_string(single_match.group(1))

    return np.nan


def include_destination(location):

    if pd.isna(location):
        return False

    loc = str(location).strip()

    # Include any Mound location

    if loc.lower().startswith("mound"):
        return True

    # Match Block A-Y only

    match = re.fullmatch(
        r"block\s+([a-z]+)",
        loc,
        flags=re.IGNORECASE
    )

    if not match:
        return False

    block = match.group(1).upper()

    return len(block) == 1 and block <= "Y"


def add_excel_formatting(writer, sheet_name, df):

    worksheet = writer.sheets[sheet_name]

    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions

    for col_idx, col_name in enumerate(df.columns, start=1):

        max_length = max(
            len(str(col_name)),
            df[col_name].astype(str).map(len).max()
            if len(df) > 0
            else 0
        )

        adjusted_width = min(
            max(max_length + 2, 10),
            40
        )

        worksheet.column_dimensions[
            worksheet.cell(
                row=1,
                column=col_idx
            ).column_letter
        ].width = adjusted_width


# ============================================================
# LOAD
# ============================================================

print("Loading data...")

df = pd.read_csv(INPUT_FILE)

df.columns = df.columns.str.strip().str.lower()

# ============================================================
# CLEAN LOCATIONS
# ============================================================

df["from_clean"] = df["from"].apply(extract_location)
df["to_clean"] = df["to"].apply(extract_location)

# ============================================================
# TIME PARSING
# ============================================================

df["arrival_time"] = df["time"].apply(
    extract_arrival_time
)

df["arrival_dt"] = pd.to_datetime(
    df["date"].astype(str)
    + " "
    + df["arrival_time"],
    dayfirst=True,
    errors="coerce"
)

df = df[
    df["arrival_dt"].notna()
].copy()

# ============================================================
# FILTER TARGET AREA
# ============================================================

df = df[
    df["to_clean"].apply(include_destination)
].copy()

print(
    f"Rows after target area filter: {len(df):,}"
)

# ============================================================
# DATE FEATURES
# ============================================================

df["month"] = (
    df["arrival_dt"]
    .dt.to_period("M")
    .astype(str)
)

df["date_only"] = df["arrival_dt"].dt.date

df["hour"] = df["arrival_dt"].dt.hour

df["hour_label"] = df["hour"].apply(
    lambda x: f"{int(x):02d}:00"
)

df["weekday"] = (
    df["arrival_dt"]
    .dt.day_name()
)

df["weekday"] = pd.Categorical(
    df["weekday"],
    categories=WEEKDAY_ORDER,
    ordered=True
)

# ============================================================
# MONTHLY PROFILE
# ============================================================

monthly_profile = (
    df.groupby("month")
    .agg(
        entries=("month", "size"),
        days=("date_only", "nunique")
    )
    .reset_index()
)

monthly_profile["avg_entries_per_day"] = (
    monthly_profile["entries"]
    / monthly_profile["days"]
)

monthly_profile["avg_entries_per_day"] = (
    monthly_profile["avg_entries_per_day"]
    .round(2)
)

monthly_profile = monthly_profile.sort_values("month")

# ============================================================
# DAILY PROFILE
# ============================================================

daily_profile = (
    df.groupby("date_only")
    .size()
    .reset_index(name="entries")
)

daily_profile["weekday"] = pd.to_datetime(
    daily_profile["date_only"]
).dt.day_name()

# ============================================================
# TOP 20 DAYS
# ============================================================

top_20_days = (
    daily_profile
    .sort_values(
        "entries",
        ascending=False
    )
    .head(20)
    .copy()
    .reset_index(drop=True)
)

top_20_days.insert(
    0,
    "Rank",
    range(1, len(top_20_days) + 1)
)

# ============================================================
# WEEKDAY PROFILE
# ============================================================

weekday_profile = (
    daily_profile
    .groupby("weekday")
    .agg(
        total_entries=("entries", "sum"),
        avg_entries_per_day=("entries", "mean")
    )
    .reset_index()
)

weekday_profile = weekday_profile.sort_values(
    "avg_entries_per_day",
    ascending=False
)

# ============================================================
# HOURLY PROFILE
# ============================================================

hourly_daily = (
    df.groupby(
        ["date_only", "hour"]
    )
    .size()
    .reset_index(name="entries")
)

hourly_profile = (
    hourly_daily
    .groupby("hour")
    .agg(
        avg_entries=("entries", "mean")
    )
    .reset_index()
)

all_hours = pd.DataFrame({
    "hour": range(24)
})

hourly_profile = all_hours.merge(
    hourly_profile,
    on="hour",
    how="left"
)

hourly_profile["avg_entries"] = (
    hourly_profile["avg_entries"]
    .fillna(0)
)

# ============================================================
# WEEKDAY-HOUR PROFILE
# ============================================================

weekday_hour_base = (
    df.groupby(
        ["weekday", "date_only", "hour"],
        observed=True
    )
    .size()
    .reset_index(name="entries")
)

weekday_hour_profile = (
    weekday_hour_base
    .groupby(
        ["weekday", "hour"],
        observed=True
    )
    .agg(
        avg_entries=("entries", "mean")
    )
    .reset_index()
)

weekday_hour_profile = (
    weekday_hour_profile
    .pivot(
        index="hour",
        columns="weekday",
        values="avg_entries"
    )
    .reset_index()
)

weekday_hour_profile = (
    weekday_hour_profile[
        ["hour"] + WEEKDAY_ORDER
    ]
)

weekday_hour_profile = weekday_hour_profile.round(2)

weekday_hour_profile.rename(
    columns={"hour": "Hour"},
    inplace=True
)

weekday_hour_profile["Hour"] = (
    weekday_hour_profile["Hour"]
    .apply(lambda x: f"{int(x):02d}:00")
)

# ============================================================
# PEAKS
# ============================================================

peak_month = monthly_profile.loc[
    monthly_profile["entries"].idxmax()
]

peak_day = daily_profile.loc[
    daily_profile["entries"].idxmax()
]

peak_weekday = weekday_profile.loc[
    weekday_profile["avg_entries_per_day"].idxmax()
]

hourly_summary = (
    df.groupby(["date_only", "hour"])
    .size()
    .reset_index(name="entries")
)

hourly_average = (
    hourly_summary
    .groupby("hour")
    .agg(
        avg_entries=("entries", "mean")
    )
    .reset_index()
)

peak_hour = hourly_average.loc[
    hourly_average["avg_entries"].idxmax()
]


summary = pd.DataFrame({
    "Metric": [
        "Total Entries",
        "Peak Month",
        "Peak Month Entries",
        "Peak Weekday",
        "Peak Weekday Avg Entries",
        "Peak Day",
        "Peak Day Entries",
        "Peak Hour",
        "Peak Hour Entries"
    ],
    "Value": [
        len(df),
        peak_month["month"],
        peak_month["entries"],
        peak_weekday["weekday"],
        round(
            peak_weekday["avg_entries_per_day"],
            2
        ),
        str(peak_day["date_only"]),
        peak_day["entries"],
        f"{int(peak_hour['hour']):02d}:00",
        round(peak_hour["avg_entries"], 2)
    ]
})


# ============================================================
# PLOT 1 - WEEKDAY PROFILE
# ============================================================

plt.figure(figsize=(12, 6))

for weekday in WEEKDAY_ORDER:

    plt.plot(
        range(24),
        weekday_hour_profile[weekday],
        marker="o",
        label=weekday
    )

plt.xticks(
    range(24),
    [f"{h:02d}:00" for h in range(24)],
    rotation=45
)

plt.xlabel("Hour of Day")
plt.ylabel("Average Entries")

plt.title(
    "Average Arrivals by Hour and Weekday\n"
    "Block A-Y and Mound Area"
)

plt.legend()

plt.grid(alpha=0.3)

plt.tight_layout()

plt.savefig(
    os.path.join(
        OUTPUT_FOLDER,
        "block_area_weekday_profile.png"
    ),
    dpi=300
)

plt.close()

# ============================================================
# EXCEL OUTPUT
# ============================================================

with pd.ExcelWriter(
    OUTPUT_EXCEL,
    engine="openpyxl"
) as writer:

    summary.to_excel(
        writer,
        sheet_name="Summary",
        index=False
    )

    monthly_profile.to_excel(
        writer,
        sheet_name="Monthly Profile",
        index=False
    )

    weekday_profile.to_excel(
        writer,
        sheet_name="Weekday Profile",
        index=False
    )

    hourly_profile.to_excel(
        writer,
        sheet_name="Hourly Profile",
        index=False
    )

    top_20_days.to_excel(
        writer,
        sheet_name="Top 20 Days",
        index=False
    )


    weekday_hour_profile.to_excel(
        writer,
        sheet_name="Weekday Hour Profile",
        index=False
    )

    sheets = {
        "Summary": summary,
        "Monthly Profile": monthly_profile,
        "Weekday Profile": weekday_profile,
        "Hourly Profile": hourly_profile,
        "Top 20 Days": top_20_days,
        "Weekday Hour Profile": weekday_hour_profile
    }

    for sheet_name, sheet_df in sheets.items():
        add_excel_formatting(
            writer,
            sheet_name,
            sheet_df
        )

# ============================================================
# FINAL OUTPUT
# ============================================================

print("\n=================================================")
print("BLOCK AREA ANALYSIS COMPLETE")
print("=================================================")

print(f"Rows analysed: {len(df):,}")

print(
    f"Peak Month: {peak_month['month']} "
    f"({peak_month['entries']:,} entries | "    
    f"{peak_month['avg_entries_per_day']:.1f} avg/day)"
)

print(
    f"Peak Weekday: {peak_weekday['weekday']} "
    f"({peak_weekday['avg_entries_per_day']:.1f} avg/day)"
)

print(
    f"Peak Day: {peak_day['date_only']} "
    f"({peak_day['entries']:,} entries)"
)

print(
    f"Peak Hour: {int(peak_hour['hour']):02d}:00 "
    f"({peak_hour['avg_entries']:.1f} avg/day)"
)

print(f"\nWorkbook: {OUTPUT_EXCEL}")
print(f"Charts saved to: {OUTPUT_FOLDER}")
print("=================================================")