
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# =========================
# LOAD DATA
# =========================
df = pd.read_csv("inputs/movements.csv")

# Standardise column names
df.columns = df.columns.str.strip().str.lower()

# =========================
# 1. CLEAN FROM / TO
# =========================
def extract_location(x):
    if pd.isna(x):
        return x
    return str(x).split("/")[0].strip()

df["from_clean"] = df["from"].apply(extract_location)
df["to_clean"] = df["to"].apply(extract_location)

# Flag same location movements
df["same_location"] = df["from_clean"] == df["to_clean"]

# =========================
# 2. PARSE TIME + COMBINE WITH DATE
# =========================

# Extract start and end times from range strings like "23:55 - 23:57 (2 mins)"
times = df["time"].astype(str).str.extract(r'(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})')

df["start_time"] = times[0]
df["end_time"] = times[1]

# Handle single-time rows like "13:41" -> start=end
single_times = df["time"].astype(str).str.extract(r'^(\d{2}:\d{2})$')[0]
df["start_time"] = df["start_time"].fillna(single_times)
df["end_time"] = df["end_time"].fillna(single_times)

# Combine Date + Time -> proper datetime
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

# Fix overnight movements (cross midnight)
df.loc[df["end_dt"] < df["start_dt"], "end_dt"] += pd.Timedelta(days=1)

# Duration in hours
df["duration_hours"] = (
    (df["end_dt"] - df["start_dt"]).dt.total_seconds() / 3600
)

# =========================
# 3. CREATE SHIFTS (GAP > 6 HOURS)
# =========================
df = df.sort_values(["driver", "start_dt"])

df["prev_end"] = df.groupby("driver")["end_dt"].shift(1)

# Gap in hours
df["gap_hours"] = (
    (df["start_dt"] - df["prev_end"]).dt.total_seconds() / 3600
)

# New shift if gap > 6 OR first row
df["new_shift"] = (df["gap_hours"] > 6) | df["gap_hours"].isna()

# Assign shift ID
df["shift_id"] = df.groupby("driver")["new_shift"].cumsum()

# =========================
# 4. SHIFT-LEVEL METRICS
# =========================
shift_summary = df.groupby(["driver", "shift_id"]).agg(
    shift_start=("start_dt", "min"),
    shift_end=("end_dt", "max"),
    movements=("time", "count")
).reset_index()

# Compute shift duration
shift_summary["shift_hours"] = (
    (shift_summary["shift_end"] - shift_summary["shift_start"])
    .dt.total_seconds() / 3600
).replace(0, np.nan)

# Movements per hour
shift_summary["movements_per_hour"] = (
    shift_summary["movements"] / shift_summary["shift_hours"]
)

# Replace inf if any edge cases slipped through
shift_summary["movements_per_hour"] = shift_summary["movements_per_hour"].replace([np.inf, -np.inf], np.nan)

# =========================
# 5. ADJUSTED VERSION
# (REMOVE ZERO TIME + SAME LOCATION)
# =========================
df_adj = df[
    (df["duration_hours"] > 0) &
    (~df["same_location"])
].copy()

adj_summary = df_adj.groupby(["driver", "shift_id"]).agg(
    adj_movements=("time", "count")
).reset_index()

# Merge back
final = shift_summary.merge(
    adj_summary,
    on=["driver", "shift_id"],
    how="left"
)

final["adj_movements"] = final["adj_movements"].fillna(0)

# Adjusted movements per hour
final["adjusted_movements_per_hour"] = (
    final["adj_movements"] / final["shift_hours"]
)

final["adjusted_movements_per_hour"] = final["adjusted_movements_per_hour"].replace([np.inf, -np.inf], np.nan)

# =========================
# 6. FINAL OUTPUT FORMAT
# =========================
output = final[[
    "driver",
    "shift_start",
    "shift_end",
    "movements_per_hour",
    "adjusted_movements_per_hour"
]].copy()

# Round for readability
output["movements_per_hour"] = output["movements_per_hour"].round(2)
output["adjusted_movements_per_hour"] = output["adjusted_movements_per_hour"].round(2)

# =========================
# SAVE
# =========================
output.to_csv("driver_shift_productivity.csv", index=False)

print(output.head())

# =========================
# 7. OVERALL SUMMARY METRICS
# =========================

summary_metrics = pd.DataFrame({
    "metric": [
        "average_shift_length_hours",
        "median_shift_length_hours",
        "average_movements_per_hour",
        "median_movements_per_hour",
        "average_adjusted_movements_per_hour",
        "median_adjusted_movements_per_hour",
        "average_difference_mph_vs_adjusted",
        "median_difference_mph_vs_adjusted"
    ],
    "value": [
        final["shift_hours"].mean(),
        final["shift_hours"].median(),
        final["movements_per_hour"].mean(),
        final["movements_per_hour"].median(),
        final["adjusted_movements_per_hour"].mean(),
        final["adjusted_movements_per_hour"].median(),
        (final["movements_per_hour"] - final["adjusted_movements_per_hour"]).mean(),
        (final["movements_per_hour"] - final["adjusted_movements_per_hour"]).median()
    ]
})

# Round for readability
summary_metrics["value"] = summary_metrics["value"].round(2)

print("\n=== SUMMARY METRICS ===")
print(summary_metrics)

print("\n=== DETAILED METRICS DESCRIPTION ===")
print(final[[
    "movements_per_hour",
    "adjusted_movements_per_hour"
]].describe())

# Optional: save separately
summary_metrics.to_csv("summary_metrics.csv", index=False)

# =========================
# 8. HOURLY FEATURES (DATE AND WEEKDAY)
# =========================

# Hour / Weekday / Date breakdown
df["hour"] = df["start_dt"].dt.hour
df["weekday"] = df["start_dt"].dt.day_name()
df["date_only"] = df["start_dt"].dt.date

# Optional: consistent weekday order
weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
df["weekday"] = pd.Categorical(df["weekday"], categories=weekday_order, ordered=True)

# Adjusted hourly base
df_adj_hour = df[
    (df["duration_hours"] > 0) &
    (~df["same_location"])
].copy()



# =========================
# 9. HOURLY PRODUCTIVITY
# NEW DEFINITION:
# movements in hour / distinct drivers with a row in that hour
# =========================


# 9A. BY DATE AND HOUR
hourly_date = df.groupby(["date_only", "hour"]).agg(
    movements=("time", "count"),
    distinct_drivers=("driver", "nunique")
).reset_index()

hourly_date["movements_per_driver_hour"] = (
    hourly_date["movements"] / hourly_date["distinct_drivers"]
)

# Adjusted version
hourly_adj_date = df_adj_hour.groupby(["date_only", "hour"]).agg(
    adj_movements=("time", "count"),
    adj_distinct_drivers=("driver", "nunique")
).reset_index()

hourly_date = hourly_date.merge(
    hourly_adj_date,
    on=["date_only", "hour"],
    how="left"
)

hourly_date["adj_movements"] = hourly_date["adj_movements"].fillna(0)
hourly_date["adj_distinct_drivers"] = hourly_date["adj_distinct_drivers"].fillna(0)

hourly_date["adjusted_movements_per_driver_hour"] = np.where(
    hourly_date["adj_distinct_drivers"] > 0,
    hourly_date["adj_movements"] / hourly_date["adj_distinct_drivers"],
    np.nan
)

# Add weekday AFTER merge
hourly_date["weekday"] = pd.to_datetime(hourly_date["date_only"]).dt.day_name()
hourly_date["weekday"] = pd.Categorical(
    hourly_date["weekday"],
    categories=weekday_order,
    ordered=True
)

hourly_date = hourly_date[[
    "date_only",
    "weekday",
    "hour",
    "movements",
    "distinct_drivers",
    "movements_per_driver_hour",
    "adj_movements",
    "adj_distinct_drivers",
    "adjusted_movements_per_driver_hour"
]].round(2)

hourly_date = hourly_date.sort_values(["date_only", "hour"])

print("\n=== HOURLY BY DATE ===")
print(hourly_date.head())

hourly_date.to_csv("hourly_by_date.csv", index=False)


# 9B. BY WEEKDAY AND HOUR
hourly_weekday = hourly_date.groupby(["weekday", "hour"], observed=True).agg(
    average_movements_per_driver_hour=("movements_per_driver_hour", "mean"),
    median_movements_per_driver_hour=("movements_per_driver_hour", "median"),
    average_adjusted_movements_per_driver_hour=("adjusted_movements_per_driver_hour", "mean"),
    median_adjusted_movements_per_driver_hour=("adjusted_movements_per_driver_hour", "median")
).reset_index()

hourly_weekday = hourly_weekday.round(2)
hourly_weekday = hourly_weekday.sort_values(["weekday", "hour"])

print("\n=== HOURLY BY WEEKDAY ===")
print(hourly_weekday.head())

hourly_weekday.to_csv("hourly_by_weekday.csv", index=False)


# =========================
# 10. PLOT (LINE PER WEEKDAY) - SYSTEM LEVEL
# using the new driver-normalised metric
# =========================

plt.figure(figsize=(12, 6))

for wd in hourly_weekday["weekday"].dropna().unique():
    subset = hourly_weekday[hourly_weekday["weekday"] == wd]
    plt.plot(
        subset["hour"],
        subset["average_adjusted_movements_per_driver_hour"],
        label=wd
    )

plt.xlabel("Hour of Day")
plt.ylabel("Average Adjusted Movements per Driver")
plt.title("Average Adjusted Movements per Driver by Hour of Day and Weekday")
plt.legend()
plt.grid(True)
plt.show()

# =========================
# 11. TRAVEL TIME (FROM -> TO)
# remove same-location jobs here as requested
# =========================

travel_df = df[
    (df["duration_hours"] > 0) &
    (~df["same_location"])
].copy()

# Convert to minutes
travel_df["duration_minutes"] = travel_df["duration_hours"] * 60

# Optional clean (prevents extreme distortion)
travel_df = travel_df[travel_df["duration_minutes"] < 60]

travel_summary = travel_df.groupby(["from_clean", "to_clean"], observed=False).agg(
    avg_travel_time_minutes=("duration_minutes", "mean"),
    median_travel_time_minutes=("duration_minutes", "median"),
    movement_count=("duration_minutes", "count")
).reset_index()

travel_summary[[
    "avg_travel_time_minutes",
    "median_travel_time_minutes"
]] = travel_summary[[
    "avg_travel_time_minutes",
    "median_travel_time_minutes"
]].round(2)

print("\n=== TRAVEL TIME SUMMARY ===")
print(travel_summary.head())

travel_summary.to_csv("travel_time_summary.csv", index=False)


# =========================
# 12. DRIVER-LEVEL HOURLY DETAIL
# actual jobs per driver/date/hour only
# =========================

driver_hour_detail = df.groupby(["driver", "date_only", "weekday", "hour"], observed=True).agg(
    movements=("time", "count")
).reset_index()

driver_hour_adj = df_adj_hour.groupby(["driver", "date_only", "weekday", "hour"], observed=True).agg(
    adj_movements=("time", "count")
).reset_index()

driver_hour_detail = driver_hour_detail.merge(
    driver_hour_adj,
    on=["driver", "date_only", "weekday", "hour"],
    how="left"
)

driver_hour_detail["adj_movements"] = driver_hour_detail["adj_movements"].fillna(0)

print("\n=== DRIVER HOURLY DETAIL ===")
print(driver_hour_detail.head())

driver_hour_detail.to_csv("driver_hourly_detail.csv", index=False)


print("\n=== DRIVER HOUR DETAIL CHECK ===")
print(driver_hour_detail["movements"].describe())
print(driver_hour_detail["adj_movements"].describe())


# =========================
# 13. DRIVER-LEVEL WEEKDAY SUMMARY
# average / median jobs per active driver-hour
# =========================

driver_hour_avg = driver_hour_detail.groupby(["weekday", "hour"], observed=True).agg(
    avg_driver_movements=("movements", "mean"),
    median_driver_movements=("movements", "median"),
    avg_driver_adjusted_movements=("adj_movements", "mean"),
    median_driver_adjusted_movements=("adj_movements", "median")
).reset_index()

driver_hour_avg = driver_hour_avg.round(2)
driver_hour_avg = driver_hour_avg.sort_values(["weekday", "hour"])

print("\n=== DRIVER-AVERAGED HOURLY PRODUCTIVITY ===")
print(driver_hour_avg.head())

driver_hour_avg.to_csv("driver_hourly_average.csv", index=False)

comparison = hourly_weekday.merge(
    driver_hour_avg,
    on=["weekday", "hour"],
    how="left"
)

print("\n=== SYSTEM vs DRIVER COMPARISON ===")
print(comparison.head())

comparison.to_csv("hourly_comparison_system_vs_driver.csv", index=False)


# =========================
# 14. DRIVER-LEVEL PLOTS (WEEKDAY LINES)
# using count-based driver metrics (not duration-normalised)
# =========================

# Raw driver movements
plt.figure(figsize=(12, 6))

for wd in driver_hour_avg["weekday"].dropna().unique():
    subset = driver_hour_avg[driver_hour_avg["weekday"] == wd]
    plt.plot(
        subset["hour"],
        subset["avg_driver_movements"],
        label=wd
    )

plt.xlabel("Hour of Day")
plt.ylabel("Average Driver Movements")
plt.title("Average Driver Movements by Hour of Day and Weekday")
plt.legend()
plt.grid(True)
plt.show()

# Adjusted driver movements
plt.figure(figsize=(12, 6))

for wd in driver_hour_avg["weekday"].dropna().unique():
    subset = driver_hour_avg[driver_hour_avg["weekday"] == wd]
    plt.plot(
        subset["hour"],
        subset["avg_driver_adjusted_movements"],
        label=wd
    )

plt.xlabel("Hour of Day")
plt.ylabel("Average Driver Adjusted Movements")
plt.title("Average Driver Adjusted Movements by Hour of Day and Weekday")
plt.legend()
plt.grid(True)
plt.show()
