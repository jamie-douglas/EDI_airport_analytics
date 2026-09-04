import numpy as np
import pandas as pd
import glob
import pathlib
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

photobooth_path = SCRIPT_DIR / "Photobooth Inputs"
csv_path = r"C:\Users\jamie_douglas\Edinburgh Airport Limited\Shared Files - Business Planning\Seasonal Readiness\W26\2. Car Parking\Modelling\transaction_forecast.csv"

from modules.utils.db import get_engine

# =========================================================
# 1. LOAD AND AGGREGATE PHOTOBOOTH SCAN DATA
# =========================================================

photobooth_files = list(photobooth_path.glob("*.csv"))

photobooth_list = []
for file in photobooth_files:
    df = pd.read_csv(file)
    df["Scan Date"] = pd.to_datetime(
        df["Scan Date"],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce"
    )
    photobooth_list.append(df)

raw_scans = pd.concat(photobooth_list, ignore_index=True)
raw_scans = raw_scans.drop_duplicates()

# Collapse multiple scans into Customer Arrival (MIN) and Staff Return (MAX)
photobooth_summary = raw_scans.groupby("Booking Ref").agg(
    customer_arrival_photobooth=("Scan Date", "min"),
    staff_return_photobooth=("Scan Date", "max"),
    total_scans_count=("Scan Date", "count")
).reset_index()

# Calculate journey gap
photobooth_summary["scan_gap_hours"] = (
    (photobooth_summary["staff_return_photobooth"] - photobooth_summary["customer_arrival_photobooth"])
    .dt.total_seconds() / 3600.0
)

print(f"Bookings before journey validation: {len(photobooth_summary)}")
photobooth_summary = photobooth_summary[photobooth_summary["scan_gap_hours"] >= 6]
print(f"Bookings after journey validation: {len(photobooth_summary)}")


# =========================================================
# 2. LOAD AND COMBINE BOOKING / ACTUALS DATA
# =========================================================

dsn = 'AzureConnection'
user = 'jamie_douglas'
engine = get_engine(dsn=dsn, username=user)

sql_query = """
SELECT
    "BookingReference",
    "CheckInStarted",
    "CheckInEnded",
    "ExpectedArrivalDate",
    "ExpectedReturnDate",
    "ActualCheckedOutDate"
FROM FastPark.v_EntryAndExits
WHERE 
    (
        (ExpectedArrivalDate BETWEEN '2025-12-14' AND '2026-01-14')
        OR (ExpectedArrivalDate BETWEEN '2026-04-27' AND '2026-05-27')
        OR (ExpectedArrivalDate BETWEEN '2026-08-01' AND '2026-08-27')
    )
    AND "ActualCheckedOutDate" IS NOT NULL
"""

actuals_df = pd.read_sql(sql_query, con=engine)
actuals_df["CheckInStarted"] = pd.to_datetime(actuals_df["CheckInStarted"])
actuals_df["CheckInEnded"] = pd.to_datetime(actuals_df["CheckInEnded"])
actuals_df["ActualCheckedOutDate"] = pd.to_datetime(actuals_df["ActualCheckedOutDate"])


# =========================================================
# 3. MERGE BOOKING AND ACTUALS DATA WITH PHOTOBOOTH DATA
# =========================================================

master_df = pd.merge(
    actuals_df,
    photobooth_summary,
    left_on="BookingReference",
    right_on="Booking Ref",
).drop(columns=["Booking Ref", "Vehicle", "Customer", "Reg/Ref", "Origin"], errors="ignore")


# =========================================================
# 4. CALCULATE BASELINE METRICS & LOCKER OCCUPANCY (1-MIN GRANULARITY)
# =========================================================

# Kiosk Transaction duration (seconds) with sanity filter (> 0)
master_df["kiosk_duration_seconds"] = (master_df["CheckInEnded"] - master_df["CheckInStarted"]).dt.total_seconds()
valid_kiosk_durations = master_df[master_df["kiosk_duration_seconds"] > 0]["kiosk_duration_seconds"]

kiosk_metrics = {
    "mean_sec": valid_kiosk_durations.mean(),
    "median_sec": valid_kiosk_durations.median(),
    "p50_sec": valid_kiosk_durations.quantile(0.5),
    "p90_sec": valid_kiosk_durations.quantile(0.9),
    "p95_sec": valid_kiosk_durations.quantile(0.95),
}

# Theoretical Throughput: 5 kiosks * 60 sec / mean_sec per minute
kiosk_capacity_per_min = (5 * 60) / kiosk_metrics["mean_sec"]
max_kiosk_hourly_throughput = kiosk_capacity_per_min * 60

# Ferry Dwell Times (Kiosk Start vs Photobooth Scan)
master_df["ferry_to_kiosk_dwell_mins"] = (master_df["CheckInStarted"] - master_df["customer_arrival_photobooth"]).dt.total_seconds() / 60.0

print("\nBefore filtering:")
print(master_df["ferry_to_kiosk_dwell_mins"].describe())
len_before = len(master_df)

master_df = master_df[master_df["ferry_to_kiosk_dwell_mins"].between(0, 60)]

print("\nAfter filtering:")
print(master_df["ferry_to_kiosk_dwell_mins"].describe())
print(f"Rows removed: {len_before - len(master_df):,}")

dwell_metrics = {
    "mean_mins": master_df["ferry_to_kiosk_dwell_mins"].mean(),
    "median_mins": master_df["ferry_to_kiosk_dwell_mins"].median(),
    "p50_mins": master_df["ferry_to_kiosk_dwell_mins"].quantile(0.5),
    "p90_mins": master_df["ferry_to_kiosk_dwell_mins"].quantile(0.9),
    "p95_mins": master_df["ferry_to_kiosk_dwell_mins"].quantile(0.95),
}

print(f"\n--- KIOSK SERVICE BASELINE ---")
print(f"Average check-in time: {kiosk_metrics['mean_sec']:.1f}s")
print(f"90th Percentile check-in time: {kiosk_metrics['p90_sec']:.1f}s")
print(f"Max theoretical kiosk throughput (5 kiosks): {max_kiosk_hourly_throughput:.1f} cars/hr")

print(f"\n--- FERRY DWELL TIME BASELINE ---")
print(f"Average ferry dwell time: {dwell_metrics['mean_mins']:.1f} mins")

# Locker metrics (1-Minute Granularity)
master_df["best_case_key_deposit"] = master_df["staff_return_photobooth"] + pd.Timedelta(minutes=15)
master_df["best_case_locker_dwell_hours"] = (master_df["ActualCheckedOutDate"] - master_df["best_case_key_deposit"]).dt.total_seconds() / 3600.0

def run_locker_occupancy_analysis(df):
    start_time = df['best_case_key_deposit'].min().floor('1min')
    end_time = df['ActualCheckedOutDate'].max().ceil('1min')
    time_grid = pd.date_range(start=start_time, end=end_time, freq='1min')
    
    starts = np.sort(df['best_case_key_deposit'].values)
    ends = np.sort(df['ActualCheckedOutDate'].values)
    grid_vals = time_grid.values
    
    occupied = np.searchsorted(starts, grid_vals, side='right') - np.searchsorted(ends, grid_vals, side='right')

    return pd.DataFrame({
        'timestamp': time_grid,
        'occupied_lockers': occupied,
        'available_lockers': 297 - occupied,
        'is_over_capacity': occupied > 297
    })

locker_df = run_locker_occupancy_analysis(master_df)

print(f"\n--- KEY LOCKER CAPACITY METRICS ---")
print(f"Peak Lockers Occupied (Best Case): {locker_df['occupied_lockers'].max()} / 297")
print(f"Min Available Locker Safety Margin: {locker_df['available_lockers'].min()}")
print(f"Total hours overflowing 297 lockers: {(locker_df['is_over_capacity'].sum() * 1)/60:.1f} hours")
print(f"Median Locker Dwell: {master_df['best_case_locker_dwell_hours'].median():.1f} hrs")
print(f"P90 Locker Dwell: {master_df['best_case_locker_dwell_hours'].quantile(.90):.1f} hrs")


# =========================================================
# 5. ARRIVAL TIMESTAMP FOUNDATION
# =========================================================

master_df["true_arrival_time"] = master_df["customer_arrival_photobooth"]

print("\n--- ARRIVAL FOUNDATION ---")
print(f"Arrival records available: {len(master_df):,}")


# =========================================================
# 6. HISTORICAL PROFILES (WEEK-OF-MONTH + 1-MIN GRANULARITY)
# =========================================================

def load_full_year_profile_data(engine):
    print("\n--- Pulling Full Year Historical Data ---")
    sql = """
    SELECT
        "BookingReference",
        "CheckInStarted",
        "ActualCheckedOutDate"
    FROM FastPark.v_EntryAndExits
    WHERE
        "CheckInStarted" IS NOT NULL
        AND "CheckInStarted" >= DATEADD(year,-1,GETDATE())
    """
    df = pd.read_sql(sql, con=engine)
    df["CheckInStarted"] = pd.to_datetime(df["CheckInStarted"])
    df["ActualCheckedOutDate"] = pd.to_datetime(df["ActualCheckedOutDate"], errors="coerce")
    return df

def build_historical_arrival_profile(df):
    working = df.dropna(subset=["CheckInStarted"]).copy()
    working["week_of_month"] = (working["CheckInStarted"].dt.day - 1) // 7 + 1
    working["weekday"] = working["CheckInStarted"].dt.dayofweek
    working["hour"] = working["CheckInStarted"].dt.hour
    working["minute"] = working["CheckInStarted"].dt.minute

    profile = working.groupby(["week_of_month", "weekday", "hour", "minute"]).size().reset_index(name="count")
    group_totals = profile.groupby(["week_of_month", "weekday"])["count"].transform("sum")
    profile["show_up_probability"] = profile["count"] / group_totals
    return profile

def build_historical_exit_profile(df):
    working = df.dropna(subset=["ActualCheckedOutDate"]).copy()
    working["week_of_month"] = (working["ActualCheckedOutDate"].dt.day - 1) // 7 + 1
    working["weekday"] = working["ActualCheckedOutDate"].dt.dayofweek
    working["hour"] = working["ActualCheckedOutDate"].dt.hour
    working["minute"] = working["ActualCheckedOutDate"].dt.minute

    profile = working.groupby(["week_of_month", "weekday", "hour", "minute"]).size().reset_index(name="count")
    group_totals = profile.groupby(["week_of_month", "weekday"])["count"].transform("sum")
    profile["show_up_probability"] = profile["count"] / group_totals
    return profile


# =========================================================
# 7. FUTURE FORECAST DISAGGREGATION (1-MINUTE & WEEK-OF-MONTH)
# =========================================================

def load_monthly_forecast(csv_path):
    monthly_df = pd.read_csv(csv_path)
    monthly_df["Month"] = pd.to_datetime(monthly_df["Month"], format="%y-%b")
    return monthly_df

def disaggregate_monthly_to_timestamps(monthly_df, arrival_profile, exit_profile):
    arrival_records = []
    exit_records = []

    for _, row in monthly_df.iterrows():
        month_start = row["Month"].replace(day=1)
        month_end = month_start + pd.offsets.MonthEnd(0)
        days = pd.date_range(month_start, month_end, freq="D")

        arrivals_per_day = row["Transactions"] / len(days)
        exits_per_day = row["Transactions"] / len(days)

        for day in days:
            wom = (day.day - 1) // 7 + 1
            weekday = day.dayofweek

            # Arrival profile lookup with fallback if specific week-of-month is missing
            a_pat = arrival_profile[(arrival_profile["week_of_month"] == wom) & (arrival_profile["weekday"] == weekday)]
            if len(a_pat) == 0:
                a_pat = arrival_profile[arrival_profile["weekday"] == weekday]

            if len(a_pat) > 0:
                weights = a_pat["show_up_probability"] / a_pat["show_up_probability"].sum()
                counts = np.floor(arrivals_per_day * weights).astype(int)
                for (_, p), cnt in zip(a_pat.iterrows(), counts):
                    arrival_records.extend([
                        day + pd.Timedelta(hours=int(p["hour"]), minutes=int(p["minute"]))
                    ] * int(cnt))

            # Exit profile lookup with fallback
            e_pat = exit_profile[(exit_profile["week_of_month"] == wom) & (exit_profile["weekday"] == weekday)]
            if len(e_pat) == 0:
                e_pat = exit_profile[exit_profile["weekday"] == weekday]

            if len(e_pat) > 0:
                weights = e_pat["show_up_probability"] / e_pat["show_up_probability"].sum()
                counts = np.floor(exits_per_day * weights).astype(int)
                for (_, p), cnt in zip(e_pat.iterrows(), counts):
                    exit_records.extend([
                        day + pd.Timedelta(hours=int(p["hour"]), minutes=int(p["minute"]))
                    ] * int(cnt))

    return pd.DataFrame({"arrival_time": arrival_records}), pd.DataFrame({"exit_time": exit_records})

def build_future_forecast(engine, arrival_profile, exit_profile):
    sql = """
    SELECT
        "IntervalStartDateTimeLocal",
        "Entries",
        "Exits"
    FROM FastPark.v_ForecastEntryandExits
    WHERE "IntervalStartDateTimeLocal" >= GETDATE()
    """
    forecast_df = pd.read_sql(sql, con=engine)
    forecast_df["IntervalStartDateTimeLocal"] = pd.to_datetime(forecast_df["IntervalStartDateTimeLocal"])

    arrival_records = []
    exit_records = []

    for _, row in forecast_df.iterrows():
        ts = row["IntervalStartDateTimeLocal"]
        ts_hour_start = ts.floor("h")
        wom = (ts.day - 1) // 7 + 1
        weekday = ts.dayofweek
        hour = ts.hour

        # ARRIVALS
        a_profile = arrival_profile[
            (arrival_profile["week_of_month"] == wom) & 
            (arrival_profile["weekday"] == weekday) & 
            (arrival_profile["hour"] == hour)
        ]
        if len(a_profile) == 0:
            a_profile = arrival_profile[(arrival_profile["weekday"] == weekday) & (arrival_profile["hour"] == hour)]

        if len(a_profile) > 0:
            weights = a_profile["show_up_probability"] / a_profile["show_up_probability"].sum()
            allocated = np.floor(row["Entries"] * weights).astype(int)
            difference = int(row["Entries"] - allocated.sum())

            if difference > 0:
                largest_idx = np.argsort(weights.values)[::-1]
                allocated.iloc[largest_idx[:difference]] += 1

            for (_, p), cnt in zip(a_profile.iterrows(), allocated):
                arrival_records.extend([
                    ts_hour_start + pd.Timedelta(minutes=int(p["minute"]))
                ] * int(cnt))

        # EXITS
        e_profile = exit_profile[
            (exit_profile["week_of_month"] == wom) & 
            (exit_profile["weekday"] == weekday) & 
            (exit_profile["hour"] == hour)
        ]
        if len(e_profile) == 0:
            e_profile = exit_profile[(exit_profile["weekday"] == weekday) & (exit_profile["hour"] == hour)]

        if len(e_profile) > 0:
            weights = e_profile["show_up_probability"] / e_profile["show_up_probability"].sum()
            allocated = np.floor(row["Exits"] * weights).astype(int)
            difference = int(row["Exits"] - allocated.sum())

            if difference > 0:
                largest_idx = np.argsort(weights.values)[::-1]
                allocated.iloc[largest_idx[:difference]] += 1

            for (_, p), cnt in zip(e_profile.iterrows(), allocated):
                exit_records.extend([
                    ts_hour_start + pd.Timedelta(minutes=int(p["minute"]))
                ] * int(cnt))

    return pd.DataFrame({"arrival_time": arrival_records}), pd.DataFrame({"exit_time": exit_records})


# =========================================================
# 8. PHOTOBOOTH + KIOSK CAPACITY MODEL (1-MINUTE QUEUING)
# =========================================================

def run_capacity_simulation(arrivals_df, kiosk_service_time_sec, photobooth_service_sec):
    working = arrivals_df.copy()
    time_col = "true_arrival_time" if "true_arrival_time" in working.columns else "arrival_time"
    working["bucket"] = working[time_col].dt.floor("1min")

    time_grid = pd.date_range(start=working["bucket"].min(), end=working["bucket"].max(), freq="1min")

    arrivals = (
        working.groupby("bucket")
        .size()
        .reindex(time_grid, fill_value=0)
        .reset_index()
    )
    arrivals.columns = ["bucket", "arrivals"]

    # Calculate 1-minute throughput capacities (2 Photobooths, 5 Kiosks)
    pb_capacity = (2 * 60) / photobooth_service_sec if photobooth_service_sec > 0 else 999999.0
    kiosk_capacity = (5 * 60) / kiosk_service_time_sec

    pb_queue = 0.0
    kiosk_queue = 0.0
    output = []

    for _, row in arrivals.iterrows():
        demand = row["arrivals"]
        
        # Photobooth Queuing Mechanics
        total_pb_demand = demand + pb_queue
        pb_served = min(total_pb_demand, pb_capacity)
        pb_queue = total_pb_demand - pb_served

        # Kiosk Queuing Mechanics (Photobooth output drives Kiosk arrival)
        total_kiosk_demand = pb_served + kiosk_queue
        kiosk_served = min(total_kiosk_demand, kiosk_capacity)
        kiosk_queue = total_kiosk_demand - kiosk_served

        output.append({
            "time_bucket": row["bucket"],
            "arrivals": demand,
            "pb_queue_cars": pb_queue,
            "queue_above_10": pb_queue > 10,
            "queue_above_12": pb_queue > 12,
            "pb_throughput": pb_served,
            "kiosk_queue": kiosk_queue,
            "hall_population": kiosk_queue + kiosk_served,
            "hall_area": (kiosk_queue + kiosk_served) * 1.3
        })

    return pd.DataFrame(output)


# =========================================================
# 9. FUTURE LOCKER FORECAST & SUMMARY (1-MINUTE GRANULARITY)
# =========================================================

def run_locker_lead_time_test(future_exits_df, lead_minutes):
    future = future_exits_df.copy()
    future["locker_start"] = future["exit_time"] - pd.Timedelta(minutes=lead_minutes)
    future["locker_end"] = future["exit_time"]

    start_time = future["locker_start"].min().floor("1min")
    end_time = future["locker_end"].max().ceil("1min")
    grid = pd.date_range(start=start_time, end=end_time, freq="1min")

    starts = np.sort(future["locker_start"].values)
    ends = np.sort(future["locker_end"].values)
    grid_vals = grid.values

    occupied = np.searchsorted(starts, grid_vals, side='right') - np.searchsorted(ends, grid_vals, side='right')

    return pd.DataFrame({
        "timestamp": grid,
        "occupied_lockers": occupied,
        "available_lockers": 297 - occupied,
        "over_capacity": occupied > 297
    })

def monthly_capacity_summary(simulation_df):
    working = simulation_df.copy()
    working["month"] = working["time_bucket"].dt.to_period("M").astype(str)

    return working.groupby("month").agg(
        max_pb_queue=("pb_queue_cars", "max"),
        max_kiosk_queue=("kiosk_queue", "max"),
        hall_population=("hall_population", "max"),
        mins_queue_over_10=("queue_above_10", "sum"),
        mins_queue_over_12=("queue_above_12", "sum")
    ).reset_index()


# =========================================================
# EXECUTION FLOW
# =========================================================

full_year_history_df = load_full_year_profile_data(engine)
arrival_profile = build_historical_arrival_profile(full_year_history_df)
exit_profile = build_historical_exit_profile(full_year_history_df)

print("\n--- HISTORICAL PROFILE DIAGNOSTICS ---")
print(f"Historical Arrival Records: {full_year_history_df['CheckInStarted'].notna().sum():,}")
print(f"Historical Exit Records: {full_year_history_df['ActualCheckedOutDate'].notna().sum():,}")
print(f"Arrival Profile Buckets: {len(arrival_profile):,}")
print(f"Exit Profile Buckets: {len(exit_profile):,}")

future_arrivals_A, future_exits_A = build_future_forecast(engine, arrival_profile, exit_profile)
monthly_forecast_df = load_monthly_forecast(csv_path)
future_arrivals_B, future_exits_B = disaggregate_monthly_to_timestamps(monthly_forecast_df, arrival_profile, exit_profile)

future_arrivals_A["true_arrival_time"] = future_arrivals_A["arrival_time"]
future_arrivals_B["true_arrival_time"] = future_arrivals_B["arrival_time"]

forecast_A_current = run_capacity_simulation(future_arrivals_A, kiosk_metrics["mean_sec"], 37)
forecast_A_upgraded = run_capacity_simulation(future_arrivals_A, kiosk_metrics["mean_sec"], 0)
forecast_B_current = run_capacity_simulation(future_arrivals_B, kiosk_metrics["mean_sec"], 37)
forecast_B_upgraded = run_capacity_simulation(future_arrivals_B, kiosk_metrics["mean_sec"], 0)

print("\n--- FORECAST A DIAGNOSTICS ---")
print(f"Arrival Events: {len(future_arrivals_A):,}")
print(f"Exit Events: {len(future_exits_A):,}")

print("\n--- FORECAST B DIAGNOSTICS ---")
print(f"Arrival Events: {len(future_arrivals_B):,}")
print(f"Exit Events: {len(future_exits_B):,}")

historical_peaks_1m = master_df.groupby(master_df["true_arrival_time"].dt.floor("1min")).size()
future_peaks_A_1m = future_arrivals_A.groupby(future_arrivals_A["arrival_time"].dt.floor("1min")).size()
future_peaks_B_1m = future_arrivals_B.groupby(future_arrivals_B["arrival_time"].dt.floor("1min")).size()

pb_capacity_1m = (2 * 60) / 37

print("\n--- ARRIVAL PEAK DIAGNOSTICS (1-MIN GRANULARITY) ---")
print(f"Historical Peak 1-min Demand: {historical_peaks_1m.max():.0f} cars/min")
print(f"Forecast A Peak 1-min Demand: {future_peaks_A_1m.max():.0f} cars/min")
print(f"Forecast B Peak 1-min Demand: {future_peaks_B_1m.max():.0f} cars/min")
print(f"Photobooth Capacity (1 min): {pb_capacity_1m:.2f} cars/min")

historical_baseline = run_capacity_simulation(master_df, kiosk_metrics["mean_sec"], 37)
historical_upgraded = run_capacity_simulation(master_df, kiosk_metrics["mean_sec"], 0)

historical_comparison = {
    "Metric": [
        "Max Cars Stuck in Photobooth Queue",
        "Minutes Above 10 Cars Queue",
        "Minutes Above 12 Cars Queue",
        "Max People in Hall",
        "Max Hall Area Needed (m²)",
        "Max Kiosk Queue"
    ],
    "Current Historical (37s)": [
        round(historical_baseline["pb_queue_cars"].max(), 1),
        historical_baseline["queue_above_10"].sum(),
        historical_baseline["queue_above_12"].sum(),
        round(historical_baseline["hall_population"].max(), 1),
        f"{historical_baseline['hall_area'].max():.1f}",
        round(historical_baseline["kiosk_queue"].max(), 1)
    ],
    "Upgraded Historical (0s)": [
        round(historical_upgraded["pb_queue_cars"].max(), 1),
        historical_upgraded["queue_above_10"].sum(),
        historical_upgraded["queue_above_12"].sum(),
        round(historical_upgraded["hall_population"].max(), 1),
        f"{historical_upgraded['hall_area'].max():.1f}",
        round(historical_upgraded["kiosk_queue"].max(), 1)
    ]
}

print("\n--- HISTORICAL BARRIERLESS PHOTOBOOTH IMPACT (1-MIN GRANULARITY) ---")
print(pd.DataFrame(historical_comparison).to_string(index=False))

locker_scenarios = {
    "15 Mins": 15,
    "30 Mins": 30,
    "60 Mins": 60,
    "90 Mins": 90,
    "120 Mins": 120,
    "180 Mins": 180
}

locker_results = []
for scenario, lead_time in locker_scenarios.items():
    result = run_locker_lead_time_test(future_exits_A, lead_time)
    locker_results.append({
        "Lead Time": scenario,
        "Peak Occupancy": result["occupied_lockers"].max(),
        "Safety Margin": 297 - result["occupied_lockers"].max(),
        "Hours Over Capacity": (result["over_capacity"].sum() * 1) / 60.0
    })

future_comparison = {
    "Metric": [
        "Max Cars Stuck in Photobooth Queue",
        "Minutes Above 10 Cars Queue",
        "Minutes Above 12 Cars Queue",
        "Max People in Hall",
        "Max Hall Area Needed (m²)"
    ],
    "Future Current (37s)": [
        round(forecast_A_current["pb_queue_cars"].max(), 1),
        forecast_A_current["queue_above_10"].sum(),
        forecast_A_current["queue_above_12"].sum(),
        round(forecast_A_current["hall_population"].max(), 1),
        f"{forecast_A_current['hall_area'].max():.1f}"
    ],
    "Future Upgraded (0s)": [
        round(forecast_A_upgraded["pb_queue_cars"].max(), 1),
        forecast_A_upgraded["queue_above_10"].sum(),
        forecast_A_upgraded["queue_above_12"].sum(),
        round(forecast_A_upgraded["hall_population"].max(), 1),
        f"{forecast_A_upgraded['hall_area'].max():.1f}"
    ]
}

print("\n--- FUTURE BARRIERLESS PHOTOBOOTH IMPACT AUDIT ---")
print(pd.DataFrame(future_comparison).to_string(index=False))

print("\n--- LOCKER LEAD TIME ANALYSIS ---")
print(pd.DataFrame(locker_results).to_string(index=False))

print("\n--- MONTHLY FORECAST A CURRENT ---")
print(monthly_capacity_summary(forecast_A_current).to_string(index=False))

print("\n--- MONTHLY FORECAST A BARRIERLESS ---")
print(monthly_capacity_summary(forecast_A_upgraded).to_string(index=False))
