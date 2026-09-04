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
# 1. LOAD DATA & CALCULATE HARDWARE CAPACITIES
# =========================================================

photobooth_files = list(photobooth_path.glob("*.csv"))
photobooth_list = []
for file in photobooth_files:
    df = pd.read_csv(file)
    df["Scan Date"] = pd.to_datetime(df["Scan Date"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
    photobooth_list.append(df)

raw_scans = pd.concat(photobooth_list, ignore_index=True).drop_duplicates()

photobooth_summary = raw_scans.groupby("Booking Ref").agg(
    customer_arrival_photobooth=("Scan Date", "min"),
    staff_return_photobooth=("Scan Date", "max"),
    total_scans_count=("Scan Date", "count")
).reset_index()

photobooth_summary["scan_gap_hours"] = (
    (photobooth_summary["staff_return_photobooth"] - photobooth_summary["customer_arrival_photobooth"])
    .dt.total_seconds() / 3600.0
)
photobooth_summary = photobooth_summary[photobooth_summary["scan_gap_hours"] >= 6]

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

master_df = pd.merge(
    actuals_df,
    photobooth_summary,
    left_on="BookingReference",
    right_on="Booking Ref",
).drop(columns=["Booking Ref"], errors="ignore")

# Baseline Metrics & Dwell Delays
master_df["kiosk_duration_seconds"] = (master_df["CheckInEnded"] - master_df["CheckInStarted"]).dt.total_seconds()
valid_kiosk_durations = master_df[master_df["kiosk_duration_seconds"] > 0]["kiosk_duration_seconds"]
mean_kiosk_sec = valid_kiosk_durations.mean()

master_df["ferry_to_kiosk_dwell_mins"] = (master_df["CheckInStarted"] - master_df["customer_arrival_photobooth"]).dt.total_seconds() / 60.0
master_df = master_df[master_df["ferry_to_kiosk_dwell_mins"].between(0, 60)]
median_dwell_mins = int(round(master_df["ferry_to_kiosk_dwell_mins"].median()))

# Maximum Physical Hourly Capacities
CAPACITY_LIMITS = {
    "PB_37s_hourly_cap": (2 * 3600) / 37.0,          # ~194.6 cars/hr
    "PB_0s_hourly_cap": 999999.0,                     # Unconstrained
    "Kiosk_5_hourly_cap": (5 * 3600) / mean_kiosk_sec,# ~300 customers/hr
    "Hall_Max_Occupancy": 60,                        # Max people before crowding 78m²
    "Max_Lockers": 297                               # Total physical lockers
}

print("=" * 60)
print("a) HARDWARE HOURLY CAPACITY BENCHMARKS")
print("=" * 60)
print(f"Photobooth Capacity (37s Service, 2 Lanes): {CAPACITY_LIMITS['PB_37s_hourly_cap']:.1f} cars/hr")
print(f"Kiosk Capacity (5 Kiosks @ {mean_kiosk_sec:.1f}s avg): {CAPACITY_LIMITS['Kiosk_5_hourly_cap']:.1f} customers/hr")
print(f"Hall Space Ceiling: {CAPACITY_LIMITS['Hall_Max_Occupancy']} people (78 m² limit)")
print(f"Key Locker Capacity: {CAPACITY_LIMITS['Max_Lockers']} lockers")
print(f"Park & Walk Transit Lag: {median_dwell_mins} minutes\n")


# =========================================================
# 2. HISTORICAL PROFILES & FORECAST DISAGGREGATION ENGINE
# =========================================================

def load_full_year_profile_data(engine):
    sql = """
    SELECT "BookingReference", "CheckInStarted", "ActualCheckedOutDate"
    FROM FastPark.v_EntryAndExits
    WHERE "CheckInStarted" IS NOT NULL AND "CheckInStarted" >= DATEADD(year,-1,GETDATE())
    """
    df = pd.read_sql(sql, con=engine)
    df["CheckInStarted"] = pd.to_datetime(df["CheckInStarted"])
    df["ActualCheckedOutDate"] = pd.to_datetime(df["ActualCheckedOutDate"], errors="coerce")
    return df

def build_profile(df, time_col):
    working = df.dropna(subset=[time_col]).copy()
    working["week_of_month"] = (working[time_col].dt.day - 1) // 7 + 1
    working["weekday"] = working[time_col].dt.dayofweek
    working["hour"] = working[time_col].dt.hour
    working["minute"] = working[time_col].dt.minute

    profile = working.groupby(["week_of_month", "weekday", "hour", "minute"]).size().reset_index(name="count")
    group_totals = profile.groupby(["week_of_month", "weekday"])["count"].transform("sum")
    profile["prob"] = profile["count"] / group_totals
    return profile

full_year_history_df = load_full_year_profile_data(engine)
arrival_profile = build_profile(full_year_history_df, "CheckInStarted")
exit_profile = build_profile(full_year_history_df, "ActualCheckedOutDate")

# METHOD A: Disaggregate Hourly Forecast Query to 1-Min
def build_forecast_A(engine, arrival_profile, exit_profile):
    sql = """
    SELECT "IntervalStartDateTimeLocal", "Entries", "Exits"
    FROM FastPark.v_ForecastEntryandExits
    WHERE "IntervalStartDateTimeLocal" >= GETDATE()
    """
    df = pd.read_sql(sql, con=engine)
    df["IntervalStartDateTimeLocal"] = pd.to_datetime(df["IntervalStartDateTimeLocal"])

    arr_records, ext_records = [], []
    for _, row in df.iterrows():
        ts = row["IntervalStartDateTimeLocal"]
        wom, weekday, hour = (ts.day - 1) // 7 + 1, ts.dayofweek, ts.hour

        # Arrivals
        a_p = arrival_profile[(arrival_profile["week_of_month"] == wom) & (arrival_profile["weekday"] == weekday) & (arrival_profile["hour"] == hour)]
        if len(a_p) == 0:
            a_p = arrival_profile[(arrival_profile["weekday"] == weekday) & (arrival_profile["hour"] == hour)]
        if len(a_p) > 0:
            w = a_p["prob"] / a_p["prob"].sum()
            counts = np.floor(row["Entries"] * w).astype(int)
            diff = int(row["Entries"] - counts.sum())
            if diff > 0: counts.iloc[np.argsort(w.values)[::-1][:diff]] += 1
            for (_, p), cnt in zip(a_p.iterrows(), counts):
                arr_records.extend([ts.floor("h") + pd.Timedelta(minutes=int(p["minute"]))] * int(cnt))

        # Exits
        e_p = exit_profile[(exit_profile["week_of_month"] == wom) & (exit_profile["weekday"] == weekday) & (exit_profile["hour"] == hour)]
        if len(e_p) == 0:
            e_p = exit_profile[(exit_profile["weekday"] == weekday) & (exit_profile["hour"] == hour)]
        if len(e_p) > 0:
            w = e_p["prob"] / e_p["prob"].sum()
            counts = np.floor(row["Exits"] * w).astype(int)
            diff = int(row["Exits"] - counts.sum())
            if diff > 0: counts.iloc[np.argsort(w.values)[::-1][:diff]] += 1
            for (_, p), cnt in zip(e_p.iterrows(), counts):
                ext_records.extend([ts.floor("h") + pd.Timedelta(minutes=int(p["minute"]))] * int(cnt))

    return pd.DataFrame({"arrival_time": arr_records}), pd.DataFrame({"exit_time": ext_records})

# METHOD B: Disaggregate Monthly CSV Total Transactions to 1-Min
def build_forecast_B(csv_path, arrival_profile, exit_profile):
    monthly_df = pd.read_csv(csv_path)
    monthly_df["Month"] = pd.to_datetime(monthly_df["Month"], format="%y-%b")
    arr_records, ext_records = [], []

    for _, row in monthly_df.iterrows():
        m_start = row["Month"].replace(day=1)
        days = pd.date_range(m_start, m_start + pd.offsets.MonthEnd(0), freq="D")
        daily_vol = row["Transactions"] / len(days)

        for day in days:
            wom, weekday = (day.day - 1) // 7 + 1, day.dayofweek
            
            # Arrivals
            a_p = arrival_profile[(arrival_profile["week_of_month"] == wom) & (arrival_profile["weekday"] == weekday)]
            if len(a_p) == 0: a_p = arrival_profile[arrival_profile["weekday"] == weekday]
            if len(a_p) > 0:
                w = a_p["prob"] / a_p["prob"].sum()
                counts = np.floor(daily_vol * w).astype(int)
                for (_, p), cnt in zip(a_p.iterrows(), counts):
                    arr_records.extend([day + pd.Timedelta(hours=int(p["hour"]), minutes=int(p["minute"]))] * int(cnt))

            # Exits
            e_p = exit_profile[(exit_profile["week_of_month"] == wom) & (exit_profile["weekday"] == weekday)]
            if len(e_p) == 0: e_p = exit_profile[exit_profile["weekday"] == weekday]
            if len(e_p) > 0:
                w = e_p["prob"] / e_p["prob"].sum()
                counts = np.floor(daily_vol * w).astype(int)
                for (_, p), cnt in zip(e_p.iterrows(), counts):
                    ext_records.extend([day + pd.Timedelta(hours=int(p["hour"]), minutes=int(p["minute"]))] * int(cnt))

    return pd.DataFrame({"arrival_time": arr_records}), pd.DataFrame({"exit_time": ext_records})

print("=" * 60)
print("b) GENERATING 1-MINUTE FORECAST PROFILES")
print("=" * 60)
future_arrivals_A, future_exits_A = build_forecast_A(engine, arrival_profile, exit_profile)
future_arrivals_B, future_exits_B = build_forecast_B(csv_path, arrival_profile, exit_profile)
print(f"Forecast A Generated: {len(future_arrivals_A):,} arrival events")
print(f"Forecast B Generated: {len(future_arrivals_B):,} arrival events\n")


# =========================================================
# 3. QUEUE SIMULATION ENGINE WITH DWELL LAG
# =========================================================

def run_capacity_simulation(arrivals_df, kiosk_service_sec, photobooth_service_sec, dwell_lag_mins):
    working = arrivals_df.copy()
    time_col = "true_arrival_time" if "true_arrival_time" in working.columns else "arrival_time"
    working["bucket"] = working[time_col].dt.floor("1min")

    time_grid = pd.date_range(start=working["bucket"].min(), end=working["bucket"].max(), freq="1min")
    arrivals = working.groupby("bucket").size().reindex(time_grid, fill_value=0).values

    n_steps = len(time_grid)
    pb_capacity_1m = (2 * 60) / photobooth_service_sec if photobooth_service_sec > 0 else 999999.0
    kiosk_capacity_1m = (5 * 60) / kiosk_service_sec

    pb_queue = 0.0
    kiosk_queue = 0.0
    kiosk_arrivals_delayed = np.zeros(n_steps + dwell_lag_mins + 100)

    output = []
    for i in range(n_steps):
        demand = arrivals[i]
        
        # 1. Photobooth Processing
        total_pb_demand = demand + pb_queue
        pb_served = min(total_pb_demand, pb_capacity_1m)
        pb_queue = total_pb_demand - pb_served

        # 2. Park & Walk Lag Delay (Delays Kiosk Arrival by dwell_lag_mins)
        kiosk_arrivals_delayed[i + dwell_lag_mins] += pb_served

        # 3. Kiosk Processing
        current_kiosk_arrival = kiosk_arrivals_delayed[i]
        total_kiosk_demand = current_kiosk_arrival + kiosk_queue
        kiosk_served = min(total_kiosk_demand, kiosk_capacity_1m)
        kiosk_queue = total_kiosk_demand - kiosk_served

        hall_pop = kiosk_queue + kiosk_served
        output.append({
            "time_bucket": time_grid[i],
            "arrivals": demand,
            "pb_queue_cars": pb_queue,
            "pb_queue_over_10": pb_queue > 10,
            "pb_queue_over_12": pb_queue > 12,
            "kiosk_queue_people": kiosk_queue,
            "hall_population": hall_pop,
            "hall_over_60": hall_pop > 60
        })

    return pd.DataFrame(output)


# =========================================================
# 4. MONTHLY COMPARATIVE AUDIT ENGINE (37s vs 0s)
# =========================================================

def build_monthly_comparison_table(df_37s, df_0s, label="Forecast"):
    df_37s["month"] = df_37s["time_bucket"].dt.to_period("M").astype(str)
    df_0s["month"] = df_0s["time_bucket"].dt.to_period("M").astype(str)

    m37 = df_37s.groupby("month").agg(
        max_pb_q_37s=("pb_queue_cars", "max"),
        mins_pb_over_10_37s=("pb_queue_over_10", "sum"),
        max_hall_pop_37s=("hall_population", "max"),
        mins_hall_over_60_37s=("hall_over_60", "sum")
    )

    m0 = df_0s.groupby("month").agg(
        max_pb_q_0s=("pb_queue_cars", "max"),
        max_hall_pop_0s=("hall_population", "max"),
        mins_hall_over_60_0s=("hall_over_60", "sum")
    )

    comp = m37.join(m0).reset_index()
    comp.insert(0, "Source", label)
    
    # Capacity Status Determination
    comp["Status_37s"] = np.where((comp["max_pb_q_37s"] > 10) | (comp["max_hall_pop_37s"] > 60), "OVER CAP", "OK")
    comp["Status_0s"] = np.where((comp["max_pb_q_0s"] > 10) | (comp["max_hall_pop_0s"] > 60), "OVER CAP", "OK")
    
    return comp


# =========================================================
# EXECUTION & AUDIT OUTPUTS
# =========================================================

# Historic Baseline
master_df["true_arrival_time"] = master_df["customer_arrival_photobooth"]
hist_37s = run_capacity_simulation(master_df, mean_kiosk_sec, 37, median_dwell_mins)
hist_0s  = run_capacity_simulation(master_df, mean_kiosk_sec, 0, median_dwell_mins)

# Forecast A Baseline
fA_37s = run_capacity_simulation(future_arrivals_A, mean_kiosk_sec, 37, median_dwell_mins)
fA_0s  = run_capacity_simulation(future_arrivals_A, mean_kiosk_sec, 0, median_dwell_mins)

# Forecast B Baseline
fB_37s = run_capacity_simulation(future_arrivals_B, mean_kiosk_sec, 37, median_dwell_mins)
fB_0s  = run_capacity_simulation(future_arrivals_B, mean_kiosk_sec, 0, median_dwell_mins)


print("=" * 60)
print("c) & d) MONTHLY CAPACITY AUDIT & PHOTOBOOTH IMPACT (37s vs 0s)")
print("=" * 60)

hist_table = build_monthly_comparison_table(hist_37s, hist_0s, "Historical")
fA_table   = build_monthly_comparison_table(fA_37s, fA_0s, "Forecast A (Interval)")
fB_table   = build_monthly_comparison_table(fB_37s, fB_0s, "Forecast B (Monthly)")

full_audit = pd.concat([hist_table, fA_table, fB_table], ignore_index=True)

# Reorder & format for clear presentation
display_cols = [
    "Source", "month", 
    "max_pb_q_37s", "mins_pb_over_10_37s", "max_hall_pop_37s", "Status_37s",
    "max_pb_q_0s", "max_hall_pop_0s", "mins_hall_over_60_0s", "Status_0s"
]

print(full_audit[display_cols].to_string(index=False))


# =========================================================
# KEY LOCKER CAPACITY BREACH ANALYSIS
# =========================================================

def analyze_future_lockers(future_exits_df, lead_mins=15):
    future = future_exits_df.copy()
    future["locker_start"] = future["exit_time"] - pd.Timedelta(minutes=lead_mins)
    future["locker_end"] = future["exit_time"]

    time_grid = pd.date_range(start=future["locker_start"].min().floor("1min"), end=future["locker_end"].max().ceil("1min"), freq="1min")
    starts, ends = np.sort(future["locker_start"].values), np.sort(future["locker_end"].values)
    occupied = np.searchsorted(starts, time_grid.values, side='right') - np.searchsorted(ends, time_grid.values, side='right')

    res = pd.DataFrame({"time_bucket": time_grid, "occupied": occupied})
    res["month"] = res["time_bucket"].dt.to_period("M").astype(str)
    
    summary = res.groupby("month").agg(
        peak_lockers=("occupied", "max"),
        hours_over_cap=("occupied", lambda x: (x > 297).sum() / 60.0)
    ).reset_index()
    summary["Locker_Status"] = np.where(summary["peak_lockers"] > 297, "OVER CAP", "OK")
    return summary

print("\n" + "=" * 60)
print("FUTURE KEY LOCKER CAPACITY BREACH SUMMARY (297 CAP)")
print("=" * 60)
print("Forecast A Key Lockers:")
print(analyze_future_lockers(future_exits_A).to_string(index=False))
print("\nForecast B Key Lockers:")
print(analyze_future_lockers(future_exits_B).to_string(index=False))
