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

from modules.utils.db import get_engine

# ============================
# 1. LOAD AND AGGREGATE PHOTOBOOTH SCAN DATA
# ==========================

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

# Drop duplicates in case the same scan is recorded multiple times
raw_scans = raw_scans.drop_duplicates()

# Collapse multiple scans into Customer Arrival (MIN) and Staff Return (MAX)
photobooth_summary = raw_scans.groupby("Booking Ref").agg(
    customer_arrival_photobooth=("Scan Date", "min"),
    staff_return_photobooth=("Scan Date", "max"),
    total_scans_count=("Scan Date", "count")
).reset_index()

# Calculate the time between customer arrival and staff return scans
photobooth_summary["scan_gap_hours"] = (photobooth_summary["staff_return_photobooth"] - photobooth_summary["customer_arrival_photobooth"]).dt.total_seconds() / 3600.0

print(f"Bookings before journey validation: {len(photobooth_summary)}")

# Keep only bookings with arrival and return scans at least 6 hours apart
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
# 4. CALCULATE BASELINE METRICS
# =========================================================

# 1. Kiosk Transaction duration (seconds)
master_df["kiosk_duration_seconds"] = (master_df["CheckInEnded"] - master_df["CheckInStarted"]).dt.total_seconds()
kiosk_metrics = {
    "mean_sec": master_df["kiosk_duration_seconds"].mean(),
    "median_sec": master_df["kiosk_duration_seconds"].median(),
    "p50_sec": master_df["kiosk_duration_seconds"].quantile(0.5),
    "p90_sec": master_df["kiosk_duration_seconds"].quantile(0.9),
    "p95_sec": master_df["kiosk_duration_seconds"].quantile(0.95),
}

# Theoretical Max Kiosk Throughput (5 kiosks)
max_kiosk_hourly_throughput = (5 * 3600) / kiosk_metrics["mean_sec"]

# 2. Ferry Dwell and Walk Times
master_df["ferry_to_kiosk_dwell_mins"] = (master_df["CheckInStarted"] - master_df["customer_arrival_photobooth"]).dt.total_seconds() / 60.0

print("\nBefore filtering:")
print(master_df["ferry_to_kiosk_dwell_mins"].describe())
len_before = len(master_df)

master_df = master_df[
    master_df["ferry_to_kiosk_dwell_mins"].between(0, 60)
]

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

# 3. Arrival Offset Distribution
master_df["arrival_offset_min"] = (master_df["customer_arrival_photobooth"] - master_df["ExpectedArrivalDate"]).dt.total_seconds() / 60.0
offset_metrics = {
    "mean_offset_min": master_df["arrival_offset_min"].mean(),
    "median_offset_min": master_df["arrival_offset_min"].median(),
    "p10_early_min": master_df["arrival_offset_min"].quantile(0.1),
    "p90_late_min": master_df["arrival_offset_min"].quantile(0.9),
}

print(f"--- KIOSK SERVICE BASELINE ---")
print(f"Average check-in time: {kiosk_metrics['mean_sec']:.1f}s")
print(f"90th Percentile check-in time: {kiosk_metrics['p90_sec']:.1f}s")
print(f"Max theoretical kiosk throughput (5 kiosks): {max_kiosk_hourly_throughput:.1f} cars/hr")

print(f"\n--- FERRY DWELL TIME BASELINE ---")
print(f"Average ferry dwell time: {dwell_metrics['mean_mins']:.1f} mins")

# Best-case key deposit and locker metrics
master_df["best_case_key_deposit"] = master_df["staff_return_photobooth"] + pd.Timedelta(minutes=10)
master_df["best_case_locker_dwell_hours"] = (master_df["ActualCheckedOutDate"] - master_df["best_case_key_deposit"]).dt.total_seconds() / 3600.0

def run_locker_occupancy_analysis(df):
    start_time = df['best_case_key_deposit'].min().floor('15min')
    end_time = df['ActualCheckedOutDate'].max().ceil('15min')
    time_grid = pd.date_range(start=start_time, end=end_time, freq='15min')
    locker_records = []

    for current_time in time_grid:
        occupied = df[
            (df['best_case_key_deposit'] <= current_time) &
            (df['ActualCheckedOutDate'] > current_time)
        ].shape[0]

        locker_records.append({
            'timestamp': current_time,
            'occupied_lockers': occupied,
            'available_lockers': 297 - occupied,
            'is_over_capacity': occupied > 297
        })

    return pd.DataFrame(locker_records)

locker_df = run_locker_occupancy_analysis(master_df)

print(f"\n--- KEY LOCKER CAPACITY METRICS ---")
print(f"Peak Lockers Occupied (Best Case): {locker_df['occupied_lockers'].max()} / 297")
print(f"Min Available Locker Safety Margin: {locker_df['available_lockers'].min()}")
print(f"Total hours overflowing 297 lockers: {(locker_df['is_over_capacity'].sum() * 15)/60:.1f} hours")

# =========================================================
# 5. CALCULATE DRIFT & RECONSTRUCT TRUE ARRIVALS
# =========================================================

master_df["raw_arrival_offset_mins"] = (
    master_df["customer_arrival_photobooth"] - master_df["ExpectedArrivalDate"]
).dt.total_seconds() / 60.0

quiet_hours_mask = master_df["customer_arrival_photobooth"].dt.hour.isin([11, 12, 13, 14, 21, 22, 23])
baseline_offset_mins = master_df.loc[quiet_hours_mask, "raw_arrival_offset_mins"].median()

print(f"\n--- QUEUE DRIFT ANALYSIS ---")
print(f"Natural Baseline Offset: {baseline_offset_mins:.1f} mins")

master_df["estimated_queue_wait_mins"] = np.maximum(
    0, 
    master_df["raw_arrival_offset_mins"] - baseline_offset_mins
)

print(f"Max estimated queue wait found: {master_df['estimated_queue_wait_mins'].max():.1f} mins")

master_df["true_arrival_time"] = (
    master_df["customer_arrival_photobooth"] - pd.to_timedelta(master_df["estimated_queue_wait_mins"], unit='m')
)

# =========================================================
# 6. 2-STAGE CAPACITY SIMULATION (PHOTOBOOTH -> KIOSK)
# =========================================================

def run_2_stage_simulation(df, photobooth_delay_sec=37):
    df['arrival_bucket'] = df['true_arrival_time'].dt.floor('15min')
    
    start_time = df['arrival_bucket'].min()
    end_time = df['arrival_bucket'].max()
    time_grid = pd.date_range(start=start_time, end=end_time, freq='15min')
    
    raw_demand = df.groupby('arrival_bucket').size().reindex(time_grid, fill_value=0).reset_index()
    raw_demand.columns = ['time_bucket', 'raw_arrivals']
    
    if photobooth_delay_sec > 0:
        pb_capacity_15min = (2 * 900) / photobooth_delay_sec
    else:
        pb_capacity_15min = float('inf')
        
    avg_kiosk_sec = df["kiosk_duration_seconds"].mean()
    kiosk_capacity_15min = (5 * 900) / avg_kiosk_sec

    pb_queue = 0
    kiosk_queue = 0
    records = []

    for _, row in raw_demand.iterrows():
        arrivals = row['raw_arrivals']
        
        # Stage 1: Photobooth
        total_at_pb = pb_queue + arrivals
        pb_output = min(total_at_pb, pb_capacity_15min)
        pb_queue = total_at_pb - pb_output
        
        # Stage 2: Kiosk Hall
        total_at_kiosk = kiosk_queue + pb_output
        kiosk_served = min(total_at_kiosk, kiosk_capacity_15min)
        kiosk_queue = total_at_kiosk - kiosk_served
        
        total_people_in_hall = kiosk_queue + kiosk_served
        estimated_hall_area_m2 = total_people_in_hall * 1.3
        
        records.append({
            'time_bucket': row['time_bucket'],
            'true_arrivals': arrivals,
            'pb_queue_cars': pb_queue,
            'pb_throughput': pb_output,
            'total_people_in_hall': total_people_in_hall,
            'kiosk_queue_people': kiosk_queue,
            'hall_area_m2': estimated_hall_area_m2,
            'is_hall_overflowing': estimated_hall_area_m2 > 78.0
        })

    return pd.DataFrame(records)


# =========================================================
# 7. HISTORICAL BARRIERLESS PHOTOBOOTH COMPARISON
# =========================================================

hist_baseline_df = run_2_stage_simulation(master_df, photobooth_delay_sec=37)
hist_upgraded_df = run_2_stage_simulation(master_df, photobooth_delay_sec=0)

historical_comparison = {
    "Metric": [
        "Max Cars Stuck in Photobooth Queue",
        "Max People in Hall (15-min Window)",
        "Max Hall Area Needed (m²)",
        "15-min Periods Exceeding 78m² Hall Space"
    ],
    "Current Historical (37s Photobooth)": [
        hist_baseline_df['pb_queue_cars'].max(),
        hist_baseline_df['total_people_in_hall'].max(),
        f"{hist_baseline_df['hall_area_m2'].max():.1f} m²",
        (hist_baseline_df["hall_area_m2"] > 78.0).sum()
    ],
    "Upgraded Historical (0s Photobooth)": [
        hist_upgraded_df['pb_queue_cars'].max(),
        hist_upgraded_df['total_people_in_hall'].max(),
        f"{hist_upgraded_df['hall_area_m2'].max():.1f} m²",
        (hist_upgraded_df["hall_area_m2"] > 78.0).sum()
    ]
}

print("\n--- HISTORICAL BARRIERLESS PHOTOBOOTH IMPACT ---")
print(pd.DataFrame(historical_comparison).to_string(index=False))


# =========================================================
# 8. FUTURE CAPACITY FORECASTING & PROJECTION ENGINE
# =========================================================

def load_full_year_profile_data(engine):
    print("\n--- Pulling Full Year Historical Data for Profile ---")
    profile_sql = """
    SELECT 
        "BookingReference",
        "CheckInStarted",
        "ExpectedArrivalDate",
        "ActualCheckedOutDate"
    FROM FastPark.v_EntryAndExits
    WHERE 
        "ExpectedArrivalDate" >= DATEADD(year, -1, GETDATE())
        AND "ExpectedArrivalDate" < GETDATE()
        AND "CheckInStarted" IS NOT NULL
    """
    profile_raw = pd.read_sql(profile_sql, con=engine)
    profile_raw["CheckInStarted"] = pd.to_datetime(profile_raw["CheckInStarted"])
    profile_raw["ActualCheckedOutDate"] = pd.to_datetime(profile_raw["ActualCheckedOutDate"], errors="coerce")
    return profile_raw


def build_historical_arrival_profile(profile_df):
    df = profile_df.copy()
    df["weekday"] = df["CheckInStarted"].dt.dayofweek
    df["hour"] = df["CheckInStarted"].dt.hour
    df["minute"] = (df["CheckInStarted"].dt.minute // 15) * 15  
    
    profile_counts = df.groupby(["weekday", "hour", "minute"]).size().reset_index(name="count")
    
    hourly_totals = profile_counts.groupby(["weekday", "hour"])["count"].transform("sum")
    profile_counts["intra_hour_weight"] = profile_counts["count"] / hourly_totals
    
    total_arrivals = len(df)
    profile_counts["global_probability"] = profile_counts["count"] / total_arrivals
    
    return profile_counts


def get_future_demand_data(source_option="A", profile_counts=None, historical_df=None, engine=None):
    if source_option == "A":
        print("\n--- Pulling Hourly Forecast from Database ---")
        future_sql = """
        SELECT 
            "IntervalStartDateTimeLocal",
            "Entries",
            "Exits"
        FROM FastPark.v_ForecastEntryandExits
        WHERE "IntervalStartDateTimeLocal" >= GETDATE()
        """
        forecast_raw = pd.read_sql(future_sql, con=engine)
        forecast_raw["IntervalStartDateTimeLocal"] = pd.to_datetime(forecast_raw["IntervalStartDateTimeLocal"])
        
        historical_df = historical_df.dropna(subset=["CheckInStarted", "ActualCheckedOutDate"]).copy()
        valid_trip_durations = (historical_df["ActualCheckedOutDate"] - historical_df["CheckInStarted"]).dt.total_seconds().values if len(historical_df) > 0 else np.array([7 * 86400])
        
        simulated_timestamps = []
        simulated_departures = []
        
        for _, row in forecast_raw.iterrows():
            start_time = row["IntervalStartDateTimeLocal"]
            entries = int(row["Entries"])
            if entries <= 0:
                continue
            
            wday = start_time.dayofweek
            hr = start_time.hour
            
            sub_profile = profile_counts[(profile_counts["weekday"] == wday) & (profile_counts["hour"] == hr)]
            weights = {0: 0.25, 15: 0.25, 30: 0.25, 45: 0.25} if sub_profile.empty else dict(zip(sub_profile["minute"], sub_profile["intra_hour_weight"]))
            
            for minute_offset, weight in weights.items():
                count = round(entries * weight)
                if count > 0:
                    slot_time = start_time + pd.Timedelta(minutes=minute_offset)
                    for _ in range(count):
                        simulated_timestamps.append(slot_time)
                        simulated_departures.append(slot_time + pd.Timedelta(seconds=np.random.choice(valid_trip_durations)))
                    
        return pd.DataFrame({
            "true_arrival_time": simulated_timestamps,
            "simulated_departure_time": simulated_departures
        })
        
    elif source_option == "B":
        print("\n--- Reading Monthly Transaction Forecast from Excel ---")
        excel_path = SCRIPT_DIR / "transaction_forecast.xlsx"
        monthly_df = pd.read_excel(excel_path)
        monthly_df["ParsedDate"] = pd.to_datetime(monthly_df["Month"], format="%y-%b")
        return monthly_df
    else:
        raise ValueError("Invalid source_option. Choose 'A' or 'B'.")


def disaggregate_monthly_to_timestamps(monthly_df, profile_counts, historical_df):
    simulated_timestamps = []
    simulated_departures = []
    
    historical_df = historical_df.dropna(subset=["CheckInStarted", "ActualCheckedOutDate"]).copy()
    valid_trip_durations = (historical_df["ActualCheckedOutDate"] - historical_df["CheckInStarted"]).dt.total_seconds().values if len(historical_df) > 0 else np.array([7 * 86400])
    
    for _, row in monthly_df.iterrows():
        target_month = row["ParsedDate"]
        total_bookings = int(row["Transactions"])
        days_in_month = pd.Period(target_month.strftime("%Y-%m")).days_in_month
        
        p_subset = profile_counts.copy()
        p_subset["allocated_count"] = (p_subset["global_probability"] * total_bookings).round().astype(int)
        
        for _, p_row in p_subset.iterrows():
            if p_row["allocated_count"] <= 0:
                continue
            
            month_dates = pd.date_range(start=target_month, periods=days_in_month, freq="D")
            matching_dates = month_dates[month_dates.dayofweek == p_row["weekday"]]
            
            if len(matching_dates) == 0:
                continue
                
            count_per_date = max(1, p_row["allocated_count"] // len(matching_dates))
            
            for d in matching_dates:
                timestamp = d.replace(hour=int(p_row["hour"]), minute=int(p_row["minute"]))
                for _ in range(count_per_date):
                    simulated_timestamps.append(timestamp)
                    simulated_departures.append(timestamp + pd.Timedelta(seconds=np.random.choice(valid_trip_durations)))
                
    return pd.DataFrame({
        "true_arrival_time": simulated_timestamps,
        "simulated_departure_time": simulated_departures
    })


# =========================================================
# 9. EXECUTE FUTURE CAPACITY RISK AUDIT (37s vs 0s Comparison)
# =========================================================

FORECAST_SOURCE_OPTION = "A" 

full_year_history_df = load_full_year_profile_data(engine=engine)
historical_profile = build_historical_arrival_profile(full_year_history_df)

if FORECAST_SOURCE_OPTION == "A":
    future_raw_df = get_future_demand_data(source_option="A", profile_counts=historical_profile, historical_df=full_year_history_df, engine=engine)
else:
    monthly_demand_df = get_future_demand_data(source_option="B")
    future_raw_df = disaggregate_monthly_to_timestamps(monthly_demand_df, historical_profile, full_year_history_df)

future_raw_df["kiosk_duration_seconds"] = master_df["kiosk_duration_seconds"].mean()

# Run future simulations for both Current (37s) and Upgraded (0s)
future_baseline_df = run_2_stage_simulation(future_raw_df, photobooth_delay_sec=37)
future_upgraded_df = run_2_stage_simulation(future_raw_df, photobooth_delay_sec=0)

future_comparison = {
    "Metric": [
        "Max Future Cars Stuck in Photobooth Queue",
        "Max Future People in Hall (15-min Window)",
        "Max Future Hall Area Needed (m²)",
        "Future 15-min Periods Exceeding 78m² Hall Space"
    ],
    "Future Current (37s Photobooth)": [
        future_baseline_df['pb_queue_cars'].max(),
        future_baseline_df['total_people_in_hall'].max(),
        f"{future_baseline_df['hall_area_m2'].max():.1f} m²",
        (future_baseline_df["hall_area_m2"] > 78.0).sum()
    ],
    "Future Upgraded (0s Photobooth)": [
        future_upgraded_df['pb_queue_cars'].max(),
        future_upgraded_df['total_people_in_hall'].max(),
        f"{future_upgraded_df['hall_area_m2'].max():.1f} m²",
        (future_upgraded_df["hall_area_m2"] > 78.0).sum()
    ]
}

print("\n--- FUTURE BARRIERLESS PHOTOBOOTH IMPACT AUDIT ---")
print(pd.DataFrame(future_comparison).to_string(index=False))
