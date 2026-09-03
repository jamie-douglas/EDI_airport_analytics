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
        
        estimated_hall_area_m2 = (kiosk_queue + kiosk_served) * 1.3
        
        records.append({
            'time_bucket': row['time_bucket'],
            'true_arrivals': arrivals,
            'pb_queue_cars': pb_queue,
            'pb_throughput': pb_output,
            'kiosk_queue_people': kiosk_queue,
            'hall_area_m2': estimated_hall_area_m2,
            'is_hall_overflowing': estimated_hall_area_m2 > 78.0
        })

    return pd.DataFrame(records)

# =========================================================
# 7. RUN THE COMPARISON
# =========================================================

baseline_df = run_2_stage_simulation(master_df, photobooth_delay_sec=37)
upgraded_df = run_2_stage_simulation(master_df, photobooth_delay_sec=0)

comparison = {
    "Metric": [
        "Max Cars Stuck in Photobooth Queue",
        "Max Peak Queue in Hall (People)",
        "Max Hall Area Needed (m²)",
        "15-min Periods Exceeding 78m² Hall Space"
    ],
    "Current (37s Photobooth)": [
        baseline_df['pb_queue_cars'].max(),
        baseline_df['kiosk_queue_people'].max(),
        f"{baseline_df['hall_area_m2'].max():.1f} m²",
        (baseline_df["hall_area_m2"] > 78.0).sum()
    ],
    "Upgraded (0s Photobooth)": [
        upgraded_df['pb_queue_cars'].max(),
        upgraded_df['kiosk_queue_people'].max(),
        f"{upgraded_df['hall_area_m2'].max():.1f} m²",
        (upgraded_df["hall_area_m2"] > 78.0).sum()
    ]
}

print("\n--- BARRIERLESS PHOTOBOOTH IMPACT ---")
print(pd.DataFrame(comparison).to_string(index=False))

# =========================================================
# 8. FUTURE CAPACITY FORECASTING & PROJECTION ENGINE
# =========================================================

def load_full_year_profile_data(engine):
    """
    Pulls a full year of historical entry/exit data from the database 
    including check-in and checkout times for profile and trip duration modeling.
    """
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
        AND "ActualCheckedOutDate" IS NOT NULL
    """
    profile_raw = pd.read_sql(profile_sql, con=engine)
    profile_raw["CheckInStarted"] = pd.to_datetime(profile_raw["CheckInStarted"])
    profile_raw["ActualCheckedOutDate"] = pd.to_datetime(profile_raw["ActualCheckedOutDate"], errors="coerce")
    return profile_raw


def build_historical_arrival_profile(profile_df):
    """
    Extracts the detailed 15-minute arrival profile broken down by 
    weekday, hour, and minute using a full year of historical check-in data.
    """
    df = profile_df.copy()
    df["weekday"] = df["CheckInStarted"].dt.dayofweek
    df["hour"] = df["CheckInStarted"].dt.hour
    df["minute"] = (df["CheckInStarted"].dt.minute // 15) * 15  # snap to 15-min bins
    
    profile_counts = df.groupby(["weekday", "hour", "minute"]).size().reset_index(name="count")
    
    # Calculate intra-hour proportions for Option A (hourly disaggregation)
    hourly_totals = profile_counts.groupby(["weekday", "hour"])["count"].transform("sum")
    profile_counts["intra_hour_weight"] = profile_counts["count"] / hourly_totals
    
    # Calculate global proportions for Option B (monthly disaggregation)
    total_arrivals = len(df)
    profile_counts["global_probability"] = profile_counts["count"] / total_arrivals
    
    return profile_counts


def get_future_demand_data(source_option="A", profile_counts=None, engine=None):
    """
    Loads future demand data:
    A) Pulls hourly forecast from FastPark.v_ForecastEntryandExits (separate entries & exits) and disaggregates.
    B) Reads monthly transaction forecasts from 'transaction_forecast.xlsx' (total booking transactions).
    """
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
        
        simulated_entries = []
        simulated_exits = []
        
        for _, row in forecast_raw.iterrows():
            start_time = row["IntervalStartDateTimeLocal"]
            entries = int(row["Entries"])
            exits = int(row["Exits"])
            
            wday = start_time.dayofweek
            hr = start_time.hour
            
            sub_profile = profile_counts[(profile_counts["weekday"] == wday) & (profile_counts["hour"] == hr)]
            
            if sub_profile.empty:
                weights = {0: 0.25, 15: 0.25, 30: 0.25, 45: 0.25}
            else:
                weights = dict(zip(sub_profile["minute"], sub_profile["intra_hour_weight"]))
            
            # Distribute entries
            if entries > 0:
                for minute_offset, weight in weights.items():
                    count = round(entries * weight)
                    if count > 0:
                        slot_time = start_time + pd.Timedelta(minutes=minute_offset)
                        simulated_entries.extend([slot_time] * count)
                        
            # Distribute exits (using the same intra-hour timing profile)
            if exits > 0:
                for minute_offset, weight in weights.items():
                    count = round(exits * weight)
                    if count > 0:
                        slot_time = start_time + pd.Timedelta(minutes=minute_offset)
                        simulated_exits.extend([slot_time] * count)
                    
        return pd.DataFrame({
            "true_arrival_time": simulated_entries,
            "simulated_departure_time": simulated_exits[:len(simulated_entries)] if len(simulated_exits) >= len(simulated_entries) else simulated_entries
        })
        
    elif source_option == "B":
        print("\n--- Reading Monthly Transaction Forecast from Excel ---")
        excel_path = r"C:\Users\jamie_douglas\Edinburgh Airport Limited\Shared Files - Business Planning\Seasonal Readiness\W26\2. Car Parking\Modelling\transaction_forecast.csv"
        monthly_df = pd.read_csv(excel_path)
        monthly_df["ParsedDate"] = pd.to_datetime(monthly_df["Month"], format="%y-%b")
        return monthly_df
    
    else:
        raise ValueError("Invalid source_option. Choose 'A' or 'B'.")


def disaggregate_monthly_to_timestamps(monthly_df, profile_counts, historical_df):
    """
    Disaggregates monthly transaction totals down to specific 15-minute 
    timestamps using historical global arrival profile weights, while modeling 
    trip durations to generate corresponding departure timestamps for locker audits.
    """
    simulated_timestamps = []
    simulated_departures = []
    
    # Calculate historical trip durations (Check-In to Actual Checked Out) to sample from
    historical_df = historical_df.dropna(subset=["CheckInStarted", "ActualCheckedOutDate"]).copy()
    historical_df["trip_duration"] = (historical_df["ActualCheckedOutDate"] - historical_df["CheckInStarted"]).dt.total_seconds()
    valid_trip_durations = historical_df["trip_duration"].values
    
    for _, row in monthly_df.iterrows():
        target_month = row["ParsedDate"]
        total_bookings = int(row["Transactions"])
        days_in_month = pd.Period(target_month.strftime("%Y-%m")).days_in_month
        
        profile_counts["allocated_count"] = (profile_counts["global_probability"] * total_bookings).round().astype(int)
        
        for _, p_row in profile_counts.iterrows():
            if p_row["allocated_count"] <= 0:
                continue
            
            month_dates = pd.date_range(
                start=target_month, 
                periods=days_in_month, 
                freq="D"
            )
            matching_dates = month_dates[month_dates.dayofweek == p_row["weekday"]]
            
            if len(matching_dates) == 0:
                continue
                
            count_per_date = int(
                max(1, p_row["allocated_count"] // len(matching_dates))
            )
            
            for d in matching_dates:
                timestamp = d.replace(hour=int(p_row["hour"]), minute=int(p_row["minute"]))
                for _ in range(count_per_date):
                    simulated_timestamps.append(timestamp)
                    # Sample a realistic trip duration from historical data
                    if len(valid_trip_durations) > 0:
                        sampled_duration = np.random.choice(valid_trip_durations)
                    else:
                        sampled_duration = 7 * 86400  # Default 7 days if empty
                    simulated_departures.append(timestamp + pd.Timedelta(seconds=sampled_duration))
                
    return pd.DataFrame({
        "true_arrival_time": simulated_timestamps,
        "simulated_departure_time": simulated_departures
    })


# =========================================================
# 9. EXECUTE FUTURE CAPACITY RISK AUDIT (All 4 Components)
# =========================================================

# Choose Data Option: 'A' (Database Hourly Forecast) or 'B' (Excel Monthly Transactions)
FORECAST_SOURCE_OPTION = "A" 

# 1. Load full year of historical data for accurate profiling and trip sampling
full_year_history_df = load_full_year_profile_data(engine=engine)
historical_profile = build_historical_arrival_profile(full_year_history_df)

# 2. Get future demand based on selected option
if FORECAST_SOURCE_OPTION == "A":
    future_raw_df = get_future_demand_data(source_option="A", profile_counts=historical_profile, engine=engine)
else:
    monthly_demand_df = get_future_demand_data(source_option="B")
    future_raw_df = disaggregate_monthly_to_timestamps(monthly_demand_df, historical_profile, full_year_history_df)

# Attach average kiosk duration metric from historical baseline
future_raw_df["kiosk_duration_seconds"] = master_df["kiosk_duration_seconds"].mean()
# Add best-case key deposit timing (+10 mins after arrival/photobooth simulation proxy)
future_raw_df["best_case_key_deposit"] = future_raw_df["true_arrival_time"] + pd.Timedelta(minutes=10)

# 3. Run the 2-Stage Simulation on Future Projections (Photobooth, Kiosks, Hall Standing Area)
future_sim_df = run_2_stage_simulation(future_raw_df, photobooth_delay_sec=37)

# 4. Run Future Locker Occupancy Analysis (297 Lockers)
def run_future_locker_analysis(df):
    start_time = df['best_case_key_deposit'].min().floor('15min')
    end_time = df['simulated_departure_time'].max().ceil('15min')
    time_grid = pd.date_range(start=start_time, end=end_time, freq='15min')
    locker_records = []

    for current_time in time_grid:
        occupied = df[
            (df['best_case_key_deposit'] <= current_time) &
            (df['simulated_departure_time'] > current_time)
        ].shape[0]

        locker_records.append({
            'timestamp': current_time,
            'occupied_lockers': occupied,
            'available_lockers': 297 - occupied,
            'is_over_capacity': occupied > 297
        })

    return pd.DataFrame(locker_records)

future_locker_df = run_future_locker_analysis(future_raw_df)

# 5. Aggregate Monthly Breach Metrics (Hall & Lockers)
future_sim_df["YearMonth"] = future_sim_df["time_bucket"].dt.to_period("M")
monthly_audit = future_sim_df.groupby("YearMonth").agg(
    total_arrivals=("true_arrivals", "sum"),
    max_pb_queue_cars=("pb_queue_cars", "max"),
    max_hall_queue_people=("kiosk_queue_people", "max"),
    max_hall_area_m2=("hall_area_m2", "max"),
    periods_exceeding_78m2=("is_hall_overflowing", "sum")
).reset_index()

monthly_audit["overflow_hours_exceeded"] = (monthly_audit["periods_exceeding_78m2"] * 15) / 60.0

print("\n--- FUTURE CAPACITY BREACH AUDIT (BOOTH, KIOSKS, HALL) ---")
print(monthly_audit.to_string(index=False))

print(f"\n--- FUTURE KEY LOCKER CAPACITY METRICS (297 Lockers) ---")
print(f"Peak Future Lockers Occupied: {future_locker_df['occupied_lockers'].max()} / 297")
print(f"Min Available Locker Safety Margin: {future_locker_df['available_lockers'].min()}")
print(f"Total future hours overflowing 297 lockers: {(future_locker_df['is_over_capacity'].sum() * 15)/60:.1f} hours")
