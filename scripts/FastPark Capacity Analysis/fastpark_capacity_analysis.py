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
# LOAD AND AGGREGATE PHOTOBOOTH SCAN DATA
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

#Drop duplicates in case the same scan is recorded multiple times
raw_scans = raw_scans.drop_duplicates()

#collapse multiple scans into Customer Arrival (MIN) and Staff Return (MAX)
photobooth_summary = raw_scans.groupby("Booking Ref").agg(
    customer_arrival_photobooth=("Scan Date", "min"),
    staff_return_photobooth=("Scan Date", "max"),
    total_scans_count=("Scan Date", "count")
).reset_index()

#calculate the time between customer arrival and staff return scans
photobooth_summary["scan_gap_hours"] = (photobooth_summary["staff_return_photobooth"] - photobooth_summary["customer_arrival_photobooth"]).dt.total_seconds() / 3600.0

print(f"Bookings before journey validation: {len(photobooth_summary)}")

#keep only bookings with arrival and return scans at least 6 hours apart
photobooth_summary = photobooth_summary[photobooth_summary["scan_gap_hours"] >= 6]

print(f"Bookings after journey validation: {len(photobooth_summary)}")


#============================
# LOAD AND COMBINE BOOKING / ACTUALS DATA
#===========================
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

#===========================
#3. MERGE BOOKING AND ACTUALS DATA WITH PHOTOBOOTH DATA
#===========================

master_df = pd.merge(
    actuals_df,
    photobooth_summary,
    left_on="BookingReference",
    right_on="Booking Ref",
).drop(columns=["Booking Ref", "Vehicle", "Customer", "Reg/Ref", "Origin"], errors="ignore")

#============================
#4. CALCULATE METRICS
#===========================


#1. Kiosk Transaction duration (seconds)

master_df["kiosk_duration_seconds"] = (master_df["CheckInEnded"] - master_df["CheckInStarted"]).dt.total_seconds()
kiosk_metrics = {
    "mean_sec": master_df["kiosk_duration_seconds"].mean(),
    "median_sec": master_df["kiosk_duration_seconds"].median(),
    "p50_sec": master_df["kiosk_duration_seconds"].quantile(0.5),
    "p90_sec": master_df["kiosk_duration_seconds"].quantile(0.9),
    "p95_sec": master_df["kiosk_duration_seconds"].quantile(0.95),
}

#Theoretical Max Kiosk Throughput (5 kiosks)
#Max cars/Hr = (5 kisosks * 3600 seconds/hr) / mean kiosk duration (seconds)
max_kiosk_hourly_throughput = (5 * 3600) / kiosk_metrics["mean_sec"]


#2. Ferry Dwell and Walk Times (Minutess between photobooth scan and kiosk check in started)

master_df["ferry_to_kiosk_dwell_mins"] = (master_df["CheckInStarted"] - master_df["customer_arrival_photobooth"]).dt.total_seconds() / 60.0

print("\nBefore filtering:")
print(master_df["ferry_to_kiosk_dwell_mins"].describe())
len_before = len(master_df)

master_df = master_df[
    master_df["ferry_to_kiosk_dwell_mins"].between(0,60)
]

print("\nAfter filtering:")
print(master_df["ferry_to_kiosk_dwell_mins"].describe())
print(
    f"Rows removed: "
    f"{len_before - len(master_df):,}"
)


dwell_metrics = {
    "mean_mins": master_df["ferry_to_kiosk_dwell_mins"].mean(),
    "median_mins": master_df["ferry_to_kiosk_dwell_mins"].median(),
    "p50_mins": master_df["ferry_to_kiosk_dwell_mins"].quantile(0.5),
    "p90_mins": master_df["ferry_to_kiosk_dwell_mins"].quantile(0.9),
    "p95_mins": master_df["ferry_to_kiosk_dwell_mins"].quantile(0.95),
}


# 3. Arrival Offset Distribution (difference between booked and actual arrivals)
master_df["arrival_offset_min"] = (master_df["customer_arrival_photobooth"] - master_df["ExpectedArrivalDate"]).dt.total_seconds() / 60.0
offset_metrics = {
    "mean_offset_min": master_df["arrival_offset_min"].mean(),
    "median_offset_min": master_df["arrival_offset_min"].median(),
    "p10_early_min": master_df["arrival_offset_min"].quantile(0.1), # early arrivals
    "p90_late_min": master_df["arrival_offset_min"].quantile(0.9), # late arrivals
}

print(f"--- KIOSK SERVICE BASELINE ---")
print(f"Average check-in time: {kiosk_metrics['mean_sec']:.1f}s")
print(f"90th Percentile check-in time: {kiosk_metrics['p90_sec']:.1f}s")
print(f"Max theoretical kiosk throughput (5 kiosks): {max_kiosk_hourly_throughput:.1f} cars/hr")

print(f"\n --- FERRY DWELL TIME BASELINE ---")
print(f"Average ferry dwell time: {dwell_metrics['mean_mins']:.1f} mins")

#best-case key deposit time (Staff return scan + 10 min walk/park/key allocator buffer)
master_df["best_case_key_deposit"] = master_df["staff_return_photobooth"] + pd.Timedelta(minutes=10)

#best-case locker dwell time in hours
master_df["best_case_locker_dwell_hours"] = (master_df["ActualCheckedOutDate"] - master_df["best_case_key_deposit"]).dt.total_seconds() / 3600.0

def run_photobooth_queue_analysis(
    df,
    photobooth_service_sec=37,
    number_of_photobooths=2
):
    """
    Simulates a first-come, first-served photobooth queue using
    exact customer arrival timestamps.

    Each customer:
    1. Arrives at the photobooth
    2. Waits if all photobooths are occupied
    3. Receives photobooth service
    4. Continues to the kiosk hall
    """

    if number_of_photobooths < 1:
        raise ValueError(
            "number_of_photobooths must be at least 1."
        )

    if photobooth_service_sec < 0:
        raise ValueError(
            "photobooth_service_sec cannot be negative."
        )

    queue_df = (
        df
        .dropna(subset=["customer_arrival_photobooth"])
        .copy()
        .sort_values("customer_arrival_photobooth")
        .reset_index(drop=True)
    )

    # Stores the next available time for every photobooth.
    photobooth_available_times = [
        queue_df["customer_arrival_photobooth"].min()
        for _ in range(number_of_photobooths)
    ]

    service_start_times = []
    service_end_times = []
    queue_wait_seconds = []
    allocated_photobooths = []

    for arrival_time in queue_df["customer_arrival_photobooth"]:

        # Select the photobooth that becomes available first.
        photobooth_number = min(
            range(number_of_photobooths),
            key=lambda booth: photobooth_available_times[booth]
        )

        next_available_time = (
            photobooth_available_times[photobooth_number]
        )

        service_start = max(
            arrival_time,
            next_available_time
        )

        service_end = (
            service_start
            + pd.Timedelta(seconds=photobooth_service_sec)
        )

        wait_seconds = (
            service_start - arrival_time
        ).total_seconds()

        service_start_times.append(service_start)
        service_end_times.append(service_end)
        queue_wait_seconds.append(wait_seconds)
        allocated_photobooths.append(photobooth_number + 1)

        photobooth_available_times[photobooth_number] = (
            service_end
        )

    queue_df["photobooth_number"] = allocated_photobooths

    queue_df["photobooth_service_start"] = (
        service_start_times
    )

    queue_df["photobooth_service_end"] = (
        service_end_times
    )

    queue_df["photobooth_queue_wait_seconds"] = (
        queue_wait_seconds
    )

    queue_df["photobooth_queue_wait_mins"] = (
        queue_df["photobooth_queue_wait_seconds"] / 60.0
    )

    return queue_df

def run_15min_capacity_analysis(df, photobooth_delay_sec=37):
    #Resample arrivals into 15-minute windows
    df['kiosk_hall_arrival'] = (df['customer_arrival_photobooth'] + pd.to_timedelta(photobooth_delay_sec, unit='s'))
                                
    df['arrival_bucket'] = df['kiosk_hall_arrival'].dt.floor('15min')

    #1. Aggregate Arrivals per bucket
    hourly_summary = df.groupby('arrival_bucket').size().to_frame(name='arrival_count').reset_index()

    #2. Service Rate per 15-minute bucket
    #Average items processed  per 15 mins = (5 kiosks * 900 seconds) / mean kiosk duration (seconds)
    avg_kiosk_sec = df["kiosk_duration_seconds"].mean()
    kiosk_capacity_per_15min = (5 * 900) / avg_kiosk_sec

    #3. Track Queue and Footprint
    queue = 0
    records = []

    for _, row in hourly_summary.iterrows():
        arrivals = row['arrival_count']

        #customers seeking kiosks in this bucket
        total_waiting = queue + arrivals

        #served in this bucket
        served = min(total_waiting, kiosk_capacity_per_15min)

        #remaining in queue for next bucket
        queue = total_waiting - served

        #Calculate Hall footprint (Assuming average 1.3m^2 per person in queue + at kiosk)
        ##using 1.3m^2 as an average between LH average of 1.5m^2 and SH average of 1.2m^2)

        total__customer_demand = total_waiting
        estimated_hall_area_m2 = total__customer_demand * 1.3


        records.append({
            'arrival_bucket': row['arrival_bucket'],
            'arrivals': arrivals,
            'queue_length': queue,
            'customer_demand': total__customer_demand,
            'hall_area_m2': estimated_hall_area_m2,
            'is_hall_overflowing': estimated_hall_area_m2 > 78.0,  # Assumption: 78m^2 is the max hall area
        })

    return pd.DataFrame(records)

baseline_capacity = run_15min_capacity_analysis(master_df, photobooth_delay_sec=37)

peak_overflows = baseline_capacity[baseline_capacity['is_hall_overflowing']]
print(f"\n--- KISOK HALL FOOTPRINT RISK ---")
print(F"Total 15-minute periods exceeding 78m^2 capacity: {len(peak_overflows)}")
print(f"Maximum customer demand in any 15-minute period: {baseline_capacity['customer_demand'].max()} people")
print(f"Max queue carried into next period: {baseline_capacity['queue_length'].max()} people")
print(f"Max physical area required: {baseline_capacity['hall_area_m2'].max():.1f} m^2")

def run_locker_occupancy_analysis(df):
    #Create 15-minute timeline covering the dataset
    start_time = df['best_case_key_deposit'].min().floor('15min')
    end_time = df['ActualCheckedOutDate'].max().ceil('15min')

    time_grid = pd.date_range(start=start_time, end=end_time, freq='15min')

    locker_records = []

    #Efficient vectorised balance calculation
    for current_time in time_grid:
        #Count keys deposited on/before current_time AND picked up after current_time
        occupied = df[
            (df['best_case_key_deposit'] <= current_time) &
            (df['ActualCheckedOutDate'] > current_time)
        ].shape[0]

        locker_records.append({
            'timestamp': current_time,
            'occupied_lockers': occupied,
            'available_lockers': 297 - occupied,  # Assuming 3 x 99 lockers total
            'is_over_capacity': occupied > 297
        })

    return pd.DataFrame(locker_records)

locker_df = run_locker_occupancy_analysis(master_df)

print(f"\n--- KEY LOCKER CAPACITY METRICS ---")
print(f"Peak Lockers Occupied (Best Case): {locker_df['occupied_lockers'].max()} / 297")
print(f"Min Available Locker Safety Margin: {locker_df['available_lockers'].min()}")
print(f"Total hours overflowing 297 lockers: {(locker_df['is_over_capacity'].sum() * 15)/60:.1f} hours")
    
#============================
#4. SIMULATE BARRIERLESS PHOTOBOOTHS
#===========================

def simulate_barrierless_impact(master_df):
    #1. Baseline: current Photobooth (37s per car)
    baseline_df = run_15min_capacity_analysis(master_df, photobooth_delay_sec=37)

    #2. Upgraded: Instant photobooth (0s per car)
    upgraded_df = run_15min_capacity_analysis(master_df, photobooth_delay_sec=0)

    #Comparison metrics
    comparison = {
        "Metric": [
            "Max Peak Queue (Customers)",
            "Max Hall Area Needed (m^2)",
            "Periods Exceeding 78m^2 Hall Capacity",
            "Avg Wait Time in Queue (mins)"
        ],
        "Current (37s Photobooth)": [
            baseline_df['queue_length'].max(),
            f"{baseline_df['hall_area_m2'].max():.1f} m^2",
            (baseline_df["hall_area_m2"] >78.).sum(),
            f"{baseline_df['queue_length'].mean() * (master_df['kiosk_duration_seconds'].mean()/5) / 60:.1f} min"
        ],
        "Upgraded (0s Photobooth)": [
            upgraded_df['queue_length'].max(),
            f"{upgraded_df['hall_area_m2'].max():.1f} m^2",
            (upgraded_df["hall_area_m2"] >78.).sum(),
            f"{upgraded_df['queue_length'].mean() * (master_df['kiosk_duration_seconds'].mean()/5) / 60:.1f} min"
        ]
    }

    return pd.DataFrame(comparison)

impact_results = simulate_barrierless_impact(master_df)
print(impact_results.to_string(index=False))

