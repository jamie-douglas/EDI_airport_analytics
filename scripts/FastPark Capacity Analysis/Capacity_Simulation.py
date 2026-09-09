import sys
import pathlib
from pathlib import Path
import numpy as np
import pandas as pd
import simpy

# Script directory setup
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

photobooth_path = SCRIPT_DIR / "Photobooth Inputs"
forecast_b_csv_path = r"C:\Users\jamie_douglas\Edinburgh Airport Limited\Shared Files - Business Planning\Seasonal Readiness\W26\2. Car Parking\Modelling\transaction_forecast.csv"

from modules.utils.db import get_engine

# =========================================================
# 1. DATA INGESTION & DYNAMIC DWELL CALCULATION
# =========================================================
dsn = 'AzureConnection'
user = 'jamie_douglas'
engine = get_engine(dsn=dsn, username=user)

# Load Photobooth Scans
photobooth_files = list(photobooth_path.glob("*.csv"))
photobooth_list = []
for file in photobooth_files:
    df = pd.read_csv(file)
    df["Scan Date"] = pd.to_datetime(df["Scan Date"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
    photobooth_list.append(df)

raw_scans = pd.concat(photobooth_list, ignore_index=True).drop_duplicates() if photobooth_list else pd.DataFrame()

if not raw_scans.empty:
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
else:
    photobooth_summary = pd.DataFrame(columns=["Booking Ref", "customer_arrival_photobooth", "staff_return_photobooth"])

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
    how="inner"
).drop(columns=["Booking Ref"], errors="ignore")

# Calculate Dynamic Baseline Metrics
master_df["kiosk_duration_seconds"] = (master_df["CheckInEnded"] - master_df["CheckInStarted"]).dt.total_seconds()
valid_kiosks = master_df[master_df["kiosk_duration_seconds"] > 0]["kiosk_duration_seconds"]
mean_kiosk_sec = float(valid_kiosks.mean()) if len(valid_kiosks) > 0 else 36.0

master_df["ferry_to_kiosk_dwell_mins"] = (master_df["CheckInStarted"] - master_df["customer_arrival_photobooth"]).dt.total_seconds() / 60.0
valid_dwells = master_df[master_df["ferry_to_kiosk_dwell_mins"].between(0, 60)]["ferry_to_kiosk_dwell_mins"]
dynamic_dwell_mins = float(valid_dwells.median()) if len(valid_dwells) > 0 else 5.0

print("=" * 65)
print("📌 DYNAMIC HISTORICAL METRICS CALCULATED")
print("=" * 65)
print(f"Mean Kiosk Transaction Time: {mean_kiosk_sec:.1f} seconds")
print(f"Dynamic Park & Walk Dwell Lag: {dynamic_dwell_mins:.1f} minutes\n")


# =========================================================
# 2. PROFILING & FORECAST DISAGGREGATION
# =========================================================
historic_arrivals = pd.DataFrame({"arrival_time": actuals_df["CheckInStarted"].dropna()}).sort_values("arrival_time").reset_index(drop=True)
historic_exits = pd.DataFrame({"exit_time": actuals_df["ActualCheckedOutDate"].dropna()}).sort_values("exit_time").reset_index(drop=True)

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

def build_nested_profiles(history_df, time_col):
    working = history_df.dropna(subset=[time_col]).copy()
    working["month"] = working[time_col].dt.to_period("M")
    working["day_of_month"] = working[time_col].dt.day
    working["weekday"] = working[time_col].dt.dayofweek
    working["hour"] = working[time_col].dt.hour
    working["min15"] = (working[time_col].dt.minute // 15) * 15

    dom_counts = working.groupby(["month", "day_of_month"]).size().reset_index(name="count")
    dom_monthly_totals = dom_counts.groupby("month")["count"].transform("sum")
    dom_counts["dom_prob"] = dom_counts["count"] / dom_monthly_totals
    dom_profile = dom_counts.groupby("day_of_month")["dom_prob"].mean().reset_index()
    dom_profile["dom_prob"] /= dom_profile["dom_prob"].sum()

    m15_profile = working.groupby(["weekday", "hour", "min15"]).size().reset_index(name="count")
    dow_totals = m15_profile.groupby("weekday")["count"].transform("sum")
    m15_profile["m15_prob"] = m15_profile["count"] / dow_totals

    return dom_profile, m15_profile

def disaggregate_volume_to_1min(total_volume, day_date, m15_profile):
    weekday = day_date.dayofweek
    dow_p = m15_profile[m15_profile["weekday"] == weekday].copy()
    
    if dow_p.empty:
        dow_p = m15_profile.groupby(["hour", "min15"])["m15_prob"].mean().reset_index()

    dow_p["w"] = dow_p["m15_prob"] / dow_p["m15_prob"].sum()
    raw_counts = total_volume * dow_p["w"]
    int_counts = np.floor(raw_counts).astype(int)
    remainder = int(total_volume - int_counts.sum())
    
    if remainder > 0:
        top_indices = np.argsort((raw_counts - int_counts).values)[::-1][:remainder]
        int_counts.iloc[top_indices] += 1

    records = []
    for (_, row), cnt in zip(dow_p.iterrows(), int_counts):
        if cnt > 0:
            block_start = day_date + pd.Timedelta(hours=int(row["hour"]), minutes=int(row["min15"]))
            minute_offsets = np.random.randint(0, 15, size=cnt)
            records.extend([block_start + pd.Timedelta(minutes=int(m)) for m in minute_offsets])

    return records

def build_forecast_B(csv_path, history_df):
    if str(csv_path).endswith(('.xlsx', '.xls')):
        monthly_df = pd.read_excel(csv_path)
    else:
        monthly_df = pd.read_csv(csv_path)

    monthly_df["Month"] = pd.to_datetime(monthly_df["Month"], format="%y-%b")
    
    dom_arr_prof, m15_arr_prof = build_nested_profiles(history_df, "CheckInStarted")
    dom_ext_prof, m15_ext_prof = build_nested_profiles(history_df, "ActualCheckedOutDate")

    arr_records, ext_records = [], []

    for _, row in monthly_df.iterrows():
        total_tx = int(row["Transactions"])
        m_start = row["Month"].replace(day=1)
        days_in_month = pd.date_range(m_start, m_start + pd.offsets.MonthEnd(0), freq="D")
        num_days = len(days_in_month)

        valid_dom_arr = dom_arr_prof[dom_arr_prof["day_of_month"] <= num_days].copy()
        valid_dom_arr["w"] = valid_dom_arr["dom_prob"] / valid_dom_arr["dom_prob"].sum()
        daily_arr_volumes = np.floor(total_tx * valid_dom_arr["w"]).astype(int)

        valid_dom_ext = dom_ext_prof[dom_ext_prof["day_of_month"] <= num_days].copy()
        valid_dom_ext["w"] = valid_dom_ext["dom_prob"] / valid_dom_ext["dom_prob"].sum()
        daily_ext_volumes = np.floor(total_tx * valid_dom_ext["w"]).astype(int)

        for idx, day in enumerate(days_in_month):
            v_arr = daily_arr_volumes.iloc[idx] if idx < len(daily_arr_volumes) else 0
            v_ext = daily_ext_volumes.iloc[idx] if idx < len(daily_ext_volumes) else 0

            if v_arr > 0:
                arr_records.extend(disaggregate_volume_to_1min(v_arr, day, m15_arr_prof))
            if v_ext > 0:
                ext_records.extend(disaggregate_volume_to_1min(v_ext, day, m15_ext_prof))

    df_arr = pd.DataFrame({"arrival_time": arr_records}).sort_values("arrival_time").reset_index(drop=True)
    df_ext = pd.DataFrame({"exit_time": ext_records}).sort_values("exit_time").reset_index(drop=True)

    return df_arr, df_ext

print("Loading Historical Actuals...")
print("Disaggregating Forecast A (Short-Term Hourly View via SQL)...")
future_arrivals_A, future_exits_A = build_forecast_A(engine, arrival_profile, exit_profile)

print("Disaggregating Forecast B (Long-Term Monthly File)...")
# FIX: Pass full_year_history_df instead of profile DataFrames
future_arrivals_B, future_exits_B = build_forecast_B(forecast_b_csv_path, full_year_history_df)

analysis_datasets = [
    ("Historical Actuals (Peak Periods)", historic_arrivals, historic_exits),
    ("Forecast A (Short-Term 2-Month Daily)", future_arrivals_A, future_exits_A),
    ("Forecast B (Long-Term Monthly)", future_arrivals_B, future_exits_B)
]


# =========================================================
# 3. SIMPY PIPELINE ENGINE
# =========================================================
def customer_process(env, photobooths, kiosks, pb_service_mean, kiosk_service_mean, dwell_sec, logs):
    if pb_service_mean > 0:
        logs['pb_queue'].append(len(photobooths.queue))
        with photobooths.request() as req:
            yield req
            st = max(1.0, np.random.normal(pb_service_mean, 4.0))
            yield env.timeout(st)
    else:
        logs['pb_queue'].append(0)

    actual_dwell = max(30.0, np.random.normal(dwell_sec, 60.0))
    yield env.timeout(actual_dwell)

    current_hall_pop = len(kiosks.queue) + kiosks.count
    logs['hall_pop'].append(current_hall_pop)
    logs['kiosk_queue'].append(len(kiosks.queue))

    with kiosks.request() as req:
        yield req
        k_st = max(5.0, np.random.normal(kiosk_service_mean, 8.0))
        yield env.timeout(k_st)

def staff_process(env, photobooths, pb_service_mean, logs):
    if pb_service_mean > 0:
        logs['pb_queue'].append(len(photobooths.queue))
        with photobooths.request() as req:
            yield req
            st = max(1.0, np.random.normal(pb_service_mean, 3.0))
            yield env.timeout(st)

def run_pipeline_day(env, events_df, photobooths, kiosks, pb_cust_sec, kiosk_sec, dwell_sec, logs):
    previous_time = 0.0
    for _, row in events_df.iterrows():
        time_until_arrival = row['sim_seconds'] - previous_time
        if time_until_arrival > 0:
            yield env.timeout(time_until_arrival)
            
        if row['is_staff']:
            env.process(staff_process(env, photobooths, row['service_sec'], logs))
        else:
            env.process(customer_process(env, photobooths, kiosks, row['service_sec'], kiosk_sec, dwell_sec, logs))
            
        previous_time = row['sim_seconds']

def run_monte_carlo_iteration(arrivals_df, exits_df, pb_cust_sec=37.0, pb_capacity=2, kiosk_sec=36.0, num_kiosks=5, dwell_mins=5.0):
    df_cust = arrivals_df.copy()
    df_cust.rename(columns={'arrival_time': 'timestamp'}, inplace=True)
    df_cust['service_sec'] = pb_cust_sec
    df_cust['is_staff'] = False

    df_staff = exits_df.copy()
    df_staff.rename(columns={'exit_time': 'timestamp'}, inplace=True)
    df_staff['service_sec'] = 12.0 if pb_cust_sec > 0 else 0.0
    df_staff['is_staff'] = True
    
    offsets = np.random.randint(60, 120, size=len(df_staff))
    # FIX: Updated unit='m' to unit='min'
    df_staff['timestamp'] = df_staff['timestamp'] - pd.to_timedelta(offsets, unit='min')

    sim_events = pd.concat([df_cust, df_staff]).sort_values('timestamp').reset_index(drop=True)
    start_time = sim_events['timestamp'].min()
    sim_events['sim_seconds'] = (sim_events['timestamp'] - start_time).dt.total_seconds()

    env = simpy.Environment()
    photobooths = simpy.Resource(env, capacity=pb_capacity)
    kiosks = simpy.Resource(env, capacity=num_kiosks)
    logs = {'pb_queue': [], 'kiosk_queue': [], 'hall_pop': []}
    
    env.process(run_pipeline_day(env, sim_events, photobooths, kiosks, pb_cust_sec, kiosk_sec, dwell_mins * 60.0, logs))
    env.run()

    max_pb_q = max(logs['pb_queue']) if logs['pb_queue'] else 0
    max_hall_pop = max(logs['hall_pop']) if logs['hall_pop'] else 0

    return {'pb_jam': max_pb_q >= 10, 'max_pb_q': max_pb_q, 'max_hall_pop': max_hall_pop, 'hall_breach': max_hall_pop > 60}


# =========================================================
# 4. PHOTOBOOTH QUEUE & TRAFFIC IMPACT ANALYSIS
# =========================================================
def analyze_photobooth_queues(forecast_list, kiosk_sec, dwell_mins, iterations=100):
    for label, arr_df, ext_df in forecast_list:
        print("\n" + "=" * 70)
        print(f"🚘 PHOTOBOOTH QUEUE & TRAFFIC NETWORK RISK: {label}")
        print("=" * 70)

        configs = [
            ("Current Photobooths (2 Lanes - Barriered 37s)", 37.0, 2),
            ("New Single Photobooth (1 Lane - 5s Clearance Headway)", 5.0, 1)
        ]

        for p_label, p_sec, p_cap in configs:
            runs = [
                run_monte_carlo_iteration(
                    arr_df, ext_df, 
                    pb_cust_sec=p_sec, 
                    pb_capacity=p_cap, 
                    kiosk_sec=kiosk_sec, 
                    num_kiosks=5, 
                    dwell_mins=dwell_mins
                ) for _ in range(iterations)
            ]
            jam_risk = np.mean([r['pb_jam'] for r in runs]) * 100
            avg_pb_q = np.mean([r['max_pb_q'] for r in runs])
            avg_hall = np.mean([r['max_hall_pop'] for r in runs])
            hall_breach_risk = np.mean([r['hall_breach'] for r in runs]) * 100

            print(f"  [{p_label}]")
            print(f"    • Peak Photobooth Queue : {avg_pb_q:.1f} cars")
            print(f"    • Road Network Jam Risk  : {jam_risk:.1f}% (Queue >= 10 cars)")
            print(f"    • Downstream Hall Peak   : {avg_hall:.1f} people | Hall Breach Risk (>60): {hall_breach_risk:.1f}%\n")


# =========================================================
# 5. KIOSK MITIGATION SOLVER (ZERO-BARRIER SETUP)
# =========================================================
def run_dual_kiosk_solver(forecast_list, base_kiosk_sec, dwell_mins, iterations=100, hall_capacity=60):
    for label, arr_df, ext_df in forecast_list:
        print("\n" + "=" * 70)
        print(f"🛠️ KIOSK HALL MITIGATION SOLVER: {label}")
        print("=" * 70)

        base_runs = [
            run_monte_carlo_iteration(
                arr_df, ext_df, 
                pb_cust_sec=5.0, 
                pb_capacity=1, 
                kiosk_sec=base_kiosk_sec, 
                num_kiosks=5, 
                dwell_mins=dwell_mins
            ) for _ in range(iterations)
        ]
        
        base_breach_risk = np.mean([r['hall_breach'] for r in base_runs]) * 100
        base_avg_hall = np.mean([r['max_hall_pop'] for r in base_runs])

        print(f"  [Baseline Check: 5 Kiosks @ {base_kiosk_sec:.1f}s]")
        print(f"    • Peak Hall Pop : {base_avg_hall:.1f} / {hall_capacity} people")
        print(f"    • Hall Breach Risk: {base_breach_risk:.1f}%")

        if base_breach_risk == 0:
            print(f"  ✅ NO BREACH DETECTED: System operates within capacity limit ({hall_capacity} people). Mitigation solver skipped.\n")
            continue

        print(f"\n  ⚠️ CAPACITY BREACH DETECTED ({base_breach_risk:.1f}% Risk): Evaluating Mitigations...\n")

        # Option A: Hardware Scaling
        print("Option 1: Add Kiosk Hardware (holding average speed at 36s)...")
        required_kiosks = 5
        for k_count in range(6, 12):
            runs = [
                run_monte_carlo_iteration(
                    arr_df, ext_df, 
                    pb_cust_sec=5.0, 
                    pb_capacity=1, 
                    kiosk_sec=base_kiosk_sec, 
                    num_kiosks=k_count, 
                    dwell_mins=dwell_mins
                ) for _ in range(iterations)
            ]
            breach_risk = np.mean([r['hall_breach'] for r in runs]) * 100
            avg_hall = np.mean([r['max_hall_pop'] for r in runs])
            print(f"  • {k_count} Kiosks @ {base_kiosk_sec:.1f}s: Peak Hall = {avg_hall:.1f} people | Hall Breach Risk = {breach_risk:.1f}%")
            if breach_risk == 0 and required_kiosks == 5:
                required_kiosks = k_count
                break

        # Option B: Process Speed Optimization
        print("\nOption 2: Optimize Transaction Speed (holding hardware at 5 Kiosks)...")
        target_speed = base_kiosk_sec
        for test_sec in range(int(base_kiosk_sec) - 2, 5, -2):
            runs = [
                run_monte_carlo_iteration(
                    arr_df, ext_df, 
                    pb_cust_sec=5.0, 
                    pb_capacity=1, 
                    kiosk_sec=test_sec, 
                    num_kiosks=5, 
                    dwell_mins=dwell_mins
                ) for _ in range(iterations)
            ]
            breach_risk = np.mean([r['hall_breach'] for r in runs]) * 100
            avg_hall = np.mean([r['max_hall_pop'] for r in runs])
            print(f"  • 5 Kiosks @ {test_sec}s avg: Peak Hall = {avg_hall:.1f} people | Hall Breach Risk = {breach_risk:.1f}%")
            if breach_risk == 0:
                target_speed = test_sec
                break

        print("\n" + "-" * 70)
        print(f"💡 MITIGATION SUMMARY FOR {label}:")
        print(f"    1. Hardware Solution : Increase kiosks from 5 to {required_kiosks}.")
        print(f"    2. Process Solution  : Reduce check-in duration from {base_kiosk_sec:.1f}s to {target_speed}s.")
        print("-" * 70)


# =========================================================
# 6. KEY LOCKER SENSITIVITY ANALYSIS
# =========================================================
def analyze_dual_key_locker_sensitivity(forecast_list, lead_time_options=[15, 30, 45, 60, 120]):
    for label, _, ext_df in forecast_list:
        print("\n" + "=" * 70)
        print(f"🔑 KEY LOCKER CAPACITY SENSITIVITY: {label}")
        print("=" * 70)

        results = []
        for lead in lead_time_options:
            future = ext_df.copy()
            future["locker_start"] = future["exit_time"] - pd.Timedelta(minutes=lead)
            future["locker_end"] = future["exit_time"]

            time_grid = pd.date_range(start=future["locker_start"].min().floor("1min"), end=future["locker_end"].max().ceil("1min"), freq="1min")
            starts, ends = np.sort(future["locker_start"].values), np.sort(future["locker_end"].values)
            occupied = np.searchsorted(starts, time_grid.values, side='right') - np.searchsorted(ends, time_grid.values, side='right')

            res = pd.DataFrame({"time_bucket": time_grid, "occupied": occupied})
            peak_lockers = res["occupied"].max()
            hours_over = (res["occupied"] > 297).sum() / 60.0

            results.append({
                "Staff Lead Time": f"{lead} Mins Before Return",
                "Peak Lockers Occupied": peak_lockers,
                "Hours Over 297 Cap": f"{hours_over:.1f} hrs",
                "Capacity Status": "OVER CAP" if peak_lockers > 297 else "OK"
            })

        print(pd.DataFrame(results).to_string(index=False))
        print("=" * 70)


# =========================================================
# 7. SCRIPT EXECUTION
# =========================================================
if __name__ == "__main__":
    # Lower iteration count for quick testing, increase to 100 for production
    ITERATIONS = 10 
    
    analyze_photobooth_queues(analysis_datasets, mean_kiosk_sec, dynamic_dwell_mins, iterations=ITERATIONS)
    run_dual_kiosk_solver(analysis_datasets, mean_kiosk_sec, dynamic_dwell_mins, iterations=ITERATIONS)
    analyze_dual_key_locker_sensitivity(analysis_datasets)
