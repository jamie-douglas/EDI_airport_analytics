import sys
import os
import pathlib
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import pandas as pd
import simpy
from functools import lru_cache

# Script directory setup
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

PHOTOBOOTH_PATH = SCRIPT_DIR / "Photobooth Inputs"
FORECAST_B_CSV_PATH = r"C:\Users\jamie_douglas\OneDrive - Edinburgh Airport Limited\Documents\transaction_forecast.xlsx"

from modules.utils.db import get_engine

# =========================================================
# 1. DATA INGESTION & METRICS
# =========================================================
def load_and_calculate_metrics():
    dsn = 'AzureConnection'
    user = 'jamie_douglas'
    engine = get_engine(dsn=dsn, username=user)

    # Load Photobooth Scans
    photobooth_files = list(PHOTOBOOTH_PATH.glob("*.csv")) if PHOTOBOOTH_PATH.exists() else []
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
    actuals_df["ExpectedReturnDate"] = pd.to_datetime(actuals_df["ExpectedReturnDate"])
    actuals_df["ActualCheckedOutDate"] = pd.to_datetime(actuals_df["ActualCheckedOutDate"])

    master_df = pd.merge(
        actuals_df,
        photobooth_summary,
        left_on="BookingReference",
        right_on="Booking Ref",
        how="inner"
    ).drop(columns=["Booking Ref"], errors="ignore")

    # Dynamic Metrics
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

    return engine, actuals_df, mean_kiosk_sec, dynamic_dwell_mins


# =========================================================
# 2. PROFILING & SIMULATION HELPERS
# =========================================================
@lru_cache(maxsize=1)
def load_full_year_profile_data_cached(dsn_name, username):
    sql = """
    SELECT "BookingReference", "CheckInStarted", "ActualCheckedOutDate"
    FROM FastPark.v_EntryAndExits
    WHERE "CheckInStarted" IS NOT NULL AND "CheckInStarted" >= DATEADD(year,-1,GETDATE())
    """
    eng = get_engine(dsn=dsn_name, username=username)
    df = pd.read_sql(sql, con=eng)
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
            
            # POISSON / EXPONENTIAL ARRIVALS: Simulates real burstiness instead of uniform spacing
            # Mean inter-arrival time in seconds for this 15-min block
            mean_inter_arrival = (15 * 60) / cnt
            inter_arrivals = np.random.exponential(scale=mean_inter_arrival, size=cnt)
            arrival_offsets_sec = np.cumsum(inter_arrivals)
            
            # Scale to fit within the 15-minute (900s) window
            if arrival_offsets_sec[-1] > 0:
                arrival_offsets_sec = (arrival_offsets_sec / arrival_offsets_sec[-1]) * 899.0

            for sec_offset in arrival_offsets_sec:
                records.append(block_start + pd.Timedelta(seconds=float(sec_offset)))

    return records

def build_forecast_B(csv_path, history_df):
    if not Path(csv_path).exists():
        return pd.DataFrame(columns=["arrival_time"]), pd.DataFrame(columns=["exit_time"])

    monthly_df = pd.read_excel(csv_path) if str(csv_path).endswith(('.xlsx', '.xls')) else pd.read_csv(csv_path)
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
        rem_arr = int(total_tx - daily_arr_volumes.sum())
        if rem_arr > 0:
            daily_arr_volumes.iloc[np.argsort((total_tx * valid_dom_arr["w"] - daily_arr_volumes).values)[::-1][:rem_arr]] += 1

        valid_dom_ext = dom_ext_prof[dom_ext_prof["day_of_month"] <= num_days].copy()
        valid_dom_ext["w"] = valid_dom_ext["dom_prob"] / valid_dom_ext["dom_prob"].sum()
        daily_ext_volumes = np.floor(total_tx * valid_dom_ext["w"]).astype(int)
        rem_ext = int(total_tx - daily_ext_volumes.sum())
        if rem_ext > 0:
            daily_ext_volumes.iloc[np.argsort((total_tx * valid_dom_ext["w"] - daily_ext_volumes).values)[::-1][:rem_ext]] += 1

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

def get_seasonal_multiplier(dt):
    month, day = dt.month, dt.day
    if month in [6, 7, 8] or (month == 12 and day >= 15) or (month == 1 and day <= 5):
        return 1.5
    elif month in [11, 1, 2, 3]:
        return 1.2
    return 1.35

def _mc_worker_unpack(args_tuple):
    return run_monte_carlo_iteration(*args_tuple)

def run_monte_carlo_parallel(params_tuple, iterations=10, max_workers=None):
    if max_workers is None:
        max_workers = max(1, (os.cpu_count() or 2) - 1)

    args_list = [params_tuple for _ in range(iterations)]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_mc_worker_unpack, args_list))

    return results

def customer_process(env, photobooths, kiosks, pb_st, kiosk_st, dwell_sec, logs, arrival_dt, start_time, metrics_state, full_logs):
    curr_time = start_time + pd.Timedelta(seconds=env.now)
    
    if pb_st > 0:
        curr_pb_q = len(photobooths.queue)
        if curr_pb_q > metrics_state['max_pb_q']:
            metrics_state['max_pb_q'] = curr_pb_q
        if full_logs and logs is not None:
            logs.append({'timestamp': curr_time, 'pb_queue': curr_pb_q, 'hall_pop': 0})
            
        with photobooths.request() as req:
            yield req
            yield env.timeout(pb_st)

    yield env.timeout(dwell_sec)

    curr_time = start_time + pd.Timedelta(seconds=env.now)
    multiplier = get_seasonal_multiplier(arrival_dt)
    vehicles_in_hall = len(kiosks.queue) + kiosks.count + 1
    current_hall_pop = int(vehicles_in_hall * multiplier)
    
    curr_pb_q = len(photobooths.queue)
    if curr_pb_q > metrics_state['max_pb_q']:
        metrics_state['max_pb_q'] = curr_pb_q
    if current_hall_pop > metrics_state['max_hall_pop']:
        metrics_state['max_hall_pop'] = current_hall_pop
    if current_hall_pop > 60:
        metrics_state['breach_count'] += 1

    if full_logs and logs is not None:
        logs.append({'timestamp': curr_time, 'pb_queue': curr_pb_q, 'hall_pop': current_hall_pop})

    with kiosks.request() as req:
        yield req
        yield env.timeout(kiosk_st)

def staff_process(env, photobooths, pb_st, logs, start_time, metrics_state, full_logs):
    if pb_st > 0:
        curr_time = start_time + pd.Timedelta(seconds=env.now)
        curr_pb_q = len(photobooths.queue)
        if curr_pb_q > metrics_state['max_pb_q']:
            metrics_state['max_pb_q'] = curr_pb_q
        if full_logs and logs is not None:
            logs.append({'timestamp': curr_time, 'pb_queue': curr_pb_q, 'hall_pop': 0})
        with photobooths.request() as req:
            yield req
            yield env.timeout(pb_st)

def run_pipeline_day(env, events_df, photobooths, kiosks, logs, start_time, metrics_state, full_logs):
    previous_time = 0.0
    for _, row in events_df.iterrows():
        time_until_arrival = row['sim_seconds'] - previous_time
        if time_until_arrival > 0:
            yield env.timeout(time_until_arrival)
            
        if row['is_staff']:
            env.process(staff_process(env, photobooths, row['pb_st'], logs, start_time, metrics_state, full_logs))
        else:
            env.process(customer_process(env, photobooths, kiosks, row['pb_st'], row['kiosk_st'], row['dwell_sec'], logs, row['timestamp'], start_time, metrics_state, full_logs))
            
        previous_time = row['sim_seconds']

def run_monte_carlo_iteration(
    arr_df, ext_df, pb_cust_sec=37.0, pb_capacity=2, kiosk_sec=36.0, num_kiosks=5, dwell_mins=5.0, full_logs=False
):
    df_cust = arr_df.copy().rename(columns={'arrival_time': 'timestamp'})
    df_cust['service_sec'] = pb_cust_sec
    df_cust['is_staff'] = False
    
    n_cust = len(df_cust)
    df_cust['pb_st'] = np.maximum(1.0, np.random.normal(pb_cust_sec, 4.0, size=n_cust)) if pb_cust_sec > 0 else 0.0
    df_cust['dwell_sec'] = np.maximum(30.0, np.random.normal(dwell_mins * 60.0, 60.0, size=n_cust))
    df_cust['kiosk_st'] = np.maximum(5.0, np.random.normal(kiosk_sec, 8.0, size=n_cust))

    df_staff = ext_df.copy().rename(columns={'exit_time': 'timestamp'})
    df_staff['service_sec'] = 12.0 if pb_cust_sec > 0 else 0.0
    df_staff['is_staff'] = True
    
    n_staff = len(df_staff)
    df_staff['pb_st'] = np.maximum(1.0, np.random.normal(12.0 if pb_cust_sec > 0 else 0.0, 3.0, size=n_staff)) if pb_cust_sec > 0 else 0.0
    df_staff['dwell_sec'] = 0.0
    df_staff['kiosk_st'] = 0.0
    
    offsets = np.random.randint(60, 120, size=len(df_staff))
    df_staff['timestamp'] = df_staff['timestamp'] - pd.to_timedelta(offsets, unit='min')

    sim_events = pd.concat([df_cust, df_staff]).sort_values('timestamp').reset_index(drop=True)
    if sim_events.empty:
        return {'pb_jam': False, 'max_pb_q': 0, 'max_hall_pop': 0, 'hall_breach': False, 'breach_count': 0, 'logs': []}

    start_time = sim_events['timestamp'].min()
    sim_events['sim_seconds'] = (sim_events['timestamp'] - start_time).dt.total_seconds()

    env = simpy.Environment()
    photobooths = simpy.Resource(env, capacity=pb_capacity)
    kiosks = simpy.Resource(env, capacity=num_kiosks)
    
    logs = [] if full_logs else None
    metrics_state = {'max_pb_q': 0, 'max_hall_pop': 0, 'breach_count': 0}
    
    env.process(run_pipeline_day(env, sim_events, photobooths, kiosks, logs, start_time, metrics_state, full_logs))
    env.run()

    max_pb_q = metrics_state['max_pb_q']
    max_hall_pop = metrics_state['max_hall_pop']
    breach_count = metrics_state['breach_count']

    return {
        'pb_jam': max_pb_q >= 10,
        'max_pb_q': max_pb_q,
        'max_hall_pop': max_hall_pop,
        'hall_breach': max_hall_pop > 60,
        'breach_count': breach_count,
        'logs': logs if full_logs else []
    }

def solve_min_kiosks(
    arr_df, ext_df, p_sec, p_cap, kiosk_sec, dwell_mins,
    min_kiosks=4, max_kiosks=16, iterations=10, max_allowed_breaches=0
):
    low = min_kiosks
    high = max_kiosks
    best_k = max_kiosks

    while low <= high:
        mid_kiosks = (low + high) // 2
        params = (arr_df, ext_df, p_sec, p_cap, kiosk_sec, mid_kiosks, dwell_mins, False)

        total_breaches = 0
        failed = False

        for i in range(iterations):
            res = run_monte_carlo_iteration(*params)
            total_breaches += res.get('breach_count', 1 if res['hall_breach'] else 0)

            if total_breaches > max_allowed_breaches:
                failed = True
                break

        if not failed:
            best_k = mid_kiosks
            high = mid_kiosks - 1
        else:
            low = mid_kiosks + 1

    return best_k

def analyze_photobooth_queues(forecast_list, kiosk_sec, dwell_mins, iterations=10):
    for label, arr_df, ext_df in forecast_list:
        print("\n" + "=" * 70)
        print(f"🚘 PHOTOBOOTH QUEUE & TRAFFIC NETWORK RISK: {label}")
        print("=" * 70)
        configs = [
            ("Current Photobooths (2 Lanes - Barriered 37s)", 37.0, 2),
            ("New Single Photobooth (1 Lane - 5s Clearance Headway)", 5.0, 1)
        ]
        for p_label, p_sec, p_cap in configs:
            params = (arr_df, ext_df, p_sec, p_cap, kiosk_sec, 5, dwell_mins, False)
            runs = run_monte_carlo_parallel(params, iterations=iterations)
            
            jam_risk = np.mean([r['pb_jam'] for r in runs]) * 100
            avg_pb_q = np.mean([r['max_pb_q'] for r in runs])
            avg_hall = np.mean([r['max_hall_pop'] for r in runs])
            hall_breach_risk = np.mean([r['hall_breach'] for r in runs]) * 100
            print(f"  [{p_label}]")
            print(f"    • Peak Photobooth Queue : {avg_pb_q:.1f} cars")
            print(f"    • Road Network Jam Risk  : {jam_risk:.1f}% (Queue >= 10 cars)")
            print(f"    • Downstream Hall Peak   : {avg_hall:.1f} people | Hall Breach Risk (>60): {hall_breach_risk:.1f}%\n")

def run_dual_kiosk_solver(forecast_list, base_kiosk_sec, dwell_mins, iterations=10, hall_capacity=60):
    for label, arr_df, ext_df in forecast_list:
        print("\n" + "=" * 70)
        print(f"🛠️ KIOSK HALL MITIGATION SOLVER: {label}")
        print("=" * 70)
        
        base_params = (arr_df, ext_df, 5.0, 1, base_kiosk_sec, 5, dwell_mins, False)
        base_runs = run_monte_carlo_parallel(base_params, iterations=iterations)
        base_breach_risk = np.mean([r['hall_breach'] for r in base_runs]) * 100
        base_avg_hall = np.mean([r['max_hall_pop'] for r in base_runs])

        print(f"  [Baseline Check: 5 Kiosks @ {base_kiosk_sec:.1f}s]")
        print(f"    • Peak Hall Pop : {base_avg_hall:.1f} / {hall_capacity} people")
        print(f"    • Hall Breach Risk: {base_breach_risk:.1f}%")

        if base_breach_risk == 0:
            print(f"  ✅ NO BREACH DETECTED: System operates within capacity limit ({hall_capacity} people). Mitigation solver skipped.\n")
            continue

        print(f"\n  ⚠️ CAPACITY BREACH DETECTED ({base_breach_risk:.1f}% Risk): Evaluating Mitigations...\n")
        
        print("Option 1: Add Kiosk Hardware (holding average speed at 36s)...")
        required_kiosks = solve_min_kiosks(
            arr_df, ext_df, p_sec=5.0, p_cap=1, kiosk_sec=base_kiosk_sec, 
            dwell_mins=dwell_mins, min_kiosks=6, max_kiosks=15, iterations=iterations, max_allowed_breaches=0
        )
        print(f"  • Required Hardware: Increase kiosks from 5 to {required_kiosks}.")

        print("\nOption 2: Optimize Transaction Speed (holding hardware at 5 Kiosks)...")
        target_speed = base_kiosk_sec
        for test_sec in range(int(base_kiosk_sec) - 2, 5, -2):
            params = (arr_df, ext_df, 5.0, 1, test_sec, 5, dwell_mins, False)
            runs = run_monte_carlo_parallel(params, iterations=iterations)
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

@lru_cache(maxsize=32)
def _cached_return_delay_distribution(exp_ret_tuple, act_chk_tuple):
    exp_ret = pd.to_datetime(np.array(exp_ret_tuple))
    act_chk = pd.to_datetime(np.array(act_chk_tuple))
    delays = (act_chk - exp_ret).total_seconds() / 60.0
    return np.clip(delays, -720, 2880)

def extract_return_delay_distribution(actuals_df):
    valid_subset = actuals_df.dropna(subset=['ExpectedReturnDate', 'ActualCheckedOutDate'])
    exp_ret_tuple = tuple(valid_subset['ExpectedReturnDate'].astype('int64').values)
    act_chk_tuple = tuple(valid_subset['ActualCheckedOutDate'].astype('int64').values)
    return _cached_return_delay_distribution(exp_ret_tuple, act_chk_tuple)

@lru_cache(maxsize=128)
def _cached_key_locker_occupancy(ext_int_tuple, delays_tuple, lead_time_mins):
    if not ext_int_tuple:
        return pd.DataFrame()
        
    df = pd.DataFrame({'exit_time': pd.to_datetime(np.array(ext_int_tuple))})
    delay_distribution = np.array(delays_tuple)
    
    sampled_delays = np.random.choice(delay_distribution, size=len(df)) if len(delay_distribution) > 0 else 0
    
    df['locker_start'] = df['exit_time'] - pd.Timedelta(minutes=lead_time_mins)
    df['locker_end'] = df['exit_time'] + pd.to_timedelta(sampled_delays, unit='min')
    df['locker_end'] = np.maximum(df['locker_start'] + pd.Timedelta(minutes=15), df['locker_end'])

    time_grid = pd.date_range(
        start=df['locker_start'].min().floor('1min'),
        end=df['locker_end'].max().ceil('1min'),
        freq='1min'
    )
    starts, ends = np.sort(df['locker_start'].values), np.sort(df['locker_end'].values)
    occupied = np.searchsorted(starts, time_grid.values, side='right') - np.searchsorted(ends, time_grid.values, side='right')

    return pd.DataFrame({'timestamp': time_grid, 'lockers_occupied': occupied}).set_index('timestamp')

def calculate_1min_key_locker_occupancy(ext_df, delay_distribution, lead_time_mins=45):
    if ext_df.empty:
        return pd.DataFrame()
    ext_int_tuple = tuple(ext_df['exit_time'].astype('int64').values)
    delays_tuple = tuple(delay_distribution)
    return _cached_key_locker_occupancy(ext_int_tuple, delays_tuple, lead_time_mins)

def analyze_dual_key_locker_sensitivity(forecast_list, actuals_df, lead_time_options=[15, 30, 45, 60, 120], capacity_limit=297):
    delays = extract_return_delay_distribution(actuals_df)
    
    for label, _, ext_df in forecast_list:
        if ext_df.empty:
            continue

        print("\n" + "=" * 70)
        print(f"🔑 KEY LOCKER CAPACITY SENSITIVITY: {label}")
        print("=" * 70)

        results = []
        for lead in lead_time_options:
            res = calculate_1min_key_locker_occupancy(ext_df, delays, lead_time_mins=lead)
            if res.empty:
                continue
                
            peak_lockers = res["lockers_occupied"].max()
            hours_over = (res["lockers_occupied"] > capacity_limit).sum() / 60.0

            results.append({
                "Staff Lead Time": f"{lead} Mins Before Return",
                "Peak Lockers Occupied": peak_lockers,
                "Hours Over Cap": f"{hours_over:.1f} hrs",
                "Capacity Status": "OVER CAP" if peak_lockers > capacity_limit else "OK"
            })

        print(pd.DataFrame(results).to_string(index=False))
        print("=" * 70)

def generate_monthly_breach_report(
    forecast_label, arr_df, ext_df, sim_logs, actuals_df, 
    lead_time_mins=45, pb_cust_sec=37.0, pb_capacity=2, 
    mean_kiosk_sec=36.0, num_kiosks=5, hall_capacity=60, 
    pb_queue_limit=10, locker_capacity=297
):
    print("\n" + "=" * 80)
    print(f"📊 MONTHLY OPERATIONAL BREACH REPORT: {forecast_label}")
    print("=" * 80)

    if arr_df.empty:
        print("No arrival data available.")
        return pd.DataFrame()

    # Dynamic Hourly Throughput Capacities
    pb_hourly_cap = (3600.0 / pb_cust_sec) * pb_capacity if pb_cust_sec > 0 else 0
    kiosk_hourly_cap = (3600.0 / mean_kiosk_sec) * num_kiosks if mean_kiosk_sec > 0 else 0

    start_time = arr_df['arrival_time'].min().floor('1h')
    end_time = arr_df['arrival_time'].max().ceil('1h')
    hourly_index = pd.date_range(start=start_time, end=end_time, freq='1h')

    # 1. THROUGHPUT METRICS
    hourly_arr = arr_df.set_index('arrival_time').resample('1h').size().reindex(hourly_index, fill_value=0)

    # 2. INSTANTANEOUS STATE METRICS (CONTINUOUS 1-MIN GRID WITH FFILL + HOURLY PEAK)
    delays = extract_return_delay_distribution(actuals_df)
    locker_min = calculate_1min_key_locker_occupancy(ext_df, delays, lead_time_mins=lead_time_mins)

    if sim_logs:
        sim_df = pd.DataFrame(sim_logs).set_index('timestamp').resample('1min').max()
    else:
        sim_df = pd.DataFrame(columns=['pb_queue', 'hall_pop'])

    # Continuous 1-minute time grid to prevent data loss during idle gaps
    min_timeline = pd.date_range(start=start_time, end=end_time, freq='1min')
    min_grid = pd.DataFrame(index=min_timeline)

    # Forward-fill state so idle minutes preserve the current queue/hall state
    min_grid = min_grid.join(sim_df[['pb_queue', 'hall_pop']], how='left').ffill().fillna(0)
    
    if not locker_min.empty:
        min_grid = min_grid.join(locker_min['lockers_occupied'], how='left').ffill().fillna(0)
    else:
        min_grid['lockers_occupied'] = 0

    # Resample to hourly peaks
    hourly_peaks = min_grid.resample('1h').max().reindex(hourly_index, fill_value=0)

    # 3. HOURLY BINARY EVALUATION
    hourly_report = pd.DataFrame(index=hourly_index)
    hourly_report['Month'] = hourly_report.index.to_period('M')

    # Flow Breaches
    hourly_report['pb_rate_breach'] = (hourly_arr > pb_hourly_cap).astype(int)
    hourly_report['kiosk_rate_breach'] = (hourly_arr > kiosk_hourly_cap).astype(int)

    # State Breaches
    hourly_report['pb_q_breach'] = (hourly_peaks['pb_queue'] > pb_queue_limit).astype(int)
    hourly_report['hall_pop_breach'] = (hourly_peaks['hall_pop'] > hall_capacity).astype(int)
    hourly_report['locker_breach'] = (hourly_peaks['lockers_occupied'] > locker_capacity).astype(int)

    # 4. MONTHLY SUMMARY
    monthly_summary = hourly_report.groupby('Month').agg(
        Total_Monitored_Hours=('pb_rate_breach', 'count'),
        PB_Tx_Rate_Breach_Hrs=('pb_rate_breach', 'sum'),
        PB_Queue_Breach_Hrs=('pb_q_breach', 'sum'),
        Kiosk_Tx_Rate_Breach_Hrs=('kiosk_rate_breach', 'sum'),
        Hall_Pop_Breach_Hrs=('hall_pop_breach', 'sum'),
        Locker_Breach_Hrs=('locker_breach', 'sum')
    ).reset_index()

    print(f"ℹ️ Calculated Dynamic Capacities -> Photobooth ({pb_capacity} lanes @ {pb_cust_sec:.1f}s): {pb_hourly_cap:.1f} cars/hr | Kiosks ({num_kiosks} units @ {mean_kiosk_sec:.1f}s): {kiosk_hourly_cap:.1f} check-ins/hr")
    print(monthly_summary.to_string(index=False))
    print("=" * 80)
    return monthly_summary


# =========================================================
# 3. SCRIPT EXECUTION ENTRYPOINT
# =========================================================
if __name__ == "__main__":
    ITERATIONS = 50

    # 1. Load Data & Calculate Dynamic Metrics ONCE in Main Process
    engine, actuals_df, mean_kiosk_sec, dynamic_dwell_mins = load_and_calculate_metrics()

    # 2. Build Profiles & Forecast Datasets
    historic_arrivals = pd.DataFrame({"arrival_time": actuals_df["CheckInStarted"].dropna()}).sort_values("arrival_time").reset_index(drop=True)
    historic_exits = pd.DataFrame({"exit_time": actuals_df["ActualCheckedOutDate"].dropna()}).sort_values("exit_time").reset_index(drop=True)

    full_year_history_df = load_full_year_profile_data_cached('AzureConnection', 'jamie_douglas')
    arrival_profile = build_profile(full_year_history_df, "CheckInStarted")
    exit_profile = build_profile(full_year_history_df, "ActualCheckedOutDate")

    future_arrivals_A, future_exits_A = build_forecast_A(engine, arrival_profile, exit_profile)
    future_arrivals_B, future_exits_B = build_forecast_B(FORECAST_B_CSV_PATH, full_year_history_df)

    analysis_datasets = [
        ("Historical Actuals (Peak Periods)", historic_arrivals, historic_exits),
        ("Forecast A (Short-Term 2-Month Daily)", future_arrivals_A, future_exits_A),
        ("Forecast B (Long-Term Monthly)", future_arrivals_B, future_exits_B)
    ]

    # 3. Solvers & Analysis Loops
    analyze_photobooth_queues(analysis_datasets, mean_kiosk_sec, dynamic_dwell_mins, iterations=ITERATIONS)
    run_dual_kiosk_solver(analysis_datasets, mean_kiosk_sec, dynamic_dwell_mins, iterations=ITERATIONS)
    analyze_dual_key_locker_sensitivity(analysis_datasets, actuals_df, lead_time_options=[15, 30, 45, 60, 120], capacity_limit=297)

    # 4. Monte Carlo Simulations & Monthly Breach Reports
    for label, arr_df, ext_df in analysis_datasets:
        if not arr_df.empty:
            print("\n" + "=" * 80)
            print(f"🎲 RUNNING PARALLEL MONTE CARLO SIMULATION & MONTHLY REPORT ({ITERATIONS} ITERATIONS): {label}")
            print("=" * 80)
            
            params_fast = (arr_df, ext_df, 37.0, 2, mean_kiosk_sec, 5, dynamic_dwell_mins, False)
            mc_results = run_monte_carlo_parallel(params_fast, iterations=ITERATIONS)

            detailed_run = run_monte_carlo_iteration(
                arr_df, ext_df, pb_cust_sec=37.0, pb_capacity=2, 
                kiosk_sec=mean_kiosk_sec, num_kiosks=5, dwell_mins=dynamic_dwell_mins, full_logs=True
            )

            jam_count = sum(1 for r in mc_results if r['pb_jam'])
            breach_count = sum(1 for r in mc_results if r['hall_breach'])
            avg_pb_q = np.mean([r['max_pb_q'] for r in mc_results])
            avg_hall_pop = np.mean([r['max_hall_pop'] for r in mc_results])

            print(f"Photobooth Jam Probability (Queue >= 10): {jam_count}/{ITERATIONS} ({jam_count / ITERATIONS * 100:.1f}%)")
            print(f"Hall Capacity Breach Probability (Pop > 60): {breach_count}/{ITERATIONS} ({breach_count / ITERATIONS * 100:.1f}%)")
            print(f"Mean Max Photobooth Queue: {avg_pb_q:.2f} vehicles")
            print(f"Mean Max Reception Hall Population: {avg_hall_pop:.2f} people")

            generate_monthly_breach_report(
                forecast_label=label, 
                arr_df=arr_df, 
                ext_df=ext_df, 
                sim_logs=detailed_run['logs'], 
                actuals_df=actuals_df,
                lead_time_mins=45,
                pb_cust_sec=37.0,
                pb_capacity=2,
                mean_kiosk_sec=mean_kiosk_sec,
                num_kiosks=5,
                hall_capacity=60,
                pb_queue_limit=10,
                locker_capacity=297
            )
