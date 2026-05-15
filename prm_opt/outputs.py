
# scripts/prm_opt/outputs.py

from collections import defaultdict
import pyomo.environ as pyo
import pandas as pd
import numpy as np


"""
Reporting and diagnostics utilities for PRM optimisation runs.

Contains:
- Extraction of job- and vehicle-level solution outputs
- Sanity / audit checks (non-binding, diagnostic only)
- KPI summaries for Scenario 1 and Scenario 2
- Peak-day and peak-hour fleet reporting

IMPORTANT:
This module contains no optimisation logic.
"""



# =========================================================
# A) JOB‑LEVEL ASSIGNMENTS
# =========================================================
def extract_job_assignments(model, jobs):
    
    """
    Extract passenger-level outcomes from a solved Scenario 2 model.

    One row per PRM job:
    - service start bucket
    - chosen horizontal mode
    - SLA breach indicator
    - vertical / wheelchair flags for audit purposes
    """

    
    # Build mapping: j -> feasible buckets
    jb_by_j = defaultdict(list)
    for (j, b) in model.JB:
        jb_by_j[j].append(b)

    rows = []

    for j in model.J:
        
        mode = max(list(model.M), key=lambda mm: float(pyo.value(model.x[j, mm]) or 0.0))
        
    # Served bucket: ONLY search feasible buckets
        served_bucket = None
        buckets = jb_by_j.get(j, [])

        best_val = -1.0
        for b in buckets:
            v = float(pyo.value(model.A[j, b]) or 0.0)

            # Normal case: binary 1.0
            if v > 0.5:
                served_bucket = b
                break

            # Backup: highest fractional value (numerical safety)
            if v > best_val:
                best_val = v
                served_bucket = b



        rows.append({
            "j": j,
            "Passenger ID": jobs.loc[j, "Passenger ID"],
            "flight_key": jobs.loc[j, "flight_key"],
            "dir": jobs.loc[j, "dir"],
            "scheduled_bucket": jobs.loc[j, "s"],
            "release_bucket": jobs.loc[j, "t"],
            "served_bucket": served_bucket,
            "needs_vertical": int(jobs.loc[j, "needs_vertical"]),
            "needs_wc": int(jobs.loc[j, "needs_wc"]),
            "horizontal_mode": mode,
            "sla_breached": int(pyo.value(model.y[j]) > 0.5),
        })

    return pd.DataFrame(rows)


# =========================================================
# B) VEHICLE ALLOCATION (RAW, BY FLIGHT & BUCKET)
# =========================================================

def extract_vehicle_allocations(model):
    """
    One row per (vehicle type, class, flight, bucket) where count > 0.
    SAFE for sparse FB: iterates only over model.FB.
    """
    rows = []

    for (vt, cid) in model.VC:
        for (f, b) in model.FB:   # <-- KEY CHANGE: only valid (flight,bucket) pairs
            n = int(round(pyo.value(model.k[(vt, cid), (f, b)]) or 0))
            if n > 0:
                rows.append({
                    "vehicle_type": vt,
                    "class_id": cid,
                    "flight_key": f,
                    "bucket": b,
                    "count": n,
                })

    return pd.DataFrame(rows)



# =========================================================
# C) SANITY CHECKS / DIAGNOSTICS
# =========================================================
def run_sanity_checks(job_df, vehicle_df):
    """
    Returns a dict of useful diagnostic tables.
    Nothing is enforced here — this is purely for inspection.

    
    IMPORTANT:
    These are diagnostic / audit checks.
    They do NOT imply infeasibility or errors — 
    They flag solution patterns worth reviewing for operational realism.


    """

    checks = {}

    # 1. Vertical jobs using Mini horizontally
    checks["vertical_with_mini"] = job_df.query(
        "needs_vertical == 1 and horizontal_mode == 'Mini'"
    )

    # 2. Vertical jobs using Push horizontally
    checks["vertical_with_push"] = job_df.query(
        "needs_vertical == 1 and horizontal_mode == 'Push'"
    )

    # 3. SLA breaches by direction
    checks["sla_breaches_by_dir"] = (
        job_df.groupby("dir")["sla_breached"]
        .mean()
        .rename("breach_rate")
        .reset_index()
    )

    # 4. Max vehicles per flight per bucket
    checks["vehicle_peaks"] = (
        vehicle_df
        .groupby(["flight_key", "bucket", "vehicle_type"])["count"]
        .sum()
        .reset_index()
        .sort_values("count", ascending=False)
    )

    
    # 5) Ambulifts > 1 on a flight in a bucket (sanity audit)
    amb_fb = (
        vehicle_df.query("vehicle_type == 'Amb'")
        .groupby(["flight_key", "bucket"])["count"].sum()
        .reset_index(name="amb_count")
    )
    checks["amb_over_1_per_flight_bucket"] = amb_fb.query("amb_count > 1").sort_values("amb_count", ascending=False)

    # 6) Where vertical jobs START in a bucket (proxy for docking activity starting)
    vert_starts = (
        job_df.query("needs_vertical == 1")
        .groupby(["flight_key", "served_bucket"])
        .size()
        .reset_index(name="vertical_jobs_started")
        .rename(columns={"served_bucket": "bucket"})
    )

    # 7) Combine: flights with >1 Amb AND vertical starting in same bucket
    checks["amb_over_1_and_vertical_start_same_bucket"] = (
        checks["amb_over_1_per_flight_bucket"]
        .merge(vert_starts, on=["flight_key", "bucket"], how="inner")
        .sort_values(["amb_count", "vertical_jobs_started"], ascending=False)
    )


    return checks


# =========================================================
# SUMMARY (KPI‑LEVEL)
# =========================================================

def extract_summary(model, jobs):
    """
    High-level KPIs for reporting.

    IMPORTANT:
    - GapAmb / GapMini are computed against CURRENT fleet only (exclude future vehicles).
    """
    from .params import build_vehicle_classes  # local import avoids circular imports

    B = list(model.B)

    # SLA
    y = pd.Series({j: float(pyo.value(model.y[j]) or 0.0) for j in model.J})
    sla_all = 1.0 - y.mean() if len(y) else 1.0

    arr_idx = jobs.index[jobs["dir"] == "A"]
    dep_idx = jobs.index[jobs["dir"] == "D"]

    sla_arr = 1.0 - y.loc[arr_idx].mean() if len(arr_idx) else np.nan
    sla_dep = 1.0 - y.loc[dep_idx].mean() if len(dep_idx) else np.nan

    
    # Fleet used per bucket (total across flights/classes) — SAFE for sparse FB
    amb_used = []
    mini_used = []
    drv_used = []

    # Build a map: bucket -> flights that actually exist in sparse FB
    fb_by_bucket = {}
    for (f, b) in model.FB:
        fb_by_bucket.setdefault(b, []).append(f)

    for b in B:
        flights_b = fb_by_bucket.get(b, [])

        amb = sum(
            int(round(pyo.value(model.k[(vt, cid), (f, b)]) or 0))
            for (vt, cid) in model.VC if vt == "Amb"
            for f in flights_b
        )

        mini = sum(
            int(round(pyo.value(model.k[(vt, cid), (f, b)]) or 0))
            for (vt, cid) in model.VC if vt == "Mini"
            for f in flights_b
        )

        drv = int(round(pyo.value(model.H_drv[b]) or 0))

        amb_used.append(amb)
        mini_used.append(mini)
        drv_used.append(drv)


    peak_amb = max(amb_used) if amb_used else 0
    peak_mini = max(mini_used) if mini_used else 0
    peak_drv = max(drv_used) if drv_used else 0

    # CURRENT fleet totals (exclude future)
    classes_current = build_vehicle_classes(include_future=False)
    current_amb = sum(int(c["count"]) for c in classes_current.get("Amb", []))
    current_mini = sum(int(c["count"]) for c in classes_current.get("Mini", []))

    gap_amb = max(0, peak_amb - current_amb)
    gap_mini = max(0, peak_mini - current_mini)

    return {
        "SLA_all": sla_all,
        "SLA_arr": sla_arr,
        "SLA_dep": sla_dep,

        "PeakAmb": int(peak_amb),
        "PeakMini": int(peak_mini),
        "PeakDrivers": int(peak_drv),

        # make it explicit what the baseline is
        "CurrentAmb": int(current_amb),
        "CurrentMini": int(current_mini),

        # gaps are now DEFINITELY vs current fleet
        "GapAmb": int(gap_amb),
        "GapMini": int(gap_mini),
    }


def baseline_s1_vehicle_curves_capacity(
    jobs,
    decision_col="s1_decision",
    bucket_col="s",
    *,
    count_no_vehicle_as_push: bool = False,
    # effective capacities (current fleet, conservative defaults)
    amb_seatcap: int = 3,
    amb_wccap: int = 1,
    mini_seatcap: int = 6,
    mini_wccap: int = 2,
):
    """
    Capacity-aware S1 baseline curves.

    For each (bucket, flight_key), compute required vehicles as:
      req = max(ceil(seat_demand/seatcap), ceil(wc_demand/wccap))

    Demand is based on job-level policy decisions:
      - "Ambulift Only" -> Amb demand
      - "Mini Bus Only" -> Mini demand
      - "Both" -> Amb demand + Mini demand
      - "No Vehicle" -> optionally pusher demand (not reported)

    Staff convention (your requirement):
      - 1 driver per vehicle (Amb or Mini)
      - 1 vehicle agent per vehicle (Amb or Mini)
      - "Both" implies 2 vehicles -> 2 drivers + 2 agents
      - pushers tracked optionally but not needed in final output
    """
    import pandas as pd
    import numpy as np

    if decision_col not in jobs.columns:
        raise ValueError(f"Missing {decision_col} in jobs")

    df = jobs.copy()

    # Job needs: treat needs_wc as wheelchair, otherwise seated
    # (This assumes each row = one PRM passenger/job)
    df["_wc"] = (df["needs_wc"].fillna(0).astype(int) > 0).astype(int)
    df["_seat"] = 1 - df["_wc"]

    # Which vehicle types does the policy require?
    dec = df[decision_col].fillna("No Vehicle")

    df["_need_amb"] = dec.isin(["Ambulift Only", "Both"]).astype(int)
    df["_need_mini"] = dec.isin(["Mini Bus Only", "Both"]).astype(int)
    df["_need_push"] = 0
    if count_no_vehicle_as_push:
        df["_need_push"] = (dec == "No Vehicle").astype(int)

    # Demand contributions (seat/wc) by vehicle type
    df["_amb_wc"] = df["_need_amb"] * df["_wc"]
    df["_amb_seat"] = df["_need_amb"] * df["_seat"]

    df["_mini_wc"] = df["_need_mini"] * df["_wc"]
    df["_mini_seat"] = df["_need_mini"] * df["_seat"]

    df["_push_cnt"] = df["_need_push"]  # 1 pusher per "No Vehicle" job if enabled

    # Aggregate demand to (bucket, flight_key)
    fb = (
        df.groupby([bucket_col, "flight_key"])[
            ["_amb_wc", "_amb_seat", "_mini_wc", "_mini_seat", "_push_cnt"]
        ]
        .sum()
        .reset_index()
    )

    # Vehicles required per flight/bucket (capacity-based)
    def ceildiv(a, b):
        return int(np.ceil(a / b)) if a > 0 else 0

    amb_req = []
    mini_req = []
    push_req = []

    for _, r in fb.iterrows():
        a_wc = int(r["_amb_wc"])
        a_seat = int(r["_amb_seat"])
        m_wc = int(r["_mini_wc"])
        m_seat = int(r["_mini_seat"])

        amb_needed = max(ceildiv(a_seat, amb_seatcap), ceildiv(a_wc, amb_wccap))
        mini_needed = max(ceildiv(m_seat, mini_seatcap), ceildiv(m_wc, mini_wccap))

        amb_req.append(amb_needed)
        mini_req.append(mini_needed)
        push_req.append(int(r["_push_cnt"]))

    fb["Amb_req"] = amb_req
    fb["Mini_req"] = mini_req
    fb["Push_req"] = push_req

    # Curves per bucket (sum across flights)
    amb_curve = fb.groupby(bucket_col)["Amb_req"].sum().sort_index()
    mini_curve = fb.groupby(bucket_col)["Mini_req"].sum().sort_index()
    push_curve = fb.groupby(bucket_col)["Push_req"].sum().sort_index()

    # Staff curves (separate, as requested)
    driver_curve = (amb_curve + mini_curve).astype(int)        # 1 driver per vehicle
    veh_agent_curve = (amb_curve + mini_curve).astype(int)     # 1 vehicle agent per vehicle

    return {
        "ambulift_curve": amb_curve.astype(int),
        "minibus_curve": mini_curve.astype(int),
        "pusher_curve": push_curve.astype(int),                # computed but you can ignore in output
        "driver_curve": driver_curve,
        "veh_agent_curve": veh_agent_curve,
        "fb_detail": fb,                                       # VERY useful for debugging
    }



def baseline_s1_summary(
    jobs: pd.DataFrame,
    curves: dict,
    *,
    current_amb: int = None,
    current_mini: int = None,
):
    """
    Produce an S1 summary consistent with S2-style reporting.

    Staffing conventions:
      - 1 driver per active vehicle (Amb or Mini)
      - 1 vehicle agent per active vehicle (Amb or Mini)
      - Pushers intentionally excluded at this stage
    """

    amb_curve = curves["ambulift_curve"]
    mini_curve = curves["minibus_curve"]
    drv_curve = curves["driver_curve"]

    # Vehicle agents: use explicit curve if provided, else assume same as drivers
    if "veh_agent_curve" in curves:
        agent_curve = curves["veh_agent_curve"]
    else:
        agent_curve = drv_curve

    peak_amb = int(amb_curve.max()) if len(amb_curve) else 0
    peak_mini = int(mini_curve.max()) if len(mini_curve) else 0
    peak_drv = int(drv_curve.max()) if len(drv_curve) else 0
    peak_agents = int(agent_curve.max()) if len(agent_curve) else 0

    out = {
        "PeakAmb": peak_amb,
        "PeakMini": peak_mini,
        "PeakDrivers": peak_drv,
        "PeakVehAgents": peak_agents,
    }

    # Optional fleet comparison (CURRENT fleet only — no future vehicles)
    if current_amb is not None:
        out["CurrentAmb"] = int(current_amb)
        out["GapAmb"] = int(max(0, peak_amb - current_amb))

    if current_mini is not None:
        out["CurrentMini"] = int(current_mini)
        out["GapMini"] = int(max(0, peak_mini - current_mini))

    return out



def peak_day_hourly_fleet_report(
    jobs: pd.DataFrame,
    vehicle_allocations: pd.DataFrame,
    *,
    day_from: str = "s",          # "s" scheduled bucket (recommended) or "t" release bucket
    hour_method: str = "max",     # "max" = worst 15-min in the hour (recommended)
    bucket_minutes: int = 15,
):
    """
    Finds the peak PRM day (most PRM jobs) and returns hourly ambulift/minibus
    requirements and fleet gaps vs CURRENT fleet.

    Returns dict with:
      - peak_day (date)
      - peak_day_job_count (int)
      - current_fleet (dict)
      - hourly (DataFrame indexed by hour with Amb_req, Mini_req, Amb_gap, Mini_gap)
      - peaks (dict with peak hour req + peak gaps)
    """
    from .params import build_vehicle_classes  # local import to avoid circulars

    if day_from not in jobs.columns:
        raise ValueError(f"jobs missing column '{day_from}'. Use day_from='s' or 't'.")

    if vehicle_allocations is None or len(vehicle_allocations) == 0:
        raise ValueError("vehicle_allocations is empty. Need a feasible solution with extracted allocations.")

    # 1) Peak day by job count
    tmp = jobs.copy()
    tmp["_day"] = pd.to_datetime(tmp[day_from]).dt.date
    day_counts = tmp.groupby("_day").size()
    peak_day = day_counts.idxmax()
    peak_day_job_count = int(day_counts.max())

    # 2) Filter allocations to that day and total per bucket
    veh = vehicle_allocations.copy()
    veh["bucket"] = pd.to_datetime(veh["bucket"])
    veh_day = veh[veh["bucket"].dt.date == peak_day].copy()

    # Sum across flights+classes => total vehicles required per bucket
    bucket_totals = (
        veh_day.groupby(["bucket", "vehicle_type"])["count"]
        .sum()
        .unstack("vehicle_type", fill_value=0)
        .sort_index()
    )

    # Ensure both columns exist
    if "Amb" not in bucket_totals.columns:
        bucket_totals["Amb"] = 0
    if "Mini" not in bucket_totals.columns:
        bucket_totals["Mini"] = 0

    # 3) Bucket -> Hour aggregation
    if hour_method == "max":
        hourly = bucket_totals.resample("H").max()
    elif hour_method == "mean":
        hourly = bucket_totals.resample("H").mean()
    elif hour_method == "sum":
        hourly = bucket_totals.resample("H").sum()
    else:
        raise ValueError("hour_method must be one of: 'max', 'mean', 'sum'")

    hourly = hourly.rename(columns={"Amb": "Amb_req", "Mini": "Mini_req"})

    # 4) Current fleet (exclude future)
    classes_current = build_vehicle_classes(include_future=False)
    current_amb = sum(int(c["count"]) for c in classes_current.get("Amb", []))
    current_mini = sum(int(c["count"]) for c in classes_current.get("Mini", []))

    hourly["Amb_gap"] = (hourly["Amb_req"] - current_amb).clip(lower=0)
    hourly["Mini_gap"] = (hourly["Mini_req"] - current_mini).clip(lower=0)

    peaks = {
        "peak_hour_amb_req": int(hourly["Amb_req"].max()) if len(hourly) else 0,
        "peak_hour_mini_req": int(hourly["Mini_req"].max()) if len(hourly) else 0,
        "gap_amb_peak": int(max(0, hourly["Amb_req"].max() - current_amb)) if len(hourly) else 0,
        "gap_mini_peak": int(max(0, hourly["Mini_req"].max() - current_mini)) if len(hourly) else 0,
    }

    return {
        "peak_day": peak_day,
        "peak_day_job_count": peak_day_job_count,
        "current_fleet": {"Amb": current_amb, "Mini": current_mini},
        "hourly": hourly,
        "peaks": peaks,
    }


def build_run_report(out: dict, *, day_from: str = "s", hour_method: str = "max"):
    """
    Build a consistent report bundle from one run_s25_s2_v2 output dict.

    Always includes sanity checks (when a solution exists).
    Includes peak-day hourly fleet report (from vehicle_allocations).
    """
    report = {
        "status": out.get("status"),
        "tc": out.get("tc"),
        "st": out.get("st"),
        "loaded_solution": out.get("loaded_solution", None),
        "load_error": out.get("load_error", None),
    }

    # If we don't have extracted outputs, return minimal info
    if out.get("status") in ("built_only", "infeasible") or ("vehicle_allocations" not in out):
        report["note"] = "No extracted outputs (vehicle_allocations/job_assignments/summary) for this run."
        report["jobs"] = out.get("jobs")
        report["ladder"] = out.get("ladder")
        return report

    # Core outputs
    report["summary"] = out.get("summary", {})
    report["job_assignments"] = out["job_assignments"]
    report["vehicle_allocations"] = out["vehicle_allocations"]


    # Always compute sanity checks (not optional)
    report["sanity_checks"] = run_sanity_checks(
        report["job_assignments"],
        report["vehicle_allocations"]
    )

    # Peak-day hourly fleet report (uses CURRENT fleet baselines internally)
    report["peak_day_report"] = peak_day_hourly_fleet_report(
        jobs=out["jobs"],
        vehicle_allocations=out["vehicle_allocations"],
        day_from=day_from,
        hour_method=hour_method,
    )

    return report


def print_run_report(report: dict, *, show_hours: int = 24, show_top: int = 10):
    """
    Pretty print a report from build_run_report().
    """
    print("\n" + "=" * 78)
    print("PRM OPT — Scenario 2 Output")
    print("=" * 78)
    print(f"Run status : {report.get('status')}")
    print(f"Solver     : tc={report.get('tc')} | status={report.get('st')}")
    if report.get("loaded_solution") is not None:
        print(f"Loaded sol : {report.get('loaded_solution')} | load_error={report.get('load_error')}")
    
    
    assump = report.get("assumptions", {})
    if assump:
        print("\nAssumptions:")
        print(f"  Vertical cycle extra mins : {assump.get('vertical_cycle_mins')}")


    # No outputs case
    if "note" in report:
        print("\n" + report["note"])
        print("=" * 78)
        return

    # 1) Horizon summary (already vs CURRENT fleet if you applied the patch)
    summ = report.get("summary", {})
    print("\n[1] Horizon Summary (Peak across horizon; gaps vs CURRENT fleet)")
    print(f"  SLA_all      : {summ.get('SLA_all'):.3f}" if summ.get("SLA_all") is not None else "  SLA_all      : n/a")
    print(f"  SLA_arr      : {summ.get('SLA_arr'):.3f}" if summ.get("SLA_arr") is not None else "  SLA_arr      : n/a")
    print(f"  SLA_dep      : {summ.get('SLA_dep'):.3f}" if summ.get("SLA_dep") is not None else "  SLA_dep      : n/a")

    print(f"  PeakAmb      : {summ.get('PeakAmb')}   (Current={summ.get('CurrentAmb')} | Gap={summ.get('GapAmb')})")
    print(f"  PeakMini     : {summ.get('PeakMini')}  (Current={summ.get('CurrentMini')} | Gap={summ.get('GapMini')})")
    print(f"  PeakDrivers  : {summ.get('PeakDrivers')}")

    # 2) Peak day hourly table
    peak = report.get("peak_day_report", {})
    print("\n[2] Peak Day (most PRM jobs) — Hourly Fleet Requirement + Gaps")
    print(f"  Peak day: {peak.get('peak_day')} | jobs={peak.get('peak_day_job_count')}")
    cf = peak.get("current_fleet", {})
    pk = peak.get("peaks", {})
    print(f"  Current fleet used: Amb={cf.get('Amb')} | Mini={cf.get('Mini')}")
    print(f"  Peak hour req      : Amb={pk.get('peak_hour_amb_req')} | Mini={pk.get('peak_hour_mini_req')}")
    print(f"  Peak hour gap      : Amb={pk.get('gap_amb_peak')} | Mini={pk.get('gap_mini_peak')}")

    hourly = peak.get("hourly", None)
    if isinstance(hourly, pd.DataFrame) and len(hourly):
        print("\n  Hourly table (first rows):")
        print(hourly.head(show_hours).to_string())

    # 3) Sanity checks (always)
    checks = report.get("sanity_checks", {})
    print("\n[3] Sanity Checks (always)")
    if "sla_breaches_by_dir" in checks:
        print("\n  SLA breach rates by dir:")
        print(checks["sla_breaches_by_dir"].to_string(index=False))

    if "vehicle_peaks" in checks:
        print(f"\n  Top vehicle peaks (flight, bucket, type) — top {show_top}:")
        print(checks["vehicle_peaks"].head(show_top).to_string(index=False))

    if "vertical_with_mini" in checks:
        vw = checks["vertical_with_mini"]
        print(f"\n  Vertical jobs with Mini horizontally — count={len(vw)} (top {show_top}):")
        print(vw.head(show_top).to_string(index=False) if len(vw) else "  (none)")

    if "vertical_with_push" in checks:
        vp = checks["vertical_with_push"]
        print(f"\n  Vertical jobs with Push horizontally — count={len(vp)} (top {show_top}):")
        print(vp.head(show_top).to_string(index=False) if len(vp) else "  (none)")

    print("\nDone.")
    print("=" * 78)



def peak_day_hourly_s1_report(
    jobs: pd.DataFrame,
    curves: dict,
    *,
    day_from: str = "s",
    hour_method: str = "max",
):
    """
    Scenario 1 peak day report from baseline curves.

    Now includes:
      - Drivers_req (from driver_curve)
      - VehAgents_req (from veh_agent_curve, if provided; else assumed = Drivers_req)

    Pushers may exist in curves but are NOT required for the printed output.
    """
    from .params import build_vehicle_classes  # local import avoids circular imports

    if day_from not in jobs.columns:
        raise ValueError(f"jobs missing column '{day_from}'. Use day_from='s' or 't'.")

    # 1) Peak day by PRM job count
    tmp = jobs.copy()
    tmp["_day"] = pd.to_datetime(tmp[day_from]).dt.date
    day_counts = tmp.groupby("_day").size()
    peak_day = day_counts.idxmax()
    peak_day_job_count = int(day_counts.max())

    # 2) Pull curves (15-min) and filter to peak day
    amb_curve = curves["ambulift_curve"].copy()
    mini_curve = curves["minibus_curve"].copy()
    drv_curve = curves["driver_curve"].copy()

    # veh agents: prefer explicit, else assume same as drivers
    if "veh_agent_curve" in curves:
        agent_curve = curves["veh_agent_curve"].copy()
    else:
        agent_curve = drv_curve.copy()

    # Ensure datetime indexes
    amb_curve.index = pd.to_datetime(amb_curve.index)
    mini_curve.index = pd.to_datetime(mini_curve.index)
    drv_curve.index = pd.to_datetime(drv_curve.index)
    agent_curve.index = pd.to_datetime(agent_curve.index)

    amb_day = amb_curve[amb_curve.index.date == peak_day]
    mini_day = mini_curve[mini_curve.index.date == peak_day]
    drv_day = drv_curve[drv_curve.index.date == peak_day]
    agent_day = agent_curve[agent_curve.index.date == peak_day]

    # Align into single 15-min table (fill missing buckets with 0)
    bucket_level = pd.DataFrame({
        "Amb_req": amb_day,
        "Mini_req": mini_day,
        "Drivers_req": drv_day,
        "VehAgents_req": agent_day,
    }).fillna(0).sort_index()

    # 3) Bucket -> hour aggregation
    if hour_method == "max":
        hourly = bucket_level.resample("H").max()
    elif hour_method == "mean":
        hourly = bucket_level.resample("H").mean()
    elif hour_method == "sum":
        hourly = bucket_level.resample("H").sum()
    else:
        raise ValueError("hour_method must be one of: 'max', 'mean', 'sum'")

    # 4) Current fleet (exclude future) + gaps on vehicles (not staff)
    classes_current = build_vehicle_classes(include_future=False)
    current_amb = sum(int(c["count"]) for c in classes_current.get("Amb", []))
    current_mini = sum(int(c["count"]) for c in classes_current.get("Mini", []))

    bucket_level["Amb_gap"] = (bucket_level["Amb_req"] - current_amb).clip(lower=0)
    bucket_level["Mini_gap"] = (bucket_level["Mini_req"] - current_mini).clip(lower=0)

    hourly["Amb_gap"] = (hourly["Amb_req"] - current_amb).clip(lower=0)
    hourly["Mini_gap"] = (hourly["Mini_req"] - current_mini).clip(lower=0)

    peaks = {
        "peak_15min_amb_req": int(bucket_level["Amb_req"].max()) if len(bucket_level) else 0,
        "peak_15min_mini_req": int(bucket_level["Mini_req"].max()) if len(bucket_level) else 0,
        "peak_hour_amb_req": int(hourly["Amb_req"].max()) if len(hourly) else 0,
        "peak_hour_mini_req": int(hourly["Mini_req"].max()) if len(hourly) else 0,
        "peak_hour_drivers_req": int(hourly["Drivers_req"].max()) if len(hourly) else 0,
        "peak_hour_veh_agents_req": int(hourly["VehAgents_req"].max()) if len(hourly) else 0,
        "gap_amb_peak_hour": int(max(0, hourly["Amb_req"].max() - current_amb)) if len(hourly) else 0,
        "gap_mini_peak_hour": int(max(0, hourly["Mini_req"].max() - current_mini)) if len(hourly) else 0,
    }

    return {
        "peak_day": peak_day,
        "peak_day_job_count": peak_day_job_count,
        "current_fleet": {"Amb": current_amb, "Mini": current_mini},
        "bucket_level": bucket_level,
        "hourly": hourly,
        "peaks": peaks,
    }




def build_run_report_s1(out_s1: dict, *, day_from: str = "s", hour_method: str = "max"):
    """
    Mirror S2 build_run_report(), but for S1.
    Uses policy curves instead of vehicle_allocations.
    """
    curves = {
        "ambulift_curve": out_s1["ambulift_curve"],
        "minibus_curve": out_s1["minibus_curve"],
        "driver_curve": out_s1["driver_curve"],
    }

    # Always carry vehicle agents if present; if not, we can still compute from drivers in the report step
    if "veh_agent_curve" in out_s1:
        curves["veh_agent_curve"] = out_s1["veh_agent_curve"]

    # Carry pushers if you want them later, but we won't print them
    if "pusher_curve" in out_s1:
        curves["pusher_curve"] = out_s1["pusher_curve"]

    report = {
        "status": "s1_policy",
        "summary": out_s1.get("summary", {}),
        "jobs": out_s1["jobs"],
        "curves": curves,
    }

    report["peak_day_report"] = peak_day_hourly_s1_report(
        jobs=out_s1["jobs"],
        curves=curves,
        day_from=day_from,
        hour_method=hour_method,
    )

    return report




def print_run_report_s1(report: dict, *, show_hours: int = 24):
    print("\n" + "=" * 78)
    print("PRM OPT — Scenario 1 (Policy Baseline) Output")
    print("=" * 78)

    summ = report.get("summary", {})
    print("\n[1] Horizon Summary (Peak across horizon; gaps vs CURRENT fleet)")
    for k in ["PeakAmb","PeakMini","PeakDrivers","PeakVehAgents","CurrentAmb","CurrentMini","GapAmb","GapMini"]:
        if k in summ:
            print(f"  {k:14s}: {summ[k]}")

    peak = report.get("peak_day_report", {})
    print("\n[2] Peak Day (most PRM jobs) — Hourly Requirement + Gaps")
    print(f"  Peak day: {peak.get('peak_day')} | jobs={peak.get('peak_day_job_count')}")
    cf = peak.get("current_fleet", {})
    print(f"  Current fleet: Amb={cf.get('Amb')} | Mini={cf.get('Mini')}")

    pk = peak.get("peaks", {})
    if pk:
        print(
            "  Peak hour req : "
            f"Amb={pk.get('peak_hour_amb_req')} | Mini={pk.get('peak_hour_mini_req')} | "
            f"Drivers={pk.get('peak_hour_drivers_req')} | VehAgents={pk.get('peak_hour_veh_agents_req')}"
        )
        print(
            "  Peak hour gap : "
            f"Amb={pk.get('gap_amb_peak_hour')} | Mini={pk.get('gap_mini_peak_hour')}"
        )

    hourly = peak.get("hourly", None)
    if isinstance(hourly, pd.DataFrame) and len(hourly):
        cols = [c for c in ["Amb_req","Mini_req","Drivers_req","VehAgents_req","Amb_gap","Mini_gap"] if c in hourly.columns]
        print("\n  Hourly table (first rows):")
        print(hourly[cols].head(show_hours).to_string())

    print("\nDone.")
    print("=" * 78)

