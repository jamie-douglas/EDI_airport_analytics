
# scripts/prm_opt/outputs.py

import pyomo.environ as pyo
import pandas as pd
import numpy as np


# =========================================================
# A) JOB‑LEVEL ASSIGNMENTS
# =========================================================
def extract_job_assignments(model, jobs):
    """
    One row per PRM job:
      - which service bucket it was served in
      - which HORIZONTAL mode was chosen
      - whether SLA was breached
      - vertical flags for auditing logic
    """
    rows = []

    for j in model.J:
        
        mode = max(list(model.M), key=lambda mm: float(pyo.value(model.x[j, mm]) or 0.0))
        served_bucket = max(list(model.B), key=lambda bb: float(pyo.value(model.A[j, bb]) or 0.0))


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
    This lets you see EXACTLY what the model dispatched.
    """
    rows = []

    for (vt, cid) in model.VC:
        for f in model.F:
            for b in model.B:
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

    # Fleet used per bucket (total across flights/classes)
    amb_used = []
    mini_used = []
    drv_used = []

    for b in B:
        amb = sum(
            int(round(pyo.value(model.k[(vt, cid), (f, b)]) or 0))
            for (vt, cid) in model.VC if vt == "Amb"
            for f in model.F
        )
        mini = sum(
            int(round(pyo.value(model.k[(vt, cid), (f, b)]) or 0))
            for (vt, cid) in model.VC if vt == "Mini"
            for f in model.F
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



import pandas as pd

def baseline_s1_vehicle_curves(jobs, decision_col="s1_decision", bucket_col="s"):
    """
    Build S1 baseline curves from policy decisions.

    
    Returns:
      - ambulift_curve (per bucket)
      - minibus_curve  (per bucket)
      - pusher_curve   (per bucket)
      - driver_curve   (per bucket)  [drivers = amb + mini]
    """

    if decision_col not in jobs.columns:
        raise ValueError(f"Missing {decision_col} in jobs")

    jobs = jobs.copy()
    jobs["_amb"] = 0
    jobs["_mini"] = 0
    jobs["_push"] = 0

    # Map decisions at job level
    jobs.loc[jobs[decision_col] == "Ambulift Only", "_amb"] = 1
    jobs.loc[jobs[decision_col] == "Mini Bus Only", "_mini"] = 1

    jobs.loc[jobs[decision_col] == "Both", "_amb"] = 1
    jobs.loc[jobs[decision_col] == "Both", "_mini"] = 1

    jobs.loc[jobs[decision_col] == "Push", "_push"] = 1  # optional future

    # Aggregate to flight-bucket so we count a dispatch once per flight per bucket
    fb = (
        jobs
        .groupby([bucket_col, "flight_key"])[["_amb", "_mini", "_push"]]
        .max()
        .reset_index()
    )

    # Now sum across flights per bucket
    amb_curve = fb.groupby(bucket_col)["_amb"].sum().sort_index()
    mini_curve = fb.groupby(bucket_col)["_mini"].sum().sort_index()
    push_curve = fb.groupby(bucket_col)["_push"].sum().sort_index()

    drv_curve = (amb_curve + mini_curve).astype(int)

    return {
        "ambulift_curve": amb_curve,
        "minibus_curve": mini_curve,
        "pusher_curve": push_curve,
        "driver_curve": drv_curve,
    }



def baseline_s1_summary(jobs, curves, current_amb=None, current_mini=None):
    """
    Produce an S2-style summary for S1.
    """
    amb_curve = curves["ambulift_curve"]
    mini_curve = curves["minibus_curve"]
    drv_curve = curves["driver_curve"]

    peak_amb = int(amb_curve.max()) if len(amb_curve) else 0
    peak_mini = int(mini_curve.max()) if len(mini_curve) else 0
    peak_drv = int(drv_curve.max()) if len(drv_curve) else 0

    out = {
        "PeakAmb": peak_amb,
        "PeakMini": peak_mini,
        "PeakDrivers": peak_drv,
        "PeakAmb_bucket": amb_curve.idxmax() if len(amb_curve) else None,
        "PeakMini_bucket": mini_curve.idxmax() if len(mini_curve) else None,
        "PeakDrivers_bucket": drv_curve.idxmax() if len(drv_curve) else None,
    }

    if current_amb is not None:
        out["CurrentAmb"] = int(current_amb)
        out["GapAmb"] = int(peak_amb - current_amb)
    if current_mini is not None:
        out["CurrentMini"] = int(current_mini)
        out["GapMini"] = int(peak_mini - current_mini)

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
