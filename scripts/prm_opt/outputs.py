
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
        mode = next(m for m in model.M if pyo.value(model.x[j, m]) > 0.5)
        served_bucket = next(b for b in model.B if pyo.value(model.A[j, b]) > 0.5)

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
    High‑level KPIs for reporting.
    """
    B = list(model.B)

    y = pd.Series({j: float(pyo.value(model.y[j]) or 0.0) for j in model.J})
    sla_all = 1.0 - y.mean() if len(y) else 1.0

    arr_idx = jobs.index[jobs["dir"] == "A"]
    dep_idx = jobs.index[jobs["dir"] == "D"]

    sla_arr = 1.0 - y.loc[arr_idx].mean() if len(arr_idx) else np.nan
    sla_dep = 1.0 - y.loc[dep_idx].mean() if len(dep_idx) else np.nan

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

    current_amb = sum(int(pyo.value(model.count[(vt, cid)])) for (vt, cid) in model.VC if vt == "Amb")
    current_mini = sum(int(pyo.value(model.count[(vt, cid)])) for (vt, cid) in model.VC if vt == "Mini")

    return {
        "SLA_all": sla_all,
        "SLA_arr": sla_arr,
        "SLA_dep": sla_dep,
        "PeakAmb": max(amb_used) if amb_used else 0,
        "PeakMini": max(mini_used) if mini_used else 0,
        "PeakDrivers": max(drv_used) if drv_used else 0,
        "GapAmb": (max(amb_used) - current_amb) if amb_used else 0,
        "GapMini": (max(mini_used) - current_mini) if mini_used else 0,
    }
