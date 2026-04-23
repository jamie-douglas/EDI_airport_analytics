
# scripts/prm_opt/run_s26.py

"""
Runs S26 forecast scenarios.
"""

import pyomo.environ as pyo

from .ingest_s26 import ingest_s26
from .build_jobs import build_jobs
from .policy_s1 import apply_policy_s1
from .params import build_tau_from_jobs, build_spin_minutes, build_vehicle_classes
from .pyomo_model import build_pyomo_model
from .config import PlanningToggles

from .outputs import (
    extract_summary,
    extract_job_assignments,
    extract_vehicle_allocations,
    run_sanity_checks,
    baseline_s1_summary,
    baseline_s1_vehicle_curves
)



def run_s26_s1(
    start,
    end,
    penetration_rates,
    ssr_mix,
    stand_actuals,
    stand_dist,
    service_time_params,
    chocks_offset_params,
    toggles: PlanningToggles = PlanningToggles(),
):
    df_master = ingest_s26(
        start=start,
        end=end,
        penetration_rates=penetration_rates,
        ssr_mix=ssr_mix,
        stand_actuals=stand_actuals,
        stand_dist=stand_dist,
        service_time_params=service_time_params,
        chocks_offset_params=chocks_offset_params,
    )

    jobs = build_jobs(df_master, bucket="15min", toggles=toggles)

    decisions = apply_policy_s1(jobs)
    jobs["s1_decision"] = jobs.index.map(decisions.get)

    # Baseline curves (use scheduled bucket "s" for comparability)
    curves = baseline_s1_vehicle_curves(jobs, decision_col="s1_decision", bucket_col="s")

    # Current fleet totals (same method as optimisation: from VEHICLE_MODELS)
    classes = build_vehicle_classes(include_future=False)
    current_amb = sum(c["count"] for c in classes.get("Amb", []))
    current_mini = sum(c["count"] for c in classes.get("Mini", []))

    summary = baseline_s1_summary(jobs, curves, current_amb=current_amb, current_mini=current_mini)

    return {
        "jobs": jobs,
        "summary": summary,
        **curves,
    }

    


def run_s26_s2(
    start,
    end,
    penetration_rates,
    ssr_mix,
    stand_actuals,
    stand_dist,
    service_time_params,
    chocks_offset_params,
    solver_name="highs",
    toggles: PlanningToggles = PlanningToggles(),
):
    df_master = ingest_s26(
        start=start,
        end=end,
        penetration_rates=penetration_rates,
        ssr_mix=ssr_mix,
        stand_actuals=stand_actuals,
        stand_dist=stand_dist,
        service_time_params=service_time_params,
        chocks_offset_params=chocks_offset_params,
    )

    jobs = build_jobs(df_master, bucket="15min", toggles=toggles)

    tau = build_tau_from_jobs(jobs, toggles)
    spin_removed = build_spin_minutes(jobs, spin_lock_threshold_mins=50)

    model = build_pyomo_model(
        jobs=jobs,
        tau=tau,
        spin_removed=spin_removed,
        toggles=toggles,
    )

    solver = pyo.SolverFactory(solver_name)
    solver.solve(model)

    
    summary = extract_summary(model, jobs)
    job_df = extract_job_assignments(model, jobs)
    vehicle_df = extract_vehicle_allocations(model)
    checks = run_sanity_checks(job_df, vehicle_df)

    return {
        "model": model,
        "jobs": jobs,
        "summary": summary,
        "job_assignments": job_df,
        "vehicle_allocations": vehicle_df,
        "sanity_checks": checks,
    }


