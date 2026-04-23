# scripts/prm_opt/run_s26.py


"""
Runs S26 forecast scenarios.
"""

import pyomo.environ as pyo
import pandas as pd

from .ingest_s26 import ingest_s26
from .ingest_stand_allocations import load_stand_allocations, build_stand_distribution
from .build_jobs import build_jobs
from .policy_s1 import apply_policy_s1
from .params import build_tau_from_jobs, build_spin_minutes
from .pyomo_model import build_pyomo_model
from .config import PlanningToggles


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

    # Scenario 1 policy on forecast is a comparator only (features are synthetic)
    jobs["s1_decision"] = jobs.index.map(apply_policy_s1(jobs))
    return jobs


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

    return model, jobs
