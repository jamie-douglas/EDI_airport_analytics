
"""
prm_opt.run_s26
---------------
Runs S26 forecast scenarios.
"""

import pyomo.environ as pyo

from .ingest_s26 import ingest_s26
from .build_jobs import build_jobs
from .policy_s1 import apply_policy_s1
from .params import build_tau_from_jobs, build_spin_minutes
from .pyomo_model import build_pyomo_model
from .config import PlanningToggles


def run_s26_s1(df_flight_forecast, penetration_rates, ssr_mix, stand_zone_map, toggles: PlanningToggles = PlanningToggles()):
    df_master = ingest_s26(df_flight_forecast, penetration_rates, ssr_mix, stand_zone_map)
    # S26 has no Job Start/End; ingest sets Scheduled Flight DT and base_duration_mins.
    # build_jobs uses Scheduled Flight DT for t when Job Start Time isn't present.
    jobs = build_jobs(df_master, bucket="15min", toggles=toggles)

    # S1 is less meaningful on S26 unless all tree features are synthesized,
    # but we keep it for comparison.
    jobs["s1_decision"] = jobs.index.map(apply_policy_s1(jobs))
    return jobs


def run_s26_s2(df_flight_forecast, penetration_rates, ssr_mix, stand_zone_map, solver_name="highs", toggles: PlanningToggles = PlanningToggles()):
    df_master = ingest_s26(df_flight_forecast, penetration_rates, ssr_mix, stand_zone_map)
    jobs = build_jobs(df_master, bucket="15min", toggles=toggles)

    tau = build_tau_from_jobs(jobs, toggles)
    spin_removed = build_spin_minutes(jobs, spin_lock_threshold_mins=50)

    # DEFERRED PLACEHOLDER:
    # apply ω scenario delays to Scheduled Flight DT before bucketing once SLA is active.

    model = build_pyomo_model(
        jobs=jobs,
        tau=tau,
        spin_removed=spin_removed,
        toggles=toggles,
    )

    solver = pyo.SolverFactory(solver_name)
    solver.solve(model)

    return model, jobs

































