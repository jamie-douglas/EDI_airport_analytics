
"""
prm_opt.run_s25
---------------
Runs S25 scenarios.
"""

import pyomo.environ as pyo

from .ingest_s25 import ingest_s25
from .build_jobs import build_jobs
from .policy_s1 import apply_policy_s1
from .params import build_tau_from_jobs, build_spin_minutes
from .pyomo_model import build_pyomo_model
from .config import PlanningToggles


def run_s25_s1(start, end, toggles: PlanningToggles = PlanningToggles()):
    df_prm_master = ingest_s25(start, end)
    jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)
    jobs["s1_decision"] = jobs.index.map(apply_policy_s1(jobs))
    return jobs


def run_s25_s2(start, end, solver_name="highs", toggles: PlanningToggles = PlanningToggles()):
    df_prm_master = ingest_s25(start, end)
    jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)

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
