
# scripts/prm_opt/run_s25.py

"""
Runs S25 scenarios.
"""

import pyomo.environ as pyo

from .ingest_s25 import ingest_s25
from .build_jobs import build_jobs
from .policy_s1 import apply_policy_s1
from .params import build_tau_from_jobs, build_spin_minutes, build_vehicle_classes
from .pyomo_model_legacy import build_pyomo_model
#from .pyomo_model import build_pyomo_model
from .config import PlanningToggles

from .outputs import (
    extract_summary,
    extract_job_assignments,
    extract_vehicle_allocations,
    run_sanity_checks,
    baseline_s1_summary,
    baseline_s1_vehicle_curves
)


def run_s25_s1(start, end, toggles: PlanningToggles = PlanningToggles()):
    df_prm_master = ingest_s25(start, end)
    jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)

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




def run_s25_s2(start, end, solver_name="highs", toggles: PlanningToggles = PlanningToggles()):
    df_prm_master = ingest_s25(start, end)
    jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)
    print(jobs.head())

    tau = build_tau_from_jobs(jobs, toggles)

    BUCKET_MINUTES = 15

    classes = build_vehicle_classes(include_future=False)
    print(classes)

    N_AMB = sum(
        c["count"]
        for vtype, lst in classes.items()
        if vtype == "Amb" 
        for c in lst
        )
    
    spin_removed = build_spin_minutes(jobs, spin_lock_threshold_mins=50, n_ambulifts=N_AMB, bucket_minutes=BUCKET_MINUTES)

    bad = [(b,v) for b,v in spin_removed.items() if v > BUCKET_MINUTES * N_AMB]
    print("spin_removed > total amb minutes:", len(bad))
    print(bad[:10])


    model = build_pyomo_model(
        jobs=jobs,
        tau=tau,
        spin_removed=spin_removed,
        toggles=toggles,
    )

    
    print("Vars:", model.nvariables())
    print("Cons:", model.nconstraints())

    
    
    solver = pyo.SolverFactory(solver_name)

    
    # 1. Ambulift time availability
    #model.AmbTimeCap.deactivate()

    # # 2. Minibus time availability
    #model.MiniTimeCap.deactivate()

    # # 3. Lift bottleneck
    #model.LiftCap.deactivate()

    # # 4. Fleet exclusivity
    #model.FleetExclusive.deactivate()

    # # 5. Vehicle seat / WC caps
    #model.AmbSeatCap.deactivate()
    #model.AmbWcCap.deactivate()
    #model.MiniSeatCap.deactivate()
    #model.MiniWcCap.deactivate()

    # # 6. Maximum lateness
    #model.MaxLateCap.deactivate()
                    

    # DO NOT set solver.config.stream_solver (not supported here)
    # DO set load_solutions=False via solve() args, and tee=True to see log
    results = solver.solve(model, tee=True, load_solutions=False)

    # Print status in a way that works for legacy SolverResults objects
    print("solver_status:", results.solver.status)
    print("termination_condition:", results.solver.termination_condition)

    # Only load variables / compute outputs if feasible
    tc = results.solver.termination_condition
    if str(tc).lower() not in ("optimal", "feasible"):
        # return early so extract_summary doesn't crash on missing values
        return {"results": results, "model": model, "jobs": jobs}

    # If feasible, now load the values
    model.solutions.load_from(results)

        


    # solver = pyo.SolverFactory(solver_name)
    # solver.solve(model)

    # # -----------------------------
    # # NEW OUTPUTS
    # # -----------------------------
    # summary = extract_summary(model, jobs)
    # job_df = extract_job_assignments(model, jobs)
    # vehicle_df = extract_vehicle_allocations(model)
    # checks = run_sanity_checks(job_df, vehicle_df)

    # return {
    #     "model": model,
    #     "jobs": jobs,
    #     "summary": summary,
    #     "job_assignments": job_df,
    #     "vehicle_allocations": vehicle_df,
    #     "sanity_checks": checks,
    # }

