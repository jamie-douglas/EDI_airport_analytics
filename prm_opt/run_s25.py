
# scripts/prm_opt/run_s25.py

from tracemalloc import start

import pandas as pd
import numpy as np
import time

from prm_opt.outputs import build_run_report, build_run_report_s1

"""
Scenario runners for S25 (historical data).

This file provides:
- Scenario 1 (S1): Policy-based baseline using observed decision rules
- Scenario 2 (S2): Optimisation-based fleet sizing using Pyomo (v2 model)
- Sensitivity analysis over vertical-cycle assumptions
- Optional LP-relaxation diagnostics for infeasibility analysis

Key design principle:
S25 and S26 share the same downstream logic (build_jobs → model → outputs).
Only ingestion differs.
"""

import pyomo.environ as pyo
from modules.utils.progress import step
from dataclasses import replace


from .ingest_s25 import ingest_s25
from .build_jobs import build_jobs
from .policy_s1 import apply_policy_s1
from .params import build_tau_from_jobs, build_spin_minutes, build_vehicle_classes
from .pyomo_model_v2 import build_pyomo_model
#from .pyomo_model_legacy import build_pyomo_model
from .config import LIFT_CAPACITY_MINS, LIFT_CYCLE_MINS, PlanningToggles
from .debug_tools import pre_solve_debug, feasibility_ladder, check_job_time_windows, make_lp_relaxation_model, lp_ladder_single_relax

from .outputs import (
    extract_summary,
    extract_job_assignments,
    extract_vehicle_allocations,
    run_sanity_checks,
    baseline_s1_summary,
    baseline_s1_vehicle_curves_capacity
)


def _extract_and_dispose(model, jobs):
    """
    Extract all reporting outputs from a solved model
    and safely dispose of the Pyomo model to free memory.
    """
    summary = extract_summary(model, jobs)
    job_df = extract_job_assignments(model, jobs)
    vehicle_df = extract_vehicle_allocations(model)
    sanity_checks = run_sanity_checks(job_df, vehicle_df)

    out = {
        "jobs": jobs,
        "summary": summary,
        "job_assignments": job_df,
        "vehicle_allocations": vehicle_df,
        "sanity_checks": sanity_checks,
    }

    # 🔥 critical: free memory
    del model
    import gc
    gc.collect()

    return out



def run_s25_s1(start, end, toggles: PlanningToggles = PlanningToggles()):
    
    """
    Scenario 1 (S25 baseline).

    Reproduces historical operations using a fixed policy:
    - Horizontal mode choices are fixed via a decision-tree proxy
    - No optimisation is performed
    - Outputs are capacity curves and peak requirements only

    This scenario is used as a baseline for comparison against Scenario 2.
    """

    df_prm_master = ingest_s25(start, end)
    jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)

    
    # Apply historical decision logic (Scenario 1):
    # This fixes horizontal modes based on observed S25 behaviour.
    decisions = apply_policy_s1(jobs)
    jobs["s1_decision"] = jobs.index.map(decisions.get)

    # Baseline curves (use scheduled bucket "s" for comparability)
    
    
    # Convert job-level decisions into capacity-based vehicle curves.
    # Uses effective seat / wheelchair capacities to estimate vehicles required.
    curves = baseline_s1_vehicle_curves_capacity(
        jobs,
        decision_col="s1_decision",
        bucket_col="s",
        count_no_vehicle_as_push=True,   # computed but you can ignore pusher outputs
        amb_seatcap=3, amb_wccap=1,      # conservative from current fleet
        mini_seatcap=6, mini_wccap=2,    # current minibuses
    )



    # Current fleet totals (same method as optimisation: from VEHICLE_MODELS)
    classes = build_vehicle_classes(include_future=False)
    current_amb = sum(c["count"] for c in classes.get("Amb", []))
    current_mini = sum(c["count"] for c in classes.get("Mini", []))

    summary = baseline_s1_summary(jobs, curves, current_amb=current_amb, current_mini=current_mini)

    
    return {
        "jobs": jobs,
        "summary": summary,
        **{k: curves[k] for k in ["ambulift_curve","minibus_curve","driver_curve","veh_agent_curve","pusher_curve"]},
        "fb_detail": curves["fb_detail"],
    }

def run_s25_s2_v2(
    start,
    end,
    solver_name="highs",
    toggles: PlanningToggles = PlanningToggles(),
    run_ladder: bool = False,
    solve_model: bool = True,
    time_limit_sec: int = 600,
    threads: int = 8,
    mip_rel_gap: float | None = None,
    ladder_step_limit_sec: int = 100,
):
    
    """
    Scenario 2 (v2): Optimised fleet sizing for S25.

    Pipeline:
      1) Ingest historical PRM data
      2) Build canonical job table
      3) Construct time and capacity parameters
      4) Run pre-solver feasibility diagnostics (no optimisation)
      5) Build Pyomo MILP model
      6) Solve once and extract outputs

    Notes:
      - Bucket spillover and standby logic are implemented in the v2 model
      - SLA breaches are allowed but penalised
    """


    print("\nPRM OPT — S25 Scenario 2 (v2)")
    print(f"Window : {start} → {end}\n")

    t = time.perf_counter()

    
    # --------------------------------------------------
    # STEP 1: Ingest S25 data and build PRM jobs
    # --------------------------------------------------

    print("[1/6] ingest_s25…", flush=True)
    df_prm_master = ingest_s25(start, end)
    t = step(t, f"ingest_s25 done | rows={len(df_prm_master):,}")

    print("[2/6] build_jobs…", flush=True)
    jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)
    t = step(t, f"build_jobs done | jobs={len(jobs):,}")

    
    # --------------------------------------------------
    # STEP 2: Build time / fleet parameters
    # --------------------------------------------------

    BUCKET_MINUTES = 15

    bad_jobs = check_job_time_windows(jobs, BUCKET_MINUTES, max_late_mins=180)

    print("[3/6] build_tau_from_jobs…", flush=True)
    tau = build_tau_from_jobs(jobs, toggles)
    t = step(t, "tau built")

    print("[4/6] build_vehicle_classes + build_spin_minutes…", flush=True)
    classes = build_vehicle_classes(include_future=True)
    N_AMB = sum(int(c["count"]) for c in classes.get("Amb", []))
    spin_removed = build_spin_minutes(
        jobs,
        spin_lock_threshold_mins=50,
        n_ambulifts=N_AMB,
        bucket_minutes=BUCKET_MINUTES,
    )
    t = step(t, f"classes+spin_removed built | N_AMB={N_AMB}")

    
    # --------------------------------------------------
    # STEP 3: Pre-solver feasibility diagnostics
    # (necessary-condition checks only; no optimisation)
    # --------------------------------------------------

    print("[5/6] pre_solve_debug (no solver)…", flush=True)
    pre_solve_debug(
        jobs=jobs,
        toggles=toggles,
        tau=tau,
        spin_removed=spin_removed,
        classes=classes,
        bucket_minutes=BUCKET_MINUTES,
        M_BIG=10_000,
        lift_cycle_mins=LIFT_CYCLE_MINS,
        lift_capacity_mins=LIFT_CAPACITY_MINS,
    )
    t = step(t, "pre_solve_debug done")

    # --------------------------------------------------
    # STEP 4: Build Pyomo Optimisation model
    # --------------------------------------------------
    print("[6/6] build_pyomo_model…", flush=True)
    t_build0 = time.perf_counter()
    model = build_pyomo_model(
        jobs=jobs,
        tau=tau,
        spin_removed=spin_removed,
        toggles=toggles,
    )
    print(f"    ✓ build_pyomo_model done   [{time.perf_counter() - t_build0:0.2f}s]", flush=True)
    print("Vars:", model.nvariables())
    print("Cons:", model.nconstraints())

    
    # Optional infeasibility analysis:
    # Deactivates constraint families one-by-one to identify which
    # constraints are responsible for infeasibility.
    # Intended for debugging only.

    ladder_summary = None
    if run_ladder:
        print("\n[ladder] time-capped MILP ladder…", flush=True)
        groups = [
            ("none (baseline)", []),
            ("NoServiceAfterDeadline", ["NoServiceAfterDeadline"]),
            ("AmbTimeCap", ["AmbTimeCap"]),
            ("MiniTimeCap", ["MiniTimeCap"]),
            ("FleetExclusive", ["FleetExclusive"]),
            ("AmbSeat+Wc", ["AmbSeatCap", "AmbWcCap"]),
            ("MiniSeat+Wc", ["MiniSeatCap", "MiniWcCap"]),
            ("LiftCap", ["LiftCap"]),
            ("Eligibility rules", ["SafetyStand", "NoAmbDomesticArrivals"]),
        ]
        solver_tmp = pyo.SolverFactory(solver_name)
        solver_tmp.options["time_limit"] = float(ladder_step_limit_sec)
        solver_tmp.options["threads"] = int(threads)
        ladder_summary = feasibility_ladder(model, solver_tmp, groups)

        for comp in model.component_objects(pyo.Constraint, active=False):
            comp.activate()

    
    # If you only want build+debug+ladder, stop here (v1 behaviour)
    if not solve_model:
        return {
            "status": "built_only",
            "model": model,
            "jobs": jobs,
            "ladder": ladder_summary,
        }

    
    # --------------------------------------------------
    # STEP 5: Solve MILP
    # --------------------------------------------------

    print("\n[SOLVE] solving MILP…", flush=True)
    solver = pyo.SolverFactory(solver_name)
    solver.options["time_limit"] = float(time_limit_sec)
    solver.options["threads"] = int(threads)
    if mip_rel_gap is not None:
        solver.options["mip_rel_gap"] = float(mip_rel_gap)

    t_solve0 = time.perf_counter()
    results = solver.solve(model, tee=True, load_solutions=False)
    solve_wall = time.perf_counter() - t_solve0

    
    tc = str(results.solver.termination_condition).lower()
    st = str(results.solver.status).lower()

    print(f"[SOLVE] wall={solve_wall:0.2f}s | tc={tc} | status={st}", flush=True)

    
    
    loaded = False
    load_err = None
    try:
        model.solutions.load_from(results)
        loaded = True
    except Exception as e:
        load_err = repr(e)
        loaded = False

    # 1) Infeasible -> return
    if tc in ("infeasible", "infeasibleorunbounded"):
        return {
            "status": "infeasible",
            "tc": tc, "st": st,
            "model": model, "jobs": jobs, "results": results,
            "ladder": ladder_summary,
            "loaded_solution": loaded,
            "load_error": load_err,
        }

    # 2) If we have a usable solution (optimal/feasible OR time-limit-with-incumbent), extract outputs
    if tc in ("optimal", "feasible") or (("timelimit" in tc) and loaded):
        summary = extract_summary(model, jobs)
        job_df = extract_job_assignments(model, jobs)
        vehicle_df = extract_vehicle_allocations(model)
        checks = run_sanity_checks(job_df, vehicle_df)

        return {
            "status": "optimal" if tc in ("optimal", "feasible") else "timelimit_feasible",
            "tc": tc, "st": st,
            "model": model, "jobs": jobs, "results": results,
            "summary": summary,
            "job_assignments": job_df,
            "vehicle_allocations": vehicle_df,
            "sanity_checks": checks,
            "ladder": ladder_summary,
            "loaded_solution": loaded,
            "load_error": load_err,
        }

    # 3) Otherwise: unknown termination with no incumbent
    return {
        "status": tc,
        "tc": tc, "st": st,
        "model": model, "jobs": jobs, "results": results,
        "ladder": ladder_summary,
        "loaded_solution": loaded,
        "load_error": load_err,
    }



def run_s25_s2_v2_sensitivity(
    start,
    end,
    vertical_cycle_grid=(0, 5, 10, 15),
    solver_name="highs",
    toggles: PlanningToggles = PlanningToggles(),
    time_limit_sec: int = 600,
    threads: int = 8,
    mip_rel_gap: float | None = None,
):
    
    """
    Sensitivity analysis for Scenario 2 (v2).

    Runs the same S25 demand with different assumptions on:
    - vertical_cycle_mins (additional time per repeated vertical cycle)

    Design:
    - Expensive steps (ingest, build_jobs, tau, spin) are run ONCE
    - Only the model build + solve are repeated per assumption
    """


    print("\nPRM OPT — S25 Scenario 2 (v2) — Sensitivity")
    print(f"Window : {start} → {end}")
    print(f"vertical_cycle_grid: {list(vertical_cycle_grid)}\n")

    t = time.perf_counter()

    # -------------------------
    # 1) Ingest + build jobs (ONCE)
    # -------------------------
    print("[1/4] ingest_s25…", flush=True)
    df_prm_master = ingest_s25(start, end)
    t = step(t, f"ingest_s25 done | rows={len(df_prm_master):,}")

    print("[2/4] build_jobs…", flush=True)
    jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)
    t = step(t, f"build_jobs done | jobs={len(jobs):,}")

    BUCKET_MINUTES = 15

    print("[3/4] build_tau + classes + spin_removed…", flush=True)
    tau = build_tau_from_jobs(jobs, toggles)
    classes = build_vehicle_classes(include_future=True)
    N_AMB = sum(int(c["count"]) for c in classes.get("Amb", []))
    spin_removed = build_spin_minutes(
        jobs,
        spin_lock_threshold_mins=50,
        n_ambulifts=N_AMB,
        bucket_minutes=BUCKET_MINUTES,
    )
    t = step(t, f"params built | N_AMB={N_AMB}")

    print("[4/4] pre_solve_debug (no solver)…", flush=True)
    pre_solve_debug(
        jobs=jobs,
        toggles=toggles,
        tau=tau,
        spin_removed=spin_removed,
        classes=classes,
        bucket_minutes=BUCKET_MINUTES,
        M_BIG=10_000,
        lift_cycle_mins=LIFT_CYCLE_MINS,
        lift_capacity_mins=LIFT_CAPACITY_MINS,
    )
    t = step(t, "pre_solve_debug done")

    # -------------------------
    # Sensitivity loop: model build + solve (REPEATED)
    # -------------------------


    # Sensitivity dimension:
    # Increasing vertical_cycle_mins models sequential vertical handling
    # when wheelchair demand exceeds per-cycle capacity.


    
    runs = []

    for vcm in vertical_cycle_grid:
        print("\n" + "-" * 70)
        print(f"[SENS] vertical_cycle_mins = {vcm}")
        print("-" * 70)

        toggles_local = replace(toggles, vertical_cycle_mins=float(vcm))

        model = build_pyomo_model(
            jobs=jobs,
            tau=tau,
            spin_removed=spin_removed,
            toggles=toggles_local,
        )

        solver = pyo.SolverFactory(solver_name)
        solver.options["time_limit"] = float(time_limit_sec)
        solver.options["threads"] = int(threads)
        if mip_rel_gap is not None:
            solver.options["mip_rel_gap"] = float(mip_rel_gap)

        results = solver.solve(model, tee=True, load_solutions=False)

        tc = str(results.solver.termination_condition).lower()
        st = str(results.solver.status).lower()

        loaded = False
        try:
            model.solutions.load_from(results)
            loaded = True
        except Exception as e:
            load_err = repr(e)

        run_out = {
            "vertical_cycle_mins": float(vcm),
            "tc": tc,
            "st": st,
            "status": "optimal" if tc in ("optimal", "feasible") else tc,
            "loaded_solution": loaded,
        }

        if loaded and (tc in ("optimal", "feasible") or ("timelimit" in tc)):
            run_out.update(
                _extract_and_dispose(model, jobs)
            )
        else:
            del model
            import gc
            gc.collect()

        runs.append(run_out)


    
    return {
        "status": "sensitivity_complete",
        "start": start,
        "end": end,
        "runs": runs,
    }



# DEBUG / DIAGNOSTIC FUNCTION ONLY
# --------------------------------
# This runner is used to diagnose infeasibility using LP relaxations.
# It is not part of standard S25/S26 scenario runs.


def run_s25_s2_lp(
    start,
    end,
    solver_name="highs",
    toggles: PlanningToggles = PlanningToggles(),
    run_lp_ladder: bool = True,
    lp_time_limit_sec: int = 240,
    solve_milp: bool = False,
    milp_time_limit_sec: int | None = None,
):
    """
    Scenario 2 runner using LP relaxation diagnostics.

    - run_lp_ladder:
        Runs an LP-relax feasibility ladder across constraint families.
    - solve_milp:
        If True, runs the full MILP solve after LP checks.
        If False, returns after diagnostics (recommended while debugging infeasibility).
    """

    # -------------------------
    # 1) Ingest + build jobs
    # -------------------------
    df_prm_master = ingest_s25(start, end)
    jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)
    print(jobs.head())

    # -------------------------
    # 2) Params
    # -------------------------
    BUCKET_MINUTES = 15
    tau = build_tau_from_jobs(jobs, toggles)

    classes = build_vehicle_classes(include_future=True)
    print(classes)

    N_AMB = sum(int(c["count"]) for c in classes.get("Amb", []))
    spin_removed = build_spin_minutes(
        jobs,
        spin_lock_threshold_mins=50,
        n_ambulifts=N_AMB,
        bucket_minutes=BUCKET_MINUTES,
    )

    # -------------------------
    # 3) Build model (once)
    # -------------------------
    model = build_pyomo_model(
        jobs=jobs,
        tau=tau,
        spin_removed=spin_removed,
        toggles=toggles,
    )

    print("Vars:", model.nvariables())
    print("Cons:", model.nconstraints())

    
    # -------------------------
    # 4) Build LP relaxation ONCE (fastest approach)
    # -------------------------
    lp_model = make_lp_relaxation_model(model)

    # LP baseline: solve the relaxed model directly (no extra clone)
    solver_lp = pyo.SolverFactory(solver_name)
    solver_lp.options["time_limit"] = float(lp_time_limit_sec) 
    solver_lp.options["threads"] = 8                           
    res_lp = solver_lp.solve(lp_model, tee=False, load_solutions=False)

    tc_lp = str(res_lp.solver.termination_condition).lower()
    st_lp = str(res_lp.solver.status).lower()
    print("\n[lp-baseline] termination_condition:", tc_lp, "| status:", st_lp)

    ladder = None
    if run_lp_ladder:
        groups = [
            ("none (baseline)", []),
            ("NoServiceAfterDeadline", ["NoServiceAfterDeadline"]),
            ("AmbTimeCap", ["AmbTimeCap"]),
            ("MiniTimeCap", ["MiniTimeCap"]),
            ("FleetExclusive", ["FleetExclusive"]),
            ("AmbSeat+Wc", ["AmbSeatCap", "AmbWcCap"]),
            ("MiniSeat+Wc", ["MiniSeatCap", "MiniWcCap"]),
            ("LiftCap", ["LiftCap"]),
            ("Eligibility rules", ["SafetyStand", "NoAmbDomesticArrivals"]),
        ]
        ladder = lp_ladder_single_relax(
            lp_model,
            groups,
            solver_name=solver_name,
            time_limit_sec=lp_time_limit_sec,
            threads=8,
        )


    # Stop here if you only want diagnostics
    if not solve_milp:
        return {
            "status": "lp_diagnostics_only",
            "jobs": jobs,
            "model": model,
            "lp_baseline": {"termination": tc_lp, "status": st_lp},
            "lp_ladder": ladder,
        }

    # -------------------------
    # 5) Full MILP solve (optional)
    # -------------------------
    solver = pyo.SolverFactory(solver_name)
    if milp_time_limit_sec is not None:
        solver.options["time_limit"] = int(milp_time_limit_sec)

    results = solver.solve(model, tee=True, load_solutions=False)

    print("solver_status:", results.solver.status)
    print("termination_condition:", results.solver.termination_condition)

    tc = str(results.solver.termination_condition).lower()
    if tc in ["infeasible", "infeasibleorunbounded"]:
        return {
            "status": "infeasible",
            "jobs": jobs,
            "model": model,
            "results": results,
            "lp_baseline": {"termination": tc_lp, "status": st_lp},
            "lp_ladder": ladder,
        }

    if tc not in ("optimal", "feasible"):
        return {
            "status": tc,
            "jobs": jobs,
            "model": model,
            "results": results,
            "lp_baseline": {"termination": tc_lp, "status": st_lp},
            "lp_ladder": ladder,
        }

    model.solutions.load_from(results)
    return {
        "status": "optimal",
        "jobs": jobs,
        "model": model,
        "results": results,
        "lp_baseline": {"termination": tc_lp, "status": st_lp},
        "lp_ladder": ladder,
    }


def run_month_s25(month_start, toggles):
    month_start_str = month_start.strftime("%Y-%m-%d")
    month_end_str = (month_start + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")

    print(f"\nRunning S25 {month_start.strftime('%Y-%m')}")

    # Scenario 1
    out_s1 = run_s25_s1(month_start_str, month_end_str, toggles=toggles)

    # Scenario 2
    sens = run_s25_s2_v2_sensitivity(
        start=month_start_str,
        end=month_end_str,
        vertical_cycle_grid=[10],   # ✅ must be list
        solver_name="highs",
        toggles=toggles,
        time_limit_sec=3600,
        threads=8,
        mip_rel_gap=0.10,
    )

    r = sens["runs"][0]

    report_s1 = build_run_report_s1(out_s1)
    report_s2 = build_run_report(r)

    return {
        "month": month_start.strftime("%Y-%m"),

        "S1_PeakAmb": report_s1["summary"]["PeakAmb"],
        "S1_PeakMini": report_s1["summary"]["PeakMini"],

        "S2_PeakAmb": report_s2["summary"]["PeakAmb"],
        "S2_PeakMini": report_s2["summary"]["PeakMini"],

        # ✅ SLA breakdown (important)
        "SLA_all": report_s2["summary"]["SLA_all"],
        "SLA_arr": report_s2["summary"]["SLA_arr"],
        "SLA_dep": report_s2["summary"]["SLA_dep"],
    }
