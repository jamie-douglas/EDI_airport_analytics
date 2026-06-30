
# scripts/prm_opt/run_s26.py

import time
import pandas as pd
import numpy as np
import pyomo.environ as pyo
from dataclasses import replace

from modules.utils.progress import step

from .ingest_s26 import ingest_s26
from .build_jobs import build_jobs
from .policy_s1 import apply_policy_s1
from .params import build_tau_from_jobs, build_spin_minutes, build_vehicle_classes
from .pyomo_model_v2 import build_pyomo_model
from .config import LIFT_CAPACITY_MINS, LIFT_CYCLE_MINS, PlanningToggles

from .debug_tools import pre_solve_debug, feasibility_ladder, check_job_time_windows
from prm_opt.outputs import build_run_report, build_run_report_s1
from prm_opt.run_s25 import run_s25_s1, run_s25_s2_v2_sensitivity

from .outputs import (
    extract_summary,
    extract_job_assignments,
    extract_vehicle_allocations,
    run_sanity_checks,
    baseline_s1_summary,
    baseline_s1_vehicle_curves_capacity,
)


"""
Scenario runners for S26 (forecast / simulated data).

This file mirrors S25 runner structure:
- Scenario 1 (S1): Policy-based baseline using learned S25 decision rules
- Scenario 2 (S2): Optimisation-based fleet sizing using Pyomo Model v2
- Optional sensitivity analysis for vertical_cycle_mins

Key design principle:
S25 and S26 share the same downstream logic (build_jobs → model → outputs).
Only ingestion differs.
"""


def _extract_and_dispose(model, jobs):
    """
    Extract all reporting outputs from a solved model and dispose the Pyomo model.
    This is essential for sensitivity loops to avoid accumulating multiple large models in memory.
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

    # Free the large model object (critical in loops / notebooks)
    del model
    import gc
    gc.collect()

    return out


# =========================================================
# Scenario 1 (S26 baseline via S25 policy rules)
# =========================================================
def run_s26_s1(
    start,
    end,
    penetration_rates: pd.DataFrame,
    ssr_mix: pd.DataFrame,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    service_time_params: pd.DataFrame,
    tau_mode_params: pd.DataFrame,
    chocks_offset_params: pd.DataFrame,
    *,
    early_late_std_mins: float = 15.0,
    seed: int = 42,
    turnaround_max_gap_mins: int = 240,
    spin_window_mins: int = 120,
    toggles: PlanningToggles = PlanningToggles(),
):
    """
    Scenario 1 for S26:
    - Build simulated passenger-level PRM jobs (ingest_s26)
    - Build canonical jobs table (build_jobs)
    - Apply S25 decision-tree policy (apply_policy_s1)
    - Convert decisions to fleet/staff curves (capacity-aware baseline)
    """

    df_master = ingest_s26(
        start=start,
        end=end,
        penetration_rates=penetration_rates,
        ssr_mix=ssr_mix,
        stand_actuals=stand_actuals,
        stand_dist=stand_dist,
        service_time_params=service_time_params,
        tau_mode_params=tau_mode_params,
        chocks_offset_params=chocks_offset_params,
        early_late_std_mins=early_late_std_mins,
        seed=seed,
        turnaround_max_gap_mins=turnaround_max_gap_mins,
        spin_window_mins=spin_window_mins,
    )

    jobs = build_jobs(df_master, bucket="15min", toggles=toggles, use_90th_percentile_cap=False)

    decisions = apply_policy_s1(jobs)
    jobs["s1_decision"] = jobs.index.map(decisions.get)

    # curves = baseline_s1_vehicle_curves_capacity(
    #     jobs,
    #     decision_col="s1_decision",
    #     bucket_col="s",
    #     count_no_vehicle_as_push=True,
    #     amb_seatcap=3,
    #     amb_wccap=1,
    #     mini_seatcap=6,
    #     mini_wccap=2,
    # )

    
    curves = baseline_s1_vehicle_curves_capacity(
        jobs,
        decision_col="s1_decision",
        bucket_col="s",
        count_no_vehicle_as_push=True,
        amb_seatcap=3,
        amb_wccap=1,
        mini_seatcap=6,
        mini_wccap=2,
        duration_mins=20,
        time_freq="5min",
    )


    classes = build_vehicle_classes(include_future=False)
    current_amb = sum(int(c["count"]) for c in classes.get("Amb", []))
    current_mini = sum(int(c["count"]) for c in classes.get("Mini", []))

    summary = baseline_s1_summary(jobs, curves, current_amb=current_amb, current_mini=current_mini)

    return {
        "jobs": jobs,
        "summary": summary,
        **{k: curves[k] for k in ["ambulift_curve", "minibus_curve", "driver_curve", "veh_agent_curve", "pusher_curve"]},
        "fb_detail": curves["fb_detail"],
    }


# =========================================================
# Scenario 2 (S26 optimisation, Model v2)
# =========================================================
def run_s26_s2_v2(
    start,
    end,
    penetration_rates: pd.DataFrame,
    ssr_mix: pd.DataFrame,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    service_time_params: pd.DataFrame,
    tau_mode_params: pd.DataFrame,
    chocks_offset_params: pd.DataFrame,
    *,
    early_late_std_mins: float = 15.0,
    seed: int = 42,
    turnaround_max_gap_mins: int = 240,
    spin_window_mins: int = 120,
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
    Scenario 2 (v2) for S26:
    Mirrors the S25 Scenario 2 runner structure:
      1) ingest_s26
      2) build_jobs
      3) build_tau_from_jobs + build_vehicle_classes + build_spin_minutes
      4) pre_solve_debug (no solver)
      5) build_pyomo_model (v2)
      6) solve + extract outputs
    """

    print("\nPRM OPT — S26 Scenario 2 (v2)")
    print(f"Window : {start} → {end}\n")

    t = time.perf_counter()

    # --------------------------------------------------
    # STEP 1: Ingest S26 and build PRM jobs
    # --------------------------------------------------
    print("[1/6] ingest_s26…", flush=True)
    df_master = ingest_s26(
        start=start,
        end=end,
        penetration_rates=penetration_rates,
        ssr_mix=ssr_mix,
        stand_actuals=stand_actuals,
        stand_dist=stand_dist,
        service_time_params=service_time_params,
        tau_mode_params=tau_mode_params,
        chocks_offset_params=chocks_offset_params,
        early_late_std_mins=early_late_std_mins,
        seed=seed,
        turnaround_max_gap_mins=turnaround_max_gap_mins,
        spin_window_mins=spin_window_mins,
    )
    t = step(t, f"ingest_s26 done | rows={len(df_master):,}")

    print("[2/6] build_jobs…", flush=True)
    jobs = build_jobs(df_master, bucket="15min", toggles=toggles, use_90th_percentile_cap=False)
    t = step(t, f"build_jobs done | jobs={len(jobs):,}")

    # --------------------------------------------------
    # STEP 2: Build time / fleet parameters
    # --------------------------------------------------
    BUCKET_MINUTES = 15
    _ = check_job_time_windows(jobs, BUCKET_MINUTES, max_late_mins=int(getattr(toggles, "max_late_mins", 180) or 180))

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
    # STEP 4: Build model
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

    # Optional infeasibility ladder (debug only)
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

    if tc in ("infeasible", "infeasibleorunbounded"):
        return {
            "status": "infeasible",
            "tc": tc, "st": st,
            "model": model, "jobs": jobs, "results": results,
            "ladder": ladder_summary,
            "loaded_solution": loaded,
            "load_error": load_err,
        }

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

    return {
        "status": tc,
        "tc": tc, "st": st,
        "model": model, "jobs": jobs, "results": results,
        "ladder": ladder_summary,
        "loaded_solution": loaded,
        "load_error": load_err,
    }


# =========================================================
# Optional: S26 sensitivity wrapper (memory-safe)
# =========================================================
def run_s26_s2_v2_sensitivity(
    start,
    end,
    penetration_rates: pd.DataFrame,
    ssr_mix: pd.DataFrame,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    service_time_params: pd.DataFrame,
    tau_mode_params: pd.DataFrame,
    chocks_offset_params: pd.DataFrame,
    *,
    vertical_cycle_grid=(0, 5, 10, 15),
    early_late_std_mins: float = 15.0,
    seed: int = 42,
    turnaround_max_gap_mins: int = 240,
    spin_window_mins: int = 120,
    solver_name="highs",
    toggles: PlanningToggles = PlanningToggles(),
    time_limit_sec: int = 600,
    threads: int = 8,
    mip_rel_gap: float | None = None,
):
    """
    Memory-safe sensitivity loop for S26 Scenario 2 (v2).
    Mirrors S25 sensitivity design: ingest/build once, solve per vcm, extract+dispose each model.
    """

    print("\nPRM OPT — S26 Scenario 2 (v2) — Sensitivity")
    print(f"Window : {start} → {end}")
    print(f"vertical_cycle_grid: {list(vertical_cycle_grid)}\n")

    t = time.perf_counter()

    print("[1/4] ingest_s26…", flush=True)
    df_master = ingest_s26(
        start=start,
        end=end,
        penetration_rates=penetration_rates,
        ssr_mix=ssr_mix,
        stand_actuals=stand_actuals,
        stand_dist=stand_dist,
        service_time_params=service_time_params,
        tau_mode_params=tau_mode_params,
        chocks_offset_params=chocks_offset_params,
        early_late_std_mins=early_late_std_mins,
        seed=seed,
        turnaround_max_gap_mins=turnaround_max_gap_mins,
        spin_window_mins=spin_window_mins,
    )
    t = step(t, f"ingest_s26 done | rows={len(df_master):,}")

    print("[2/4] build_jobs…", flush=True)
    jobs = build_jobs(df_master, bucket="15min", toggles=toggles, use_90th_percentile_cap=False)
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
        solver.options["user_objective_scale"] = -3
        solver.options["mip_heuristic_effort"] = 1.0
        if mip_rel_gap is not None:
            solver.options["mip_rel_gap"] = float(mip_rel_gap)

        results = solver.solve(model, tee=True, load_solutions=False)

        tc = str(results.solver.termination_condition).lower()
        st = str(results.solver.status).lower()

        loaded = False
        load_err = None
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
            "load_error": load_err,
        }

        if loaded and (tc in ("optimal", "feasible") or ("timelimit" in tc)):
            run_out.update(_extract_and_dispose(model, jobs))
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


def run_month_s26(month_start, assumptions, toggles):
    month_start_str = month_start.strftime("%Y-%m-%d")
    month_end_str = (month_start + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")

    cutoff = pd.Timestamp.today().normalize()  # ✅ dynamic cutoff

    print(f"\nRunning S26 {month_start.strftime('%Y-%m')} | cutoff={cutoff.date()}")

    # --------------------------------------------------
    # CASE 1: FULLY HISTORICAL
    # --------------------------------------------------
    if month_start < cutoff and (month_start + pd.offsets.MonthBegin(1)) <= cutoff:
        print("→ HISTORICAL ONLY")

        out_s1 = run_s25_s1(month_start_str, month_end_str, toggles=toggles)

        sens = run_s25_s2_v2_sensitivity(
            start=month_start_str,
            end=month_end_str,
            vertical_cycle_grid=[10],
            solver_name="highs",
            toggles=toggles,
            time_limit_sec=3600,
            threads=8,
            mip_rel_gap=0.30,
        )

    # --------------------------------------------------
    # CASE 2: FULLY FORECAST
    # --------------------------------------------------
    elif month_start >= cutoff:
        print("→ FORECAST ONLY")

        out_s1 = run_s26_s1(
            start=month_start_str,
            end=month_end_str,
            **assumptions["inputs"],
            toggles=toggles,
        )

        sens = run_s26_s2_v2_sensitivity(
            start=month_start_str,
            end=month_end_str,
            **assumptions["inputs"],
            vertical_cycle_grid=[10],
            solver_name="highs",
            toggles=toggles,
            time_limit_sec=600,
            threads=8,
            mip_rel_gap=0.30,
        )

    # --------------------------------------------------
    # CASE 3: SPLIT MONTH (CURRENT MONTH)
    # --------------------------------------------------
    else:
        print("→ SPLIT MONTH")

        mid = cutoff.strftime("%Y-%m-%d")

        out_fcst = run_s26_s1(
            start=mid,
            end=month_end_str,
            **assumptions["inputs"],
            toggles=toggles,
        )

        out_s1 = out_fcst  # ✅ practical simplification

        sens = run_s26_s2_v2_sensitivity(
            start=mid,
            end=month_end_str,
            **assumptions["inputs"],
            vertical_cycle_grid=[10],
            solver_name="highs",
            toggles=toggles,
            time_limit_sec=3600,
            threads=8,
            mip_rel_gap=0.10,
        )

    # --------------------------------------------------
    # Extract outputs
    # --------------------------------------------------
    r = sens["runs"][0]

    report_s1 = build_run_report_s1(out_s1)
    report_s2 = build_run_report(r)
    s2_summary = report_s2.get("summary", {})
    has_s2_summary = bool(s2_summary)

    s2_peak_amb = s2_summary.get("PeakAmb")
    s2_peak_mini = s2_summary.get("PeakMini")
    sla_all = s2_summary.get("SLA_all")
    sla_arr = s2_summary.get("SLA_arr")
    sla_dep = s2_summary.get("SLA_dep")
    allowed_breaches = s2_summary.get("allowed_breaches")
    actual_breaches = s2_summary.get("actual_breaches")
    sla_percent = s2_summary.get("sla_percent")
    sla_floor_slack = s2_summary.get("sla_floor_slack")

    
    # --------------------------------------------------
    # Print per-month summary (incremental visibility)
    # --------------------------------------------------
    print("\n================ MONTH SUMMARY =================")
    print(f"Month: {month_start.strftime('%Y-%m')} | Cutoff: {cutoff.date()}")

    print("Scenario 1:")
    print(
        f"  PeakAmb={report_s1['summary']['PeakAmb']} | "
        f"PeakMini={report_s1['summary']['PeakMini']}"
    )

    print("Scenario 2:")
    if has_s2_summary:
        print(f"  PeakAmb={s2_peak_amb} | PeakMini={s2_peak_mini}")
    else:
        print(
            f"  No S2 summary available (status={report_s2.get('status')}, "
            f"tc={report_s2.get('tc')}, st={report_s2.get('st')})"
        )

    print("SLA:")
    if sla_all is not None and sla_arr is not None and sla_dep is not None:
        print(f"  All={sla_all:.4f} | Arr={sla_arr:.4f} | Dep={sla_dep:.4f}")
    else:
        print("  All=None | Arr=None | Dep=None")

    print("SLA diagnostics:")
    print(
        f"  allowed_breaches={allowed_breaches} | "
        f"actual_breaches={actual_breaches} | "
        f"sla_percent={sla_percent:.2f}% | "
        f"sla_floor_slack={sla_floor_slack:.2f}"
        if sla_percent is not None and sla_floor_slack is not None
        else (
            f"  allowed_breaches={allowed_breaches} | "
            f"actual_breaches={actual_breaches} | "
            f"sla_percent={sla_percent} | "
            f"sla_floor_slack={sla_floor_slack}"
        )
    )

    if bool(getattr(toggles, "enforce_hard_sla_floor", False)) and (sla_floor_slack is not None) and (float(sla_floor_slack) > 0):
        print(f"  WARNING: hard SLA floor relaxed by slack on {float(sla_floor_slack):.2f} jobs")
    print("================================================\n")


    return {
        "month": month_start.strftime("%Y-%m"),
        "cutoff_used": cutoff.strftime("%Y-%m-%d"),

        "S1_PeakAmb": report_s1["summary"]["PeakAmb"],
        "S1_PeakMini": report_s1["summary"]["PeakMini"],

        "S2_PeakAmb": s2_peak_amb,
        "S2_PeakMini": s2_peak_mini,

        "SLA_all": sla_all,
        "SLA_arr": sla_arr,
        "SLA_dep": sla_dep,

        "allowed_breaches": allowed_breaches,
        "actual_breaches": actual_breaches,
        "sla_percent": sla_percent,
        "sla_floor_slack": sla_floor_slack,

        "S2_status": report_s2.get("status"),
        "S2_tc": report_s2.get("tc"),
        "S2_st": report_s2.get("st"),
    }
