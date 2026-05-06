
# scripts/prm_opt/run_s25.py

from tracemalloc import start

import pandas as pd
import numpy as np
import math
import time

"""
Runs S25 scenarios.
"""

import pyomo.environ as pyo
from pyomo.core.base import TransformationFactory
from modules.utils.progress import step


from .ingest_s25 import ingest_s25
from .build_jobs import build_jobs
from .policy_s1 import apply_policy_s1
from .params import build_tau_from_jobs, build_spin_minutes, build_vehicle_classes
from .pyomo_model_legacy import build_pyomo_model
#from .pyomo_model import build_pyomo_model
from .config import LIFT_CAPACITY_MINS, LIFT_CYCLE_MINS, PlanningToggles

from .outputs import (
    extract_summary,
    extract_job_assignments,
    extract_vehicle_allocations,
    run_sanity_checks,
    baseline_s1_summary,
    baseline_s1_vehicle_curves
)

def pre_solve_debug(
    jobs: pd.DataFrame,
    toggles,
    tau: dict | None = None,
    spin_removed: dict | None = None,
    classes: dict | None = None,
    bucket_minutes: int = 15,
    M_BIG: int = 10_000,
    lift_cycle_mins: float | None = None,
    lift_capacity_mins: float | None = None,
):
    """
    Pre-solver feasibility checks (fast necessary-condition checks).
    Does NOT solve. Prints actionable diagnostics.
    """
    print("\n" + "="*70)
    print("PRE-SOLVER FEASIBILITY DEBUG (no solver)")
    print("="*70)

    # ---- basic sanity ----
    required = ["t", "s", "dir", "sla_limit", "sla_start_time"]
    missing = [c for c in required if c not in jobs.columns]
    if missing:
        print("❌ Missing required columns:", missing)
        return {"ok": False, "missing_columns": missing}
    print("✅ Required columns present.")

    # ---- missingness / join health ----
    miss = {
        "sla_start_time": int(jobs["sla_start_time"].isna().sum()),
        "Scheduled Flight DT": int(jobs["Scheduled Flight DT"].isna().sum()) if "Scheduled Flight DT" in jobs.columns else None,
        "Chocks DT": int(jobs["Chocks DT"].isna().sum()) if "Chocks DT" in jobs.columns else None,
    }
    print("Missingness:", miss)

    # quick “which airlines are missing flight anchors?” view
    if "Airline Code" in jobs.columns:
        flag_missing_anchor = pd.Series(False, index=jobs.index)
        if "Scheduled Flight DT" in jobs.columns:
            flag_missing_anchor |= jobs["Scheduled Flight DT"].isna()
        if "Chocks DT" in jobs.columns:
            flag_missing_anchor |= jobs["Chocks DT"].isna()
        if flag_missing_anchor.any():
            by_air = (
                jobs.loc[flag_missing_anchor]
                .groupby(["Airline Code", "dir"])
                .size()
                .sort_values(ascending=False)
                .head(20)
            )
            print("\nTop missing-flight-anchor by Airline Code + dir (top 20):")
            print(by_air)

    # ---- timeline coverage check (bucket horizon) ----
    t_min = pd.to_datetime(jobs["t"]).min()
    s_max = pd.to_datetime(jobs["s"]).max()
    max_sla = float(pd.to_numeric(jobs["sla_limit"]).max())

    sla_min = pd.to_datetime(jobs["sla_start_time"]).dropna().min()
    start_time = min(t_min, sla_min).floor(f"{bucket_minutes}min")

    horizon_slack_mins = int(getattr(toggles, "horizon_slack_mins", 240) or 240)
    end_time = s_max.floor(f"{bucket_minutes}min") + pd.to_timedelta(
        int((max_sla + horizon_slack_mins) // bucket_minutes + 2) * bucket_minutes, unit="m"
    )
    B_list = pd.date_range(start=start_time, end=end_time, freq=f"{bucket_minutes}min")

    print(f"\nBucket horizon: start={start_time} end={end_time} buckets={len(B_list)}")

    t_bucket = pd.to_datetime(jobs["t"]).dt.floor(f"{bucket_minutes}min")
    sla_bucket = pd.to_datetime(jobs["sla_start_time"]).dt.floor(f"{bucket_minutes}min")

    if (~t_bucket.isin(B_list)).any():
        ex = t_bucket[~t_bucket.isin(B_list)].iloc[0]
        print("❌ Some t buckets not in horizon. Example:", ex)
    else:
        print("✅ All t buckets lie in horizon.")

    if (~sla_bucket.isin(B_list)).any():
        ex = sla_bucket[~sla_bucket.isin(B_list)].iloc[0]
        print("❌ Some sla_start buckets not in horizon. Example:", ex)
    else:
        print("✅ All sla_start buckets lie in horizon.")

    # ---- release after deadline (only relevant if you enforce deadline) ----
    if "hard_deadline_time" in jobs.columns:
        d_bucket = pd.to_datetime(jobs["hard_deadline_time"]).dt.floor(f"{bucket_minutes}min")
        bad_deadline = jobs[(~d_bucket.isna()) & (t_bucket > d_bucket)]
        print("\nJobs with release bucket AFTER hard deadline:", len(bad_deadline))
        if len(bad_deadline) > 0:
            cols = ["Passenger ID","flight_key","dir","t","hard_deadline_time","Scheduled Flight DT","release_time"]
            cols = [c for c in cols if c in bad_deadline.columns]
            print(bad_deadline[cols].head(20))

    # ---- SLA Big-M impossible (even if y=1) ----
    # Condition: earliest possible delay (serve at t) must be <= L_j + M_BIG
    L = pd.to_numeric(jobs["sla_limit"]).astype(float)
    earliest_delay_mins = (t_bucket - sla_bucket).dt.total_seconds() / 60.0
    impossible_even_with_y = jobs[earliest_delay_mins > (L + M_BIG)]
    print("\nJobs impossible even with SLA breach y=1 (earliest_delay > L + M_BIG):", len(impossible_even_with_y))
    if len(impossible_even_with_y) > 0:
        tmp = impossible_even_with_y.copy()
        tmp["earliest_delay_mins"] = earliest_delay_mins.loc[tmp.index]
        cols = ["Passenger ID","flight_key","dir","t","sla_start_time","sla_limit","earliest_delay_mins"]
        cols = [c for c in cols if c in tmp.columns]
        print(tmp[cols].head(25))

    
    # ---- spin sanity: removed minutes cannot exceed capacity ----
    if spin_removed is not None and classes is not None:
        n_amb = sum(int(c["count"]) for c in classes.get("Amb", []))
        cap_per_bucket = float(bucket_minutes * n_amb)

        # Strictly impossible (should never happen)
        bad_spin = [(b, float(v)) for b, v in spin_removed.items() if float(v) > cap_per_bucket + 1e-9]
        print("\nspin_removed > total amb minutes (impossible):", len(bad_spin))
        if bad_spin:
            print("Examples:", bad_spin[:10])

        # **NEW DEBUG**: full saturation (available ambulift minutes = 0)
        sat_spin = [(b, float(v)) for b, v in spin_removed.items() if float(v) >= cap_per_bucket - 1e-9]
        print("spin_removed >= total amb minutes (SATURATED -> available=0):", len(sat_spin))
        if sat_spin:
            print("Examples:", sat_spin[:10])

        # Helpful overview: top buckets by spin_removed
        top = sorted(((b, float(v)) for b, v in spin_removed.items()), key=lambda x: x[1], reverse=True)[:10]
        print("Top 10 spin_removed buckets (bucket, removed_mins, cap_mins):")
        for b, v in top:
            print(" ", b, "|", round(v, 1), "/", round(cap_per_bucket, 1))


    # ---- very rough necessary capacity checks (optional) ----
    if tau is not None and classes is not None:
        n_amb = sum(c["count"] for c in classes.get("Amb", []))
        n_mini = sum(c["count"] for c in classes.get("Mini", []))

        # (a) minimum ambulift minutes from vertical-only workload
        if "needs_vertical" in jobs.columns:
            min_amb = float(sum(float(tau[(j, "Amb")]) for j in jobs.index if int(jobs.loc[j, "needs_vertical"]) == 1))
            total_amb = float(n_amb * bucket_minutes * len(B_list))
            if spin_removed is not None:
                total_amb -= float(sum(float(spin_removed.get(b, 0.0)) for b in B_list))
            print("\nAmb minutes (necessary condition):")
            print("  min required (vertical-only):", round(min_amb, 1))
            print("  total available over horizon:", round(total_amb, 1))
            if min_amb > total_amb:
                print("❌ INFEASIBLE: even vertical-only amb minutes exceed total amb capacity.")

        # (b) forced-minibus jobs (Amb blocked + Push blocked)
        if all(c in jobs.columns for c in ["safety_stand","needs_vertical","Airline Code","class","dir"]):
            blocked_push = (jobs["safety_stand"] == 1) & (jobs["needs_vertical"] == 1) & (~jobs["Airline Code"].isin(["FR","RY"]))
            blocked_amb = (jobs["class"] == "Dom") & (jobs["dir"] == "A")
            must_mini = blocked_push & blocked_amb
            forced = int(must_mini.sum())
            if forced > 0:
                min_mini = float(sum(float(tau[(j, "Mini")]) for j in jobs.index if bool(must_mini.loc[j])))
                total_mini = float(n_mini * bucket_minutes * len(B_list))
                print("\nMinibus forced-only (necessary condition):")
                print("  forced Mini jobs:", forced)
                print("  min mini minutes required (forced):", round(min_mini, 1))
                print("  total mini minutes available:", round(total_mini, 1))
                if min_mini > total_mini:
                    print("❌ INFEASIBLE: forced-minibus minutes exceed total minibus capacity.")

    # ---- lift necessary check ----
    if lift_cycle_mins is not None and lift_capacity_mins is not None and "lift_gate" in jobs.columns and "needs_wc" in jobs.columns:
        lift_jobs = jobs[(jobs["lift_gate"] == 1) & (jobs["needs_wc"] == 1)]
        max_per_bucket = int(np.floor(float(lift_capacity_mins) / float(lift_cycle_mins))) if lift_cycle_mins else 0
        print("\nLift rough check:")
        print("  lift jobs:", len(lift_jobs))
        print("  max jobs per bucket:", max_per_bucket)
        if max_per_bucket == 0 and len(lift_jobs) > 0:
            print("❌ INFEASIBLE: lift capacity per bucket is 0 but lift jobs exist.")

    
    ferry_mini_reserved = getattr(toggles, "ferry_mini_reserved", {}) or {}
    if ferry_mini_reserved:
        # total mini count (including future)
        total_mini = sum(c["count"] for c in classes.get("Mini", []))
        bad = [(b, int(v)) for b, v in ferry_mini_reserved.items() if int(v) > total_mini]
        print("\nFerry mini reservation:")
        print("  buckets with reservation:", len(ferry_mini_reserved))
        print("  max reserved:", max(int(v) for v in ferry_mini_reserved.values()))
        if bad:
            print("❌ INFEASIBLE: reserved > total mini available in bucket. Examples:", bad[:10])
    else:
        print("\nFerry mini reservation: none")

    
    # Mode eligibility checks (necessary condition)
    # Amb forbidden only for Dom arrivals.
    amb_allowed = ~((jobs["class"] == "Dom") & (jobs["dir"] == "A"))

    # Push forbidden for safety_stand + vertical + not Ryanair.
    push_allowed = ~((jobs["safety_stand"] == 1) & (jobs["needs_vertical"] == 1) & (~jobs["Airline Code"].isin(["FR","RY"])))

    # Mini generally allowed in your model (no hard ban)
    mini_allowed = pd.Series(True, index=jobs.index)

    allowed_count = amb_allowed.astype(int) + push_allowed.astype(int) + mini_allowed.astype(int)
    bad = jobs[allowed_count == 0]
    print("\nJobs with ZERO feasible horizontal modes:", len(bad))
    if len(bad) > 0:
        print(bad[["Passenger ID","flight_key","dir","class","Airline Code","safety_stand","needs_vertical"]].head(20))

    
    missing_tau = []
    for j in jobs.index:
        for mm in ["Amb","Mini","Push"]:
            if (j, mm) not in tau:
                missing_tau.append((j, mm))
    print("\nMissing tau keys:", len(missing_tau))
    if missing_tau:
        print("Examples:", missing_tau[:10])

    
    if spin_removed:
        bad_spin_keys = [b for b in spin_removed.keys() if pd.to_datetime(b).floor(f"{bucket_minutes}min") != pd.to_datetime(b)]
        print("\nspin_removed keys off-grid:", len(bad_spin_keys))
        if bad_spin_keys:
            print("Example:", bad_spin_keys[0])

    
   
    print("\n" + "="*70)
    print("TIGHT-WINDOW FEASIBILITY CHECKS (2A–2E) — NO SOLVER")
    print("="*70)

    # -----------------------------
    # 2A) Build bucket grid / indices consistent with the model
    # -----------------------------
    # If you already have B_list in your debug, reuse it.
    # Otherwise, rebuild it from your horizon start/end.
    B_list = list(pd.date_range(start=start_time, end=end_time, freq=f"{bucket_minutes}min"))
    b_to_idx = {b: i for i, b in enumerate(B_list)}

    t_bucket = pd.to_datetime(jobs["t"]).dt.floor(f"{bucket_minutes}min")

    # Latest feasible bucket (deadline for D jobs, horizon-end for others)
    if "hard_deadline_time" in jobs.columns:
        d_bucket = pd.to_datetime(jobs["hard_deadline_time"]).dt.floor(f"{bucket_minutes}min")
    else:
        d_bucket = pd.Series(pd.NaT, index=jobs.index)

    latest_bucket = pd.Series(pd.NaT, index=jobs.index)
    latest_bucket.loc[jobs["dir"] == "D"] = d_bucket.loc[jobs["dir"] == "D"]
    latest_bucket.loc[jobs["dir"] != "D"] = B_list[-1]  # arrivals/others

    # Make sure latest buckets lie on the grid (otherwise show them)
    off_grid_deadline = latest_bucket.notna() & (~latest_bucket.isin(B_list))
    print("Deadlines off bucket grid (count):", int(off_grid_deadline.sum()))
    if off_grid_deadline.any():
        cols = ["Passenger ID", "flight_key", "dir", "hard_deadline_time", "t"]
        cols = [c for c in cols if c in jobs.columns]
        print(jobs.loc[off_grid_deadline, cols].head(20))

    # -----------------------------
    # 2B) Identify "tight window" jobs: earliest==latest (forced into one bucket)
    # -----------------------------
    tight = (t_bucket == latest_bucket) & latest_bucket.notna()
    print("\nTight-window jobs (must be served in exactly one bucket):", int(tight.sum()))
    if tight.any():
        cols = ["Passenger ID", "flight_key", "dir", "t", "hard_deadline_time", "sla_start_time", "Stand", "Airline Code"]
        cols = [c for c in cols if c in jobs.columns]
        print("Examples (first 20):")
        print(jobs.loc[tight, cols].head(20))

    # -----------------------------
    # 2C) Tight-window lift feasibility check
    # If too many lift-jobs are forced into the same bucket, LiftCap is infeasible.
    # -----------------------------
    if all(c in jobs.columns for c in ["lift_gate", "needs_wc"]):
        lift_jobs = (jobs["lift_gate"] == 1) & (jobs["needs_wc"] == 1)
        lift_tight = tight & lift_jobs

        # Lift capacity per bucket in "jobs per bucket"
        max_lift_per_bucket = int(np.floor(float(LIFT_CAPACITY_MINS) / float(LIFT_CYCLE_MINS))) if LIFT_CYCLE_MINS else 0
        by_bucket = lift_tight.groupby(t_bucket).sum()

        worst = int(by_bucket.max()) if len(by_bucket) else 0
        print("\nLift tight-window check:")
        print("  max lift jobs per bucket:", max_lift_per_bucket)
        print("  worst tight lift count in a bucket:", worst)

        viol = by_bucket[by_bucket > max_lift_per_bucket]
        if len(viol) > 0:
            print("❌ INFEASIBLE RISK: tight lift jobs exceed lift capacity in these buckets:")
            print(viol.head(10))
    else:
        print("\nLift tight-window check skipped (missing lift_gate/needs_wc).")

    
    # -----------------------------
    # 2D) Tight-window ambulift TIME feasibility check (vertical-only minimum)
    # -----------------------------
    N_AMB = sum(int(c["count"]) for c in classes.get("Amb", []))
    print("\nAmb tight-window time check (vertical-only):")
    print("  Total ambulifts (from classes):", int(N_AMB))

    if "needs_vertical" in jobs.columns:
        tight_vertical = tight & (jobs["needs_vertical"] == 1)

        req = {}  # bucket -> required amb minutes
        for j in jobs.index[tight_vertical]:
            b = t_bucket.loc[j]
            req[b] = req.get(b, 0.0) + float(tau[(j, "Amb")])

        infeas = []
        for b, mins in req.items():
            avail = bucket_minutes * N_AMB - float(spin_removed.get(b, 0.0))
            if mins > avail + 1e-6:
                infeas.append((b, mins, avail))

        print("  buckets with tight vertical demand:", len(req))

        # ---- NEW DEBUG: if infeasible, print spin_removed + culprits in that bucket ----
        if infeas:
            # Sort by worst shortfall first (most negative available - required)
            infeas_sorted = sorted(infeas, key=lambda x: (x[2] - x[1]))

            print("\nDetails for worst infeasible bucket(s):")
            for (b, mins, avail) in infeas_sorted[:5]:
                removed = float(spin_removed.get(b, 0.0))
                cap = bucket_minutes * N_AMB
                print(f"  bucket={b} | required={mins:.1f} | available={avail:.1f} | spin_removed={removed:.1f} | cap={cap:.1f}")

                culprits = jobs[tight_vertical & (t_bucket == b)].copy()
                cols = ["Passenger ID","flight_key","dir","Airline Code","Stand","t","hard_deadline_time","sla_start_time","needs_vertical"]
                cols = [c for c in cols if c in culprits.columns]
                print("  Culprit jobs (first 25):")
                print(culprits[cols].head(25))

            print("❌ INFEASIBLE RISK: vertical tight ambulift minutes exceed per-bucket capacity. Examples:")
            for row in infeas[:10]:
                print("   bucket:", row[0], "| required:", round(row[1], 1), "| available:", round(row[2], 1))
        else:
            print("✅ No per-bucket ambulift time overload from tight vertical jobs.")
    else:
        print("  skipped (needs_vertical missing).")


    # -----------------------------
    # 2E) Tight-window flight+bucket ambulift SEAT feasibility (vertical-only minimum)
    # Even if you assigned ALL ambulifts to one flight in a bucket, seatcap is finite.
    # If a single flight has too many vertical jobs forced into one bucket, AmbSeatCap can be infeasible.
    # -----------------------------
    total_amb_seats = sum(int(c["seatcap"]) * int(c["count"]) for c in classes.get("Amb", []))
    print("\nFlight+bucket vertical seat check (vertical-only):")
    print("  total ambulift seats in a bucket (if all ambs assigned to one flight):", int(total_amb_seats))

    if "needs_vertical" in jobs.columns and "flight_key" in jobs.columns:
        tight_vertical = tight & (jobs["needs_vertical"] == 1)
        grp = jobs.loc[tight_vertical].groupby([jobs.loc[tight_vertical, "flight_key"], t_bucket.loc[tight_vertical]])["needs_vertical"].sum()

        worst = int(grp.max()) if len(grp) else 0
        print("  worst tight vertical seats for a flight in a bucket:", worst)

        viol = grp[grp > total_amb_seats]
        if len(viol) > 0:
            print("❌ INFEASIBLE RISK: a flight has more tight vertical jobs in a bucket than ALL ambulifts can seat:")
            print(viol.head(10))
    else:
        print("  skipped (needs_vertical/flight_key missing).")

    print("\n✅ Tight-window checks complete.")




    print("\n✅ Pre-solver debug complete.")
    return {"ok": True, "missingness": miss}


def feasibility_ladder(model, solver, groups):
    """
    Solve multiple times with different constraint groups deactivated.
    Returns the first group whose deactivation restores feasibility.
    """
    
    results_summary = []

    def _deactivate(names):
        for n in names:
            if hasattr(model, n):
                getattr(model, n).deactivate()

    def _activate_all():
        for comp in model.component_objects(pyo.Constraint, active=False):
            comp.activate()

    for label, to_deactivate in groups:
        _activate_all()
        _deactivate(to_deactivate)

        t0 = time.perf_counter()
        res = solver.solve(model, tee=False, load_solutions=False)
        dt = time.perf_counter() - t0

        tc = str(res.solver.termination_condition).lower()
        st = str(res.solver.status).lower()

        print(f"[ladder] deactivate={label:25s} -> {tc} {st}  [{dt:0.2f}s]", flush=True)
        results_summary.append((label, tc, st, dt))

    _activate_all()
    return results_summary






def lp_relaxation_probe(model, solver_name="highs", time_limit_sec=30, threads=8, tee=False):
    """
    Solve LP relaxation of a MILP model (feasibility-only):
      - relax integer/binary vars to continuous
      - replace objective with 0
    Robust across Pyomo versions: tries core.relax_integer_vars, falls back to core.relax_integrality.
    Returns: (termination_condition_lower, solver_status_lower)
    """
    m = model.clone()

    # Pyomo compatibility: do not call .available(); just try new name then fallback
    try:
        TransformationFactory("core.relax_integer_vars").apply_to(m)
    except Exception:
        TransformationFactory("core.relax_integrality").apply_to(m)

    # Feasibility-only objective
    if hasattr(m, "OBJ"):
        try:
            m.OBJ.deactivate()
        except Exception:
            pass
    m.DUMMY_OBJ = pyo.Objective(expr=0.0)

    solver = pyo.SolverFactory(solver_name)
    solver.options["time_limit"] = float(time_limit_sec)
    solver.options["threads"] = int(threads)

    res = solver.solve(m, tee=tee, load_solutions=False)
    tc = str(res.solver.termination_condition).lower()
    st = str(res.solver.status).lower()
    return tc, st


def make_lp_relaxation_model(model):
    """
    Clone once, relax integer/binary vars once, and set a feasibility-only objective.
    This avoids repeating clone+relax for every ladder step.
    """
    m = model.clone()

    # Robust across Pyomo versions: try new name, fallback old
    try:
        TransformationFactory("core.relax_integer_vars").apply_to(m)
    except Exception:
        TransformationFactory("core.relax_integrality").apply_to(m)

    # Feasibility-only objective
    if hasattr(m, "OBJ"):
        try:
            m.OBJ.deactivate()
        except Exception:
            pass
    m.DUMMY_OBJ = pyo.Objective(expr=0.0)

    return m


def lp_ladder_single_relax(relaxed_model, groups, solver_name="highs", time_limit_sec=60, threads=8):
    """
    Run the constraint-family ladder on an already-relaxed model (fast).
    Only the HiGHS solve repeats; clone/relax does not.
    """
    solver = pyo.SolverFactory(solver_name)
    solver.options["time_limit"] = float(time_limit_sec) 
    solver.options["threads"] = int(threads)             

    def _activate_all():
        for comp in relaxed_model.component_objects(pyo.Constraint, active=False):
            comp.activate()

    def _deactivate(names):
        for n in names:
            if hasattr(relaxed_model, n):
                getattr(relaxed_model, n).deactivate()

    out = []
    print("\nLP-RELAX FEASIBILITY LADDER (single relax)")
    for label, to_deactivate in groups:
        _activate_all()
        _deactivate(to_deactivate)

        res = solver.solve(relaxed_model, tee=False, load_solutions=False)
        tc = str(res.solver.termination_condition).lower()
        print(f"[lp-ladder] deactivate={label:25s} -> {tc}")
        out.append((label, tc))

    _activate_all()
    return out



def lp_feasibility_ladder(model, groups, solver_name="highs", time_limit_sec=30, threads=8):
    """
    For each constraint-family group, deactivate that group in the ORIGINAL model,
    then run an LP-relaxation probe (on a clone) to test feasibility quickly.

    Returns list of dicts:
      {label, tc, status}
    """
    out = []

    # Reactivate all constraints between steps
    def _activate_all():
        for comp in model.component_objects(pyo.Constraint, active=False):
            comp.activate()

    def _deactivate(names):
        for n in names:
            if hasattr(model, n):
                getattr(model, n).deactivate()

    print("\n" + "="*70)
    print("LP-RELAX FEASIBILITY LADDER (no integer solving)")
    print("="*70)

    for label, to_deactivate in groups:
        _activate_all()
        _deactivate(to_deactivate)

        tc, st = lp_relaxation_probe(
            model,
            solver_name=solver_name,
            time_limit_sec=time_limit_sec,
            threads=threads,
        )

        # Interpret result
        if "infeasible" in tc:
            verdict = "LP_INFEASIBLE (hard contradiction)"
        elif tc in ("optimal", "feasible"):
            verdict = "LP_FEASIBLE"
        else:
            verdict = f"LP_UNKNOWN ({tc})"

        print(f"[lp-ladder] deactivate={label:25s} -> {verdict}")
        out.append({"label": label, "termination": tc, "status": st, "verdict": verdict})

    # leave model fully active at end
    _activate_all()
    return out





def solve_with_relaxed_A(model, solver_name="highs", time_limit_sec=600, threads=8):
    t0 = time.perf_counter()
    print("[relax-A(+U)] starting…", flush=True)

    # 1) clone
    m = model.clone()
    t0 = step(t0, "clone model")

    # 2) relax A
    nA = 0
    for j in m.J:
        for b in m.B:
            m.A[j, b].domain = pyo.UnitInterval
            nA += 1
    t0 = step(t0, f"relaxed A domains (count={nA:,})")

    # 3) relax U (important)
    if hasattr(m, "U"):
        nU = 0
        for j in m.J:
            for b in m.B:
                for mm in m.M:
                    m.U[j, b, mm].domain = pyo.UnitInterval
                    nU += 1
        t0 = step(t0, f"relaxed U domains (count={nU:,})")
    else:
        t0 = step(t0, "no U on model (skipped)")

    # 4) solve (this is the only part time_limit affects)
    solver = pyo.SolverFactory(solver_name)
    solver.options["time_limit"] = float(time_limit_sec)  # seconds 
    solver.options["threads"] = int(threads)

    t_solve0 = time.perf_counter()
    res = solver.solve(m, tee=False, load_solutions=False)
    solve_wall = time.perf_counter() - t_solve0

    tc = str(res.solver.termination_condition).lower()
    st = str(res.solver.status).lower()

    print(f"[relax-A(+U)] tc={tc} | status={st} | solver_wall={solve_wall:0.2f}s", flush=True)
    return tc, st








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




# def run_s25_s2(start, end, solver_name="highs", toggles: PlanningToggles = PlanningToggles()):
    
#     # -------------------------
#     # 1) Ingest + build jobs
#     # -------------------------
#     df_prm_master = ingest_s25(start, end)
#     jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)

#     print(jobs.head())


    
    
#     # -------------------------
#     # 2) Params
#     # -------------------------
#     BUCKET_MINUTES = 15

#     tau = build_tau_from_jobs(jobs, toggles)

#     classes = build_vehicle_classes(include_future=True)
#     print(classes)

#     N_AMB = sum(c["count"] for c in classes.get("Amb", []))

#     spin_removed = build_spin_minutes(
#         jobs,
#         spin_lock_threshold_mins=50,
#         n_ambulifts=N_AMB,
#         bucket_minutes=BUCKET_MINUTES,
#     )

    
#     # -------------------------
#     # 3) Pre-solver feasibility debug
#     # -------------------------
#     pre_solve_debug(
#         jobs=jobs,
#         toggles=toggles,
#         tau=tau,
#         spin_removed=spin_removed,
#         classes=classes,
#         bucket_minutes=BUCKET_MINUTES,
#         M_BIG=10_000,
#         lift_cycle_mins=LIFT_CYCLE_MINS,
#         lift_capacity_mins=LIFT_CAPACITY_MINS,
#     )
    
#     # -------------------------
#     # 4) Build model
#     # -------------------------
#     model = build_pyomo_model(
#         jobs=jobs,
#         tau=tau,
#         spin_removed=spin_removed,
#         toggles=toggles,
#     )

#     print("Vars:", model.nvariables())
#     print("Cons:", model.nconstraints())

#     # -------------------------
#     # 5) Optional constraint toggles (debug only)
#     # -------------------------
#     # model.AmbTimeCap.deactivate()
#     # model.MiniTimeCap.deactivate()
#     # model.LiftCap.deactivate()
#     # model.FleetExclusive.deactivate()
#     # model.AmbSeatCap.deactivate(); model.AmbWcCap.deactivate()
#     # model.MiniSeatCap.deactivate(); model.MiniWcCap.deactivate()
#     # NOTE: MaxLateCap removed from model per scope alignment (breach via y + penalty). 

    
#     groups = [
#         ("none (baseline)", []),
#         ("NoServiceAfterDeadline", ["NoServiceAfterDeadline"]),
#         ("AmbTimeCap", ["AmbTimeCap"]),
#         ("MiniTimeCap", ["MiniTimeCap"]),
#         ("FleetExclusive", ["FleetExclusive"]),
#         ("AmbSeat+Wc", ["AmbSeatCap", "AmbWcCap"]),
#         ("MiniSeat+Wc", ["MiniSeatCap", "MiniWcCap"]),
#         ("LiftCap", ["LiftCap"]),
#         ("Eligibility rules", ["SafetyStand", "NoAmbDomesticArrivals"]),
#     ]
                
    
#     solver = pyo.SolverFactory("highs")
#     summary = feasibility_ladder(model, solver, groups)

    
#     # -------------------------
#     # 6) Solve
#     # -------------------------
#     solver = pyo.SolverFactory(solver_name)
#     results = solver.solve(model, tee=True, load_solutions=False)

#     print("solver_status:", results.solver.status)
#     print("termination_condition:", results.solver.termination_condition)

#     tc = results.solver.termination_condition
#     if str(tc).lower() in ["infeasible", "infeasibleorunbounded"]:
#         return {"status": "infeasible", "model": model, "jobs": jobs, "results": results}

#     if str(tc).lower() not in ("optimal", "feasible"):
#         return {"status": str(tc), "model": model, "jobs": jobs, "results": results}

#     # Load solution values
#     model.solutions.load_from(results)

#     # Outputs (uncomment when you’re ready)
#     # summary = extract_summary(model, jobs)
#     # job_df = extract_job_assignments(model, jobs)
#     # vehicle_df = extract_vehicle_allocations(model)
#     # checks = run_sanity_checks(job_df, vehicle_df)

#     return {
#         "status": "optimal",
#         "model": model,
#         "jobs": jobs,
#         "results": results,
#         # "summary": summary,
#         # "job_assignments": job_df,
#         # "vehicle_allocations": vehicle_df,
#         # "sanity_checks": checks,
#     }


def run_s25_s2(
    start,
    end,
    solver_name="highs",
    toggles: PlanningToggles = PlanningToggles(),
    run_ladder: bool = False,
    solve_model: bool = True,
):
    """
    Scenario 2 runner.
    - run_ladder: if True, run constraint-family feasibility ladder (multiple solves).
    - solve_model: if False, build model + run debug/ladder only (no final solve).
    """

    
    print("\nPRM OPT — S25 Scenario 2")
    print(f"Window : {start} → {end}\n")

    t = time.perf_counter()

    # -------------------------
    # 1) Ingest + build jobs
    # -------------------------
    print("[1/7] ingest_s25…", flush=True)
    df_prm_master = ingest_s25(start, end)
    t = step(t, f"ingest_s25 done | rows={len(df_prm_master):,}")


    
    print("[2/7] build_jobs…", flush=True)
    jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)
    t = step(t, f"build_jobs done | jobs={len(jobs):,}")


    # -------------------------
    # 2) Params
    # -------------------------
    BUCKET_MINUTES = 15

    
    print("\n[3/7] build_tau_from_jobs…", flush=True)
    tau = build_tau_from_jobs(jobs, toggles)
    t = step(t, "tau built")


    print("\n[tau] first 15 tau keys/values")
    for k in list(tau.keys())[:15]:
        print(k, "->", tau[k])

    print("\n[4/7] build_vehicle_classes + build_spin_minutes…", flush=True)
    classes = build_vehicle_classes(include_future=True)
    N_AMB = sum(int(c["count"]) for c in classes.get("Amb", []))
    spin_removed = build_spin_minutes(
        jobs,
        spin_lock_threshold_mins=50,
        n_ambulifts=N_AMB,
        bucket_minutes=BUCKET_MINUTES,
    )
    t = step(t, f"classes+spin_removed built | N_AMB={N_AMB}")

    # -------------------------
    # 3) Pre-solver feasibility debug (no solver)
    # -------------------------
    print("\n[5/7] pre_solve_debug (no solver)…", flush=True)
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
    # 4) Build model (once)
    # -------------------------
    print("\n[6/7] build_pyomo_model…", flush=True)
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

    print("\n[7/7] solve_with_relaxed_A…", flush=True)
    t_relax0 = time.perf_counter()
    tcA, stA = solve_with_relaxed_A(model, solver_name=solver_name, time_limit_sec=600)
    print(f"[relax-A(+U)] {tcA} {stA}   [{time.perf_counter() - t_relax0:0.2f}s]", flush=True)

    # -------------------------
    # 5) Optional: feasibility ladder (debug only)
    # -------------------------
    ladder_summary = None
    if run_ladder:
        print("\n[ladder] running MILP ladder (timed caps recommended)…", flush=True)
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
        solver_tmp.options["time_limit"] = 100
        solver_tmp.options["threads"] = 8
        ladder_summary = feasibility_ladder(model, solver_tmp, groups)

        # IMPORTANT: after the ladder, ensure all constraints are active again
        for comp in model.component_objects(pyo.Constraint, active=False):
            comp.activate()
        
        print("[ladder] done", flush=True)

    # If you only want debug/ladder, stop here
    if not solve_model:
        return {
            "status": "built_only",
            "model": model,
            "jobs": jobs,
            "ladder": ladder_summary,
        }

    # -------------------------
    # 6) Solve (single solve)
    # -------------------------
    print("\n[SOLVE] solving MILP…", flush=True)
    solver = pyo.SolverFactory(solver_name)

    
    # Diagnostic time limit (keep ON while debugging)
    # HiGHS supports time_limit option 
    solver.options["time_limit"] = 600
    solver.options["threads"] = 8


    t_solve0 = time.perf_counter()
    results = solver.solve(model, tee=True, load_solutions=False)
    print(f"[SOLVE] returned in {time.perf_counter() - t_solve0:0.2f}s", flush=True)

    print("solver_status:", results.solver.status)
    print("termination_condition:", results.solver.termination_condition)

    tc = str(results.solver.termination_condition).lower()
    if tc in ["infeasible", "infeasibleorunbounded"]:
        return {
            "status": "infeasible",
            "model": model,
            "jobs": jobs,
            "results": results,
            "ladder": ladder_summary,
        }

    if tc not in ("optimal", "feasible"):
        return {
            "status": tc,
            "model": model,
            "jobs": jobs,
            "results": results,
            "ladder": ladder_summary,
        }

    model.solutions.load_from(results)

    return {
        "status": "optimal",
        "model": model,
        "jobs": jobs,
        "results": results,
        "ladder": ladder_summary,
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
    Scenario 2 runner (v2): build once, solve once.
    Designed to validate the UPDATED bucket spillover + combined-only standby logic.

    Outputs:
      - termination condition + status
      - (if solved) solution loaded + extracted outputs
    """

    print("\nPRM OPT — S25 Scenario 2 (v2)")
    print(f"Window : {start} → {end}\n")

    t = time.perf_counter()

    # 1) Ingest + build jobs
    print("[1/6] ingest_s25…", flush=True)
    df_prm_master = ingest_s25(start, end)
    t = step(t, f"ingest_s25 done | rows={len(df_prm_master):,}")

    print("[2/6] build_jobs…", flush=True)
    jobs = build_jobs(df_prm_master, bucket="15min", toggles=toggles)
    t = step(t, f"build_jobs done | jobs={len(jobs):,}")

    # 2) Params
    BUCKET_MINUTES = 15

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

    # 3) Pre-solver debug
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

    # 4) Build model
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

    # Optional: quick time-capped ladder (use only if solve fails / times out)
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

    # 5) Solve (single solve)
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