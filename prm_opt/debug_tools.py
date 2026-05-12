#prm_opt/debug_tools.py


"""
Diagnostic and debugging utilities for PRM optimisation models.

These tools:
- Do NOT perform optimisation
- Provide fast necessary-condition feasibility checks
- Help isolate infeasibility causes (time, capacity, eligibility, lift)

Intended use:
- Before running expensive MILP solves
- When investigating why a model becomes infeasible
"""







import pandas as pd
import numpy as np
import math
import time

import pyomo.environ as pyo
import pyomo.core.base as TransformationFactory

from .config import LIFT_CAPACITY_MINS, LIFT_CYCLE_MINS, PlanningToggles

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
    IMPORTANT:
    This function checks NECESSARY feasibility conditions only.
    Passing these checks does NOT guarantee feasibility.
    Failing them guarantees infeasibility.
    """
    print("\n" + "="*70)
    print("PRE-SOLVER FEASIBILITY DEBUG (no solver)")
    print("="*70)

    
    # --------------------------------------------------
    # 1) Basic structural sanity checks
    # --------------------------------------------------

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

    
    # --------------------------------------------------
    # 2) Time bucket horizon and SLA window coverage
    # ------------------------------------------------

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

    
    
    # --------------------------------------------------
    # 3) Spin-lock sanity checks (ambulift minutes removed)
    # --------------------------------------------------

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
    
    
    # ---- spin definition sanity (jobs-level) ----
    if "is_spin" in jobs.columns:
        spin = jobs[jobs["is_spin"] == 1]
        print("\n[Spin flag debug]")
        print("  is_spin rows:", len(spin))
        if len(spin):
            print("  is_spin by dir:")
            print(spin["dir"].value_counts(dropna=False))
            if "Minutes on Chocks" in jobs.columns:
                print("  Minutes on Chocks (spin rows) min/median/max:",
                    float(spin["Minutes on Chocks"].min()),
                    float(spin["Minutes on Chocks"].median()),
                    float(spin["Minutes on Chocks"].max()))
            if "Turnaround Vertical Count" in jobs.columns:
                print("  Turnaround Vertical Count (spin rows) min/median/max:",
                    float(spin["Turnaround Vertical Count"].min()),
                    float(spin["Turnaround Vertical Count"].median()),
                    float(spin["Turnaround Vertical Count"].max()))
        


    
    # --------------------------------------------------
    # 4) Necessary fleet-capacity checks (coarse, fast)
    # --------------------------------------------------

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
    # 5) Build bucket grid / indices consistent with the model
    # -----------------------------
    
    # These checks mirror model logic exactly (bucket grid, spillover),
    # but are implemented without Pyomo for speed.

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


    
    # --------------------------------------------------
    # 6) Tight-window feasibility checks
    # Jobs that are forced into (essentially) a single bucket
    # --------------------------------------------------

    K_CAP = int(getattr(toggles, "spill_bucket_cap", 12) or 12)
    
    def _split_mins(total_mins: float, bucket_mins: int, k_cap: int):
        total = max(0.0, float(total_mins))
        if total <= 1e-9:
            return []
        k = min(int(math.ceil(total / float(bucket_mins))), int(k_cap))
        out, rem = [], total
        for _ in range(k):
            use = min(float(bucket_mins), rem)
            out.append(use)
            rem -= use
            if rem <= 1e-9:
                break
        return out

    tight = (t_bucket == latest_bucket) & latest_bucket.notna()
    print("\nTight-window jobs (must be served in exactly one bucket):", int(tight.sum()))
    if tight.any():
        cols = ["Passenger ID", "flight_key", "dir", "t", "hard_deadline_time", "sla_start_time", "Stand", "Airline Code"]
        cols = [c for c in cols if c in jobs.columns]
        print("Examples (first 20):")
        print(jobs.loc[tight, cols].head(20))

    # -----------------------------
    # 6A) Tight-window lift feasibility check
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
    # 6B) Tight-window ambulift TIME feasibility check (vertical-only, SPILLOVER-AWARE)
    # -----------------------------
    N_AMB = sum(int(c["count"]) for c in classes.get("Amb", []))
    cap = float(bucket_minutes * N_AMB)

    print("\nAmb tight-window time check (vertical-only, spillover-aware):")
    print("  Total ambulifts (from classes):", int(N_AMB))
    print("  Spill cap (buckets):", int(K_CAP))

    if "needs_vertical" in jobs.columns:
        tight_vertical = tight & (jobs["needs_vertical"] == 1)

        # HARD FAIL: tight vertical job starts in a bucket where amb minutes available = 0
        if spin_removed is not None:
            zero_avail_buckets = {pd.to_datetime(b) for b, v in spin_removed.items() if float(v) >= cap - 1e-9}
        else:
            zero_avail_buckets = set()

        hard_fail_jobs = jobs.loc[tight_vertical].copy()
        hard_fail_jobs["t_bucket"] = t_bucket.loc[hard_fail_jobs.index]
        hard_fail_jobs = hard_fail_jobs[hard_fail_jobs["t_bucket"].isin(zero_avail_buckets)]

        print("  tight vertical jobs:", int(tight_vertical.sum()))
        print("  saturated (avail=0) buckets:", len(zero_avail_buckets))
        print("  tight vertical jobs starting in saturated bucket:", len(hard_fail_jobs))

        if len(hard_fail_jobs) > 0:
            cols = ["Passenger ID","flight_key","dir","Airline Code","Stand","t","hard_deadline_time","sla_start_time"]
            cols = [c for c in cols if c in hard_fail_jobs.columns]
            print("\n❌ HARD INFEASIBILITY: tight vertical jobs start in buckets with 0 ambulift minutes available.")
            print(hard_fail_jobs[cols].head(25))

        # Spillover-aware required minutes per bucket
        req = {}  # bucket -> required vertical ambulift minutes (spilled)
        for j in jobs.index[tight_vertical]:
            start_b = t_bucket.loc[j]
            spill = _split_mins(float(tau[(j, "Amb")]), bucket_minutes, K_CAP)
            for k, mins in enumerate(spill):
                b = start_b + pd.to_timedelta(k * bucket_minutes, unit="m")
                req[b] = req.get(b, 0.0) + float(mins)

        infeas = []
        for b, mins in req.items():
            removed = float(spin_removed.get(b, 0.0)) if spin_removed is not None else 0.0
            avail = cap - removed
            if mins > avail + 1e-6:
                infeas.append((b, mins, avail, removed))

        print("  buckets with spilled vertical demand:", len(req))
        print("  infeasible buckets (req > avail):", len(infeas))

        if infeas:
            infeas_sorted = sorted(infeas, key=lambda x: (x[2] - x[1]))  # worst shortfall first
            print("\nDetails for worst infeasible bucket(s):")
            for (b, mins, avail, removed) in infeas_sorted[:10]:
                print(f"  bucket={b} | req={mins:.1f} | avail={avail:.1f} | spin_removed={removed:.1f} | cap={cap:.1f}")
            print("\n❌ INFEASIBLE RISK (spillover-aware): spilled vertical minutes exceed availability in some buckets.")
        else:
            print("✅ No spillover-aware ambulift time overload from tight vertical jobs.")
    else:
        print("  skipped (needs_vertical missing).")



    # -----------------------------
    # 6C) Tight-window flight+bucket ambulift SEAT feasibility (vertical-only minimum)
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



def check_job_time_windows(jobs, BUCKET_MINUTES=15, max_late_mins=180):
    # Build bucket timeline the same way as the model
    t_min = pd.to_datetime(jobs["t"]).min()
    sla_starts = pd.to_datetime(
        jobs["sla_start_time"] if "sla_start_time" in jobs.columns else jobs["s"]
    )

    start_time = min(t_min, sla_starts.min()).floor(f"{BUCKET_MINUTES}min")
    end_time = max(
        pd.to_datetime(jobs["s"]).max(),
        pd.to_datetime(jobs["hard_deadline_time"]).dropna().max()
        if "hard_deadline_time" in jobs.columns else pd.to_datetime(jobs["s"]).max()
    ).ceil(f"{BUCKET_MINUTES}min") + pd.to_timedelta(6, "h")

    B_list = list(pd.date_range(start=start_time, end=end_time, freq=f"{BUCKET_MINUTES}min"))
    b_to_idx = {b: i for i, b in enumerate(B_list)}

    bad = []

    for j in jobs.index:
        # release
        release_idx = b_to_idx[pd.to_datetime(jobs.loc[j, "t"]).floor(f"{BUCKET_MINUTES}min")]

        # latest
        if (
            "hard_deadline_time" in jobs.columns
            and str(jobs.loc[j, "dir"]) == "D"
            and pd.notna(jobs.loc[j, "hard_deadline_time"])
        ):
            latest_idx = b_to_idx[
                pd.to_datetime(jobs.loc[j, "hard_deadline_time"]).ceil(f"{BUCKET_MINUTES}min")
            ]
        else:
            sla_start = (
                pd.to_datetime(jobs.loc[j, "sla_start_time"])
                if "sla_start_time" in jobs.columns and pd.notna(jobs.loc[j, "sla_start_time"])
                else pd.to_datetime(jobs.loc[j, "s"])
            ).floor(f"{BUCKET_MINUTES}min")

            latest_time = sla_start + pd.to_timedelta(
                jobs.loc[j, "sla_limit"] + max_late_mins, "m"
            )
            latest_idx = b_to_idx.get(
                latest_time.ceil(f"{BUCKET_MINUTES}min"),
                max(b_to_idx.values())
            )

        if latest_idx < release_idx:
            bad.append(j)

    print(f"\n🚨 Jobs with NO feasible start bucket: {len(bad)}")
    if bad:
        cols = [
            "Passenger ID", "flight_key", "dir",
            "t", "sla_start_time", "sla_limit", "hard_deadline_time"
        ]
        cols = [c for c in cols if c in jobs.columns]
        print(jobs.loc[bad, cols].head(15))

    return bad



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




