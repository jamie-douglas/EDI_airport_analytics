
# scripts/prm_opt/pyomo_model_legacy.py

"""
Scenario 2 optimisation (no pooling across flights).

Implements Model Scope components:
- job-level horizontal mode decisions x[j,m]
- service start bucket assignment A[j,b] to model response delay and SLA
- vehicle time availability per bucket (fleet minute capacity)
- spin locking removes ambulift minutes per bucket
- vehicle physical capacity (wheelchair slot capacity)
- Gate 7/8 lift bottleneck
- heterogeneous fleet capacities via vehicle classes
- per-flight per-bucket vehicle assignment counts k[(vtype,cid),(f,b)]
- objective: minimise vehicles dispatched + soft penalties + SLA penalty

Key modelling choices:
- If needs_vertical==1, the vertical component ALWAYS consumes Ambulift resource/time.
- Horizontal mode choice can be Amb, Mini, or Push (so vertical+Mini and vertical+Push are allowed if eligible).

Bucket spillover upgrade (THIS UPDATE):
- A[j,b]=1 means "job starts in bucket b".
- Active busy minutes (tau) are allocated across several consecutive buckets after the start bucket.
- Handover/standby minutes apply ONLY for combined jobs (i.e., when horizontal mode is NOT Ambulift).
  Reason: if horizontal is Ambulift, the same ambulift is used and there is no cross-vehicle coordination.
"""

from __future__ import annotations

import math
import pyomo.environ as pyo
import pandas as pd
import time

from .config import (
    RYANAIR_CODES,
    PENALTY,
    LIFT_CYCLE_MINS,
    LIFT_CAPACITY_MINS,
    VEHICLE_MODELS,
    PlanningToggles,
)
from .params import build_vehicle_classes
from modules.utils.progress import step


# ---------------------------------------------------------------------
# Utility: split a total number of minutes across consecutive buckets
# ---------------------------------------------------------------------
def _split_mins(total_mins: float, bucket_mins: int, k_cap: int) -> list[float]:
    """
    Split total_mins into a list of per-bucket minute allocations (front-loaded),
    capped to at most k_cap buckets.

    Example (bucket=15):
      total=32 -> [15, 15, 2]

    This supports "jobs can span buckets" without building an explicit schedule.
    """
    total = max(0.0, float(total_mins))
    if total <= 1e-9:
        return []
    k = int(math.ceil(total / float(bucket_mins)))
    k = min(k, int(k_cap))
    out = []
    rem = total
    for _ in range(k):
        use = min(float(bucket_mins), rem)
        out.append(use)
        rem -= use
        if rem <= 1e-9:
            break
    return out


def _precompute_spill_vectors(
    jobs: pd.DataFrame,
    tau: dict,
    bucket_minutes: int,
    toggles: PlanningToggles,
) -> dict:
    """
    Precompute spill vectors for active minutes and standby minutes.
    Returns dict of precomputed structures used by time-cap constraints.

    Standby/handover minutes:
    - Only relevant for combined jobs (horizontal != Ambulift).
    - We model standby as "waiting time" (counted on one side, not both).
      Defaults:
        * departures -> standby on vertical side (ambulift)
        * arrivals   -> standby on horizontal side (minibus / pusher)
    """
    k_cap = int(getattr(toggles, "spill_bucket_cap", 12) or 12)

    # Active busy minutes per job for each resource/mode
    amb_active = {j: _split_mins(tau[(j, "Amb")], bucket_minutes, k_cap) for j in jobs.index}
    mini_active = {j: _split_mins(tau[(j, "Mini")], bucket_minutes, k_cap) for j in jobs.index}
    push_active = {j: _split_mins(tau[(j, "Push")], bucket_minutes, k_cap) for j in jobs.index}

    # Vertical active minutes: ambulift only if needs_vertical==1
    amb_vert = {
        j: (amb_active[j] if int(jobs.loc[j, "needs_vertical"]) == 1 else [])
        for j in jobs.index
    }

    # Standby minutes (waiting/coordination), direction-specific
    standby_dep_vert = float(getattr(toggles, "standby_dep_vert_mins", 0.0) or 0.0)
    standby_arr_horiz = float(getattr(toggles, "standby_arr_horiz_mins", 0.0) or 0.0)

    standby_vert_total = {}
    standby_horiz_total = {}

    for j in jobs.index:
        if int(jobs.loc[j, "needs_vertical"]) == 1:
            if str(jobs.loc[j, "dir"]) == "D":
                # Departures: standby on vertical side (ambulift)
                standby_vert_total[j] = standby_dep_vert
                standby_horiz_total[j] = 0.0
            else:
                # Arrivals: standby on horizontal side (the chosen horizontal resource)
                standby_vert_total[j] = 0.0
                standby_horiz_total[j] = standby_arr_horiz
        else:
            standby_vert_total[j] = 0.0
            standby_horiz_total[j] = 0.0

    standby_vert = {j: _split_mins(standby_vert_total[j], bucket_minutes, k_cap) for j in jobs.index}
    standby_horiz = {j: _split_mins(standby_horiz_total[j], bucket_minutes, k_cap) for j in jobs.index}

    return {
        "k_cap": k_cap,
        "amb_active": amb_active,
        "mini_active": mini_active,
        "push_active": push_active,
        "amb_vert": amb_vert,
        "standby_vert": standby_vert,
        "standby_horiz": standby_horiz,
    }


def build_pyomo_model(
    jobs: pd.DataFrame,
    tau: dict,
    spin_removed: dict,
    toggles: PlanningToggles,
):
    """
    Parameters
    ----------
    jobs : DataFrame from build_jobs(). Required columns:
        - flight_key
        - t : release bucket (prepositioned)
        - s : scheduled bucket (original job start bucket)
        - needs_wc, needs_vertical, safety_stand, lift_gate
        - sla_limit (already includes sla_buffer_mins)
        - Airline Code, dir, class
        - (optional) sla_start_time, hard_deadline_time
    tau : dict[(j, mode)->minutes] for modes: Amb, Mini, Push
        (Interpreted as active busy minutes for the resource.)
    spin_removed : dict[bucket_timestamp -> minutes]
        Ambulift minutes removed in bucket due to spin lock rules (service bucket index).
    toggles : PlanningToggles
        Supports:
        - horizon_slack_mins
        - spill_bucket_cap
        - standby_dep_vert_mins
        - standby_arr_horiz_mins
        - ferry_mini_reserved / ferry_drv_reserved
    """
    
    t0 = time.perf_counter()
    t = t0

    m = pyo.ConcreteModel()
    t = step(t, "ConcreteModel created")


    # -----------------------------
    # Constants
    # -----------------------------
    BUCKET_MINUTES = 15
    M_BIG = 10_000

    MAX_LATE_MINS = int(getattr(toggles, "max_late_mins", 180) or 180)

    # Optional placeholders (do nothing unless provided)
    ferry_mini_reserved = getattr(toggles, "ferry_mini_reserved", {}) or {}
    ferry_drv_reserved = getattr(toggles, "ferry_drv_reserved", {}) or {}

    t = step(t, "constants + toggles loaded")

    # -----------------------------
    # Build SERVICE bucket timeline
    # -----------------------------
    t_min = pd.to_datetime(jobs["t"]).min()
    s_max = pd.to_datetime(jobs["s"]).max()
    max_sla = float(pd.to_numeric(jobs["sla_limit"]).max())

    # include SLA start anchors in horizon start if present
    if "sla_start_time" in jobs.columns:
        sla_min = pd.to_datetime(jobs["sla_start_time"]).dropna().min()
    else:
        sla_min = t_min

    # include hard deadlines in horizon end if present
    if "hard_deadline_time" in jobs.columns:
        deadline_max = pd.to_datetime(jobs["hard_deadline_time"]).dropna().max()
    else:
        deadline_max = s_max

    start_time = min(t_min, sla_min).floor(f"{BUCKET_MINUTES}min")

    horizon_slack_mins = int(getattr(toggles, "horizon_slack_mins", 240) or 240)
    extra_mins = max_sla + float(horizon_slack_mins)
    slack_buckets = int((extra_mins // BUCKET_MINUTES) + 2)

    end_anchor = max(s_max, deadline_max).floor(f"{BUCKET_MINUTES}min")
    end_time = end_anchor + pd.to_timedelta(slack_buckets * BUCKET_MINUTES, unit="m")

    B_list = list(pd.date_range(start=start_time, end=end_time, freq=f"{BUCKET_MINUTES}min"))
    b_to_idx = {b: i for i, b in enumerate(B_list)}
    idx_to_b = {i: b for b, i in b_to_idx.items()}

    t = step(t, f"bucket timeline built | buckets={len(B_list):,}")

    # -----------------------------
    # Sets
    # -----------------------------
    m.J = pyo.Set(initialize=list(jobs.index))
    m.B = pyo.Set(initialize=B_list, ordered=True)

    # Horizontal modes
    m.M = pyo.Set(initialize=["Amb", "Mini", "Push"])

    # Flights
    flights = sorted(jobs["flight_key"].unique())
    m.F = pyo.Set(initialize=flights)

    # # Flight x bucket pairs
    # m.FB = pyo.Set(dimen=2, initialize=[(f, b) for f in flights for b in B_list])

    t = step(t, f"sets built | J={len(jobs):,} | F={len(flights):,}")

    # -----------------------------
    # SLA parameters
    # -----------------------------
    release_b = {j: pd.to_datetime(jobs.loc[j, "t"]) for j in jobs.index}
    release_idx = {j: b_to_idx[release_b[j]] for j in jobs.index}

    # Job-specific SLA limit
    L_j = {j: float(jobs.loc[j, "sla_limit"]) for j in jobs.index}

    # SLA start bucket: prefer sla_start_time, else fall back to scheduled bucket s (or t)
    sla_start_b = {}
    for j in jobs.index:
        ts = jobs.loc[j, "sla_start_time"] if "sla_start_time" in jobs.columns else pd.NaT
        if pd.isna(ts):
            ts = jobs.loc[j, "s"] if "s" in jobs.columns else jobs.loc[j, "t"]
        sla_start_b[j] = pd.to_datetime(ts).floor(f"{BUCKET_MINUTES}min")
    sla_start_idx = {j: b_to_idx[sla_start_b[j]] for j in jobs.index}

     # Hard deadline (departures): disallow service after deadline bucket
    hard_deadline_idx = {}
    has_deadline = "hard_deadline_time" in jobs.columns
    for j in jobs.index:
        if not has_deadline:
            hard_deadline_idx[j] = None
            continue
        t_dead = jobs.loc[j, "hard_deadline_time"]
        hard_deadline_idx[j] = None if pd.isna(t_dead) else b_to_idx[pd.to_datetime(t_dead).ceil(f"{BUCKET_MINUTES}min")]
    
    t = step(t, "SLA maps built (release/sla_start/deadline)")

    
    # -----------------------------
    # Build sparse Flight x Bucket set (m.FB) to avoid flights × full-horizon explosion
    # -----------------------------
    MAX_LATE_MINS = int(getattr(toggles, "max_late_mins", 180) or 180)

    # Latest feasible service index for each job (used to bound per-flight bucket windows)
    latest_idx = {}
    for j in jobs.index:
        # Departures: hard deadline if present
        if str(jobs.loc[j, "dir"]) == "D" and hard_deadline_idx.get(j) is not None:
            latest_idx[j] = int(hard_deadline_idx[j])
        else:
            # Arrivals (or no deadline): cap latest service to SLA start + SLA limit + MAX_LATE_MINS
            latest_time = sla_start_b[j] + pd.to_timedelta(float(L_j[j] + MAX_LATE_MINS), unit="m")
            latest_time = pd.to_datetime(latest_time).ceil(f"{BUCKET_MINUTES}min")
            latest_idx[j] = min(int(b_to_idx.get(latest_time, len(B_list) - 1)), len(B_list) - 1)

    
    bad = [j for j in jobs.index if int(latest_idx[j]) < int(release_idx[j])]
    print("bad jobs (latest_idx < release_idx):", len(bad))
    if bad:
        cols = ["Passenger ID","flight_key","dir","t","sla_start_time","sla_limit","hard_deadline_time","release_time"]
        cols = [c for c in cols if c in jobs.columns]
        print(jobs.loc[bad, cols].head(20))
        # also print the raw indices for the first few
        for j in bad[:10]:
            print("j=", j, "release_idx=", release_idx[j], "latest_idx=", latest_idx[j])

        
    # -----------------------------
    # Build sparse Job x Bucket set (m.JB)
    # Feasible START buckets for each job (same timing logic as latest_idx)
    # -----------------------------
    jb_pairs = []
    jb_set = set()

    for j in jobs.index:
        lo = int(release_idx[j])
        hi = int(latest_idx[j])
        hi = min(hi, len(B_list) - 1)

        for bi in range(lo, hi + 1):
            pair = (j, idx_to_b[bi])
            jb_pairs.append(pair)
            jb_set.add(pair)

    m.JB = pyo.Set(dimen=2, initialize=jb_pairs)

    # Helpful maps: buckets feasible per job, and jobs feasible per bucket
    jb_by_j = {j: [] for j in jobs.index}
    jb_by_b = {b: [] for b in B_list}

    for (j, b) in jb_pairs:
        jb_by_j[j].append(b)
        jb_by_b[b].append(j)

    print("\n[JB sparse debug]")
    print("  JB pairs:", len(jb_pairs))
    print("  avg buckets per job:", round(len(jb_pairs) / max(1, len(jobs)), 2))

    # ------------------- end of new block ---------------------------

    # Group jobs per flight
    jobs_by_f = {f: [] for f in flights}
    for j in jobs.index:
        jobs_by_f[jobs.loc[j, "flight_key"]].append(j)

    # Build sparse (f,b) pairs
    fb_pairs = []
    for f in flights:
        Jf = jobs_by_f.get(f, [])
        if not Jf:
            continue
        lo = min(release_idx[j] for j in Jf)
        hi = max(latest_idx[j] for j in Jf)
        hi = min(hi, len(B_list) - 1)
        for bi in range(lo, hi + 1):
            fb_pairs.append((f, idx_to_b[bi]))

    t = step(t, f"sparse FB pairs list built | FB={len(fb_pairs):,}")
    
    # # -----------------------------
    # # DEBUG: sanity-check sparse FB construction
    # # -----------------------------
    # print("\n[FB sparse debug]")
    # print("  flights:", len(flights))
    # print("  buckets:", len(B_list))
    # print("  FB pairs:", len(fb_pairs))
    # print("  avg buckets per flight:", round(len(fb_pairs) / max(1, len(flights)), 2))

    # # Any flight missing FB coverage?
    # missing_f = [f for f in flights if f not in jobs_by_f or len(jobs_by_f[f]) == 0]
    # if missing_f:
    #     print("  WARNING: flights with no jobs (will be ignored):", len(missing_f))

    # # Check window lengths distribution
    # lens = []
    # for f in flights:
    #     Jf = jobs_by_f.get(f, [])
    #     if not Jf:
    #         continue
    #     lo = min(release_idx[j] for j in Jf)
    #     hi = max(latest_idx[j] for j in Jf)
    #     lens.append(hi - lo + 1)

    # if lens:
    #     print("  window buckets per flight: min/median/max =", min(lens), int(pd.Series(lens).median()), max(lens))
    #     # Flag huge windows (means MAX_LATE_MINS too large or deadlines missing)
    #     big = sum(1 for L in lens if L > 24 * 4)  # > 24 hours at 15-min buckets
    #     print("  flights with >24h service window:", big)


    m.FB = pyo.Set(dimen=2, initialize=fb_pairs)

    
    t = step(t, "m.FB set created")


    
    # --- DEBUG: FB covers all possible start buckets for each flight ---
    # (Only needed if you use sparse FB; harmless otherwise)

    if isinstance(m.FB, pyo.Set) and m.FB.dimen == 2:
        fb_set = set(m.FB.data())  # (f,b) tuples
        # quick map: for each flight, list buckets it has FB
        fb_buckets_by_f = {}
        for f, b in fb_set:
            fb_buckets_by_f.setdefault(f, set()).add(b)

        # For each job, check that its flight has FB entry at its release bucket
        missing = []
        for j in jobs.index:
            f = jobs.loc[j, "flight_key"]
            b0 = pd.to_datetime(jobs.loc[j, "t"]).floor(f"{BUCKET_MINUTES}min")
            if f in fb_buckets_by_f and b0 not in fb_buckets_by_f[f]:
                missing.append((j, f, b0))

        print("\n[FB coverage debug]")
        print("  missing (flight, release_bucket) coverage:", len(missing))
        if missing:
            print("  examples:", missing[:10])


    
    # Helpful index: flights available in each bucket (only those (f,b) in FB)
    fb_by_b = {b: [] for b in B_list}
    for (f, b) in fb_pairs:
        fb_by_b[b].append(f)

    t = step(t, "fb_by_b index built")


    # -----------------------------
    # Vehicle classes (heterogeneous fleet)
    # -----------------------------
    classes = build_vehicle_classes(include_future=True)

    vclasses = []
    seatcap = {}
    wccap = {}
    count = {}

    for vtype, lst in classes.items():
        for c in lst:
            key = (vtype, c["class_id"])
            vclasses.append(key)
            seatcap[key] = c["seatcap"]
            wccap[key] = c["wccap"]
            count[key] = c["count"]

    m.VC = pyo.Set(dimen=2, initialize=vclasses)  # (vtype, class_id)
    m.seatcap = pyo.Param(m.VC, initialize=seatcap)
    m.wccap = pyo.Param(m.VC, initialize=wccap)
    m.count = pyo.Param(m.VC, initialize=count)

    # Fleet totals
    N_AMB = sum(int(count[(vt, cid)]) for (vt, cid) in vclasses if vt == "Amb")
    N_MINI = sum(int(count[(vt, cid)]) for (vt, cid) in vclasses if vt == "Mini")

    t = step(t, f"vehicle classes set | VC={len(vclasses):,} | N_AMB={N_AMB} | N_MINI={N_MINI}")

    # -----------------------------
    # Decision variables
    # -----------------------------
    m.x = pyo.Var(m.J, m.M, domain=pyo.Binary)          # horizontal mode choice
    # m.A = pyo.Var(m.J, m.B, domain=pyo.Binary)          # start bucket (serve once)
    m.A = pyo.Var(m.JB, domain=pyo.Binary)       # start bucket only where feasible ###
    m.y = pyo.Var(m.J, domain=pyo.Binary)               # SLA breach indicator

    # Linearisation var: U[j,b,m] = A[j,b] AND x[j,m]
    #m.U = pyo.Var(m.J, m.B, m.M, domain=pyo.Binary)
    m.U = pyo.Var(m.JB, m.M, domain=pyo.Binary)  # linearisation only where A exists ###

    # Vehicles assigned to (flight, bucket)
    m.k = pyo.Var(m.VC, m.FB, domain=pyo.NonNegativeIntegers)

    # Staffing headcount per bucket
    m.H_drv = pyo.Var(m.B, domain=pyo.NonNegativeIntegers)
    m.H_vehag = pyo.Var(m.B, domain=pyo.NonNegativeIntegers)
    m.H_push = pyo.Var(m.B, domain=pyo.NonNegativeIntegers)


    print(f"[sizes] J={len(jobs):,} | B={len(B_list):,} | JB={len(jb_pairs):,} | FB={len(fb_pairs):,}")
    
    t = step(
        t,
        "vars created | "
        f"x={len(jobs)*len(m.M):,} | "
        f"JB={len(jb_pairs):,} | "
        f"A={len(jb_pairs):,} | "
        f"U={len(jb_pairs)*len(m.M):,} | "
        f"k={len(vclasses)*len(fb_pairs):,}"
    )


    # -----------------------------
    # Objective (weights are placeholders; replace with real costs later)
    # -----------------------------
    BIG = 1000.0   # prioritise minimising vehicles dispatched
    LAMBDA = 1e6   # SLA breaches are extremely undesirable

    def obj_rule(m):
        #trips = BIG * sum(m.k[vc, fb] for vc in m.VC for fb in m.FB)
        trips = BIG * pyo.quicksum(m.k[vc, fb] for vc in m.VC for fb in m.FB)###

        soft = sum(
            PENALTY["AMB_HORIZONTAL"] * m.x[j, "Amb"] +
            PENALTY["PUSH"] * m.x[j, "Push"] +
            PENALTY["TRANSFER"] * int(jobs.loc[j, "needs_vertical"]) * m.x[j, "Mini"]
            for j in m.J
        )

        sla_pen = LAMBDA * sum(m.y[j] for j in m.J)

        # Wage-weighted staffing penalty (placeholder)
        WAGE = {"Drv": 1.0, "VehAg": 1.0, "Push": 1.0}
        staff_reg = sum(
            WAGE["Drv"] * m.H_drv[b] +
            WAGE["VehAg"] * m.H_vehag[b] +
            WAGE["Push"] * m.H_push[b]
            for b in m.B
        )

        # Capex penalty for all vehicles (assumes VEHICLE_MODELS has capex_hr per class_id)
        VEHICLE_CAPEX = sum(
            float(VEHICLE_MODELS.get(cid, {}).get("capex_hr", 0.0)) *
            sum(m.k[(vtype, cid), fb] for fb in m.FB)
            for (vtype, cid) in m.VC
        )

        return trips + soft + sla_pen + staff_reg + VEHICLE_CAPEX

    m.OBJ = pyo.Objective(rule=obj_rule, sense=pyo.minimize)

    t = step(t, "objective created")

    # -----------------------------
    # Core constraints
    # -----------------------------
    # One mode per job
    m.OneMode = pyo.Constraint(m.J, rule=lambda m, j: sum(m.x[j, mm] for mm in m.M) == 1)

    # # Linearisation: U = A AND x
    # def u_le_a(m, j, b, mm):
    #     return m.U[j, b, mm] <= m.A[j, b]
    # m.U_le_A = pyo.Constraint(m.J, m.B, m.M, rule=u_le_a)


    # def u_le_x(m, j, b, mm):
    #     return m.U[j, b, mm] <= m.x[j, mm]
    # m.U_le_X = pyo.Constraint(m.J, m.B, m.M, rule=u_le_x)

    # def u_ge_and(m, j, b, mm):
    #     return m.U[j, b, mm] >= m.A[j, b] + m.x[j, mm] - 1
    # m.U_ge_AND = pyo.Constraint(m.J, m.B, m.M, rule=u_ge_and)

    # t = step(t, "constraints: U linearisation (3 blocks)")

    
    m.U_le_A = pyo.Constraint(###
        m.JB, m.M,###
        rule=lambda m, j, b, mm: m.U[j, b, mm] <= m.A[j, b]###
    )###

    m.U_le_X = pyo.Constraint(###
        m.JB, m.M,###
        rule=lambda m, j, b, mm: m.U[j, b, mm] <= m.x[j, mm]###
    )###

    m.U_ge_AND = pyo.Constraint(###
        m.JB, m.M,###
        rule=lambda m, j, b, mm: m.U[j, b, mm] >= m.A[j, b] + m.x[j, mm] - 1###
    )###

    t = step(t, "constraints: U linearisation (JB x M)")


    # Serve once (and only after release)
    # def serve_once(m, j):
    #     return sum(m.A[j, b] for b in m.B if b_to_idx[b] >= release_idx[j]) == 1
    # m.ServeOnce = pyo.Constraint(m.J, rule=serve_once)
    
    
    def serve_once(m, j):
        buckets = jb_by_j.get(j, [])
        if not buckets:
            # No feasible start buckets → job is infeasible
            return pyo.Constraint.Infeasible

        return pyo.quicksum(m.A[j, b] for b in buckets) == 1


    m.ServeOnce = pyo.Constraint(m.J, rule=serve_once)###


    def no_service_before_release(m, j, b):
        if b_to_idx[b] < release_idx[j]:
            return m.A[j, b] == 0
        return pyo.Constraint.Skip
    
    m.NoServiceBeforeRelease = pyo.Constraint(m.JB, rule=no_service_before_release)###
    #m.NoServiceBeforeRelease = pyo.Constraint(m.J, m.B, rule=no_service_before_release)



    def no_service_after_deadline(m, j, b):
        idx = hard_deadline_idx.get(j, None)
        if idx is None:
            return pyo.Constraint.Skip
        if b_to_idx[b] > idx:
            return m.A[j, b] == 0
        return pyo.Constraint.Skip
    m.NoServiceAfterDeadline = pyo.Constraint(m.JB, rule=no_service_after_deadline)###
    # m.NoServiceAfterDeadline = pyo.Constraint(m.J, m.B, rule=no_service_after_deadline)



    t = step(t, "constraints: ServeOnce + release/deadline bounds")

    # SLA breach definition: delay <= L_j + M_BIG*y
    # def sla_rule(m, j):
    #     delay = sum((b_to_idx[b] - sla_start_idx[j]) * BUCKET_MINUTES * m.A[j, b] for b in m.B)
    #     return delay <= L_j[j] + M_BIG * m.y[j]
    # m.SLA = pyo.Constraint(m.J, rule=sla_rule)
    # t = step(t, "constraints: SLA")
    
    
    def sla_rule(m, j):
        buckets = jb_by_j.get(j, [])
        if not buckets:
            return pyo.Constraint.Infeasible
        delay = pyo.quicksum(
            (b_to_idx[b] - sla_start_idx[j]) * BUCKET_MINUTES * m.A[j, b]
            for b in buckets
        )
        return delay <= L_j[j] + M_BIG * m.y[j]

    m.SLA = pyo.Constraint(m.J, rule=sla_rule)###

    t = step(t, "constraints: SLA (JB-summed)")


    # Safety stands: disallow push unless Ryanair (only if needs_vertical too)
    def safety_rule(m, j):
        if int(jobs.loc[j, "safety_stand"]) == 1 and int(jobs.loc[j, "needs_vertical"]) == 1:
            if str(jobs.loc[j, "Airline Code"]) not in RYANAIR_CODES:
                return m.x[j, "Push"] == 0
        return pyo.Constraint.Skip
    m.SafetyStand = pyo.Constraint(m.J, rule=safety_rule)

    # Domestic ARRIVALS restriction: cannot use ambulift as horizontal mode
    def no_amb_horizontal_domestic_arrivals(m, j):
        if str(jobs.loc[j, "class"]) == "Dom" and str(jobs.loc[j, "dir"]) == "A":
            return m.x[j, "Amb"] == 0
        return pyo.Constraint.Skip
    m.NoAmbDomesticArrivals = pyo.Constraint(m.J, rule=no_amb_horizontal_domestic_arrivals)

    t = step(t, "constraints: safety + domestic arrival rules")

    
    def A_at(m, j, b):###
        return m.A[j, b] if (j, b) in jb_set else 0.0###

    def U_at(m, j, b, mm):###
        return m.U[j, b, mm] if (j, b) in jb_set else 0.0###


    
    def _as_pyomo_constraint(expr):
        # Convert trivial Python booleans into valid Pyomo constraint returns
        if expr is True:
            return pyo.Constraint.Feasible
        if expr is False:
            return pyo.Constraint.Infeasible
        return expr

    # # -----------------------------
    # # Vehicle physical capacity (seat + wheelchair)
    # # Uses U[j,b,m] and A[j,b]
    # # -----------------------------
    # def mini_seat_cap(m, f, b):
    #     Jf = jobs_by_f.get(f, [])
    #     demand = sum(m.U[j, b, "Mini"] for j in Jf)
    #     supply = sum(
    #         m.seatcap[("Mini", cid)] * m.k[("Mini", cid), (f, b)]
    #         for (vt, cid) in m.VC if vt == "Mini"
    #     )
    #     return demand <= supply

    # def mini_wc_cap(m, f, b):
    #     Jf = jobs_by_f.get(f, [])
    #     demand = sum(int(jobs.loc[j, "needs_wc"]) * m.U[j, b, "Mini"] for j in Jf)
    #     supply = sum(
    #         m.wccap[("Mini", cid)] * m.k[("Mini", cid), (f, b)]
    #         for (vt, cid) in m.VC if vt == "Mini"
    #     )
    #     return demand <= supply

    # def amb_seat_cap(m, f, b):
    #     Jf = jobs_by_f.get(f, [])
    #     vertical = sum(int(jobs.loc[j, "needs_vertical"]) * m.A[j, b] for j in Jf)
    #     horizontal_amb = sum(m.U[j, b, "Amb"] for j in Jf)
    #     demand = vertical + horizontal_amb
    #     supply = sum(
    #         m.seatcap[("Amb", cid)] * m.k[("Amb", cid), (f, b)]
    #         for (vt, cid) in m.VC if vt == "Amb"
    #     )
    #     return demand <= supply

    # def amb_wc_cap(m, f, b):
    #     Jf = jobs_by_f.get(f, [])
    #     vertical_wc = sum(int(jobs.loc[j, "needs_wc"]) * int(jobs.loc[j, "needs_vertical"]) * m.A[j, b] for j in Jf)
    #     horizontal_wc = sum(int(jobs.loc[j, "needs_wc"]) * m.U[j, b, "Amb"] for j in Jf)
    #     demand = vertical_wc + horizontal_wc
    #     supply = sum(
    #         m.wccap[("Amb", cid)] * m.k[("Amb", cid), (f, b)]
    #         for (vt, cid) in m.VC if vt == "Amb"
    #     )
    #     return demand <= supply

    
    # -----------------------------
    # Vehicle physical capacity (seat + wheelchair)
    # Uses U_at/A_at to handle sparse JB
    # -----------------------------
    def mini_seat_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        demand = sum(U_at(m, j, b, "Mini") for j in Jf)
        supply = sum(
            m.seatcap[("Mini", cid)] * m.k[("Mini", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Mini"
        )
        return demand <= supply

    def mini_wc_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        demand = sum(int(jobs.loc[j, "needs_wc"]) * U_at(m, j, b, "Mini") for j in Jf)
        supply = sum(
            m.wccap[("Mini", cid)] * m.k[("Mini", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Mini"
        )
        return demand <= supply

    def amb_seat_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        vertical = sum(int(jobs.loc[j, "needs_vertical"]) * A_at(m, j, b) for j in Jf)
        
        horizontal_amb = sum(
            U_at(m, j, b, "Amb") for j in Jf
            if int(jobs.loc[j, "needs_vertical"]) == 0
            )

        demand = vertical + horizontal_amb
        supply = sum(
            m.seatcap[("Amb", cid)] * m.k[("Amb", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Amb"
        )
        return demand <= supply

    def amb_wc_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        vertical_wc = sum(
            int(jobs.loc[j, "needs_wc"]) * int(jobs.loc[j, "needs_vertical"]) * A_at(m, j, b)
            for j in Jf
        )
        
        horizontal_wc = sum(
            int(jobs.loc[j, "needs_wc"]) * U_at(m, j, b, "Amb")
            for j in Jf
            if int(jobs.loc[j, "needs_vertical"]) == 0
        )

        demand = vertical_wc + horizontal_wc
        supply = sum(
            m.wccap[("Amb", cid)] * m.k[("Amb", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Amb"
        )
        return demand <= supply


    m.MiniSeatCap = pyo.Constraint(m.FB, rule=lambda m, f, b: mini_seat_cap(m, f, b))
    m.MiniWcCap = pyo.Constraint(m.FB, rule=lambda m, f, b: mini_wc_cap(m, f, b))
    m.AmbSeatCap = pyo.Constraint(m.FB, rule=lambda m, f, b: amb_seat_cap(m, f, b))
    m.AmbWcCap = pyo.Constraint(m.FB, rule=lambda m, f, b: amb_wc_cap(m, f, b))

    t = step(t, "constraints: per-flight vehicle capacity (seat/wc)")



    # -----------------------------
    # Fleet exclusivity per bucket:
    # each vehicle class has a fixed count and cannot be allocated to > count across flights in a bucket
    # -----------------------------
    # def fleet_exclusive(m, vtype, cid, b):
    #     used = sum(m.k[(vtype, cid), (f, b)] for f in m.F)
    #     cap = int(m.count[(vtype, cid)])
    #     return used <= cap
    
    def fleet_exclusive(m, vtype, cid, b):###
        flights_b = fb_by_b.get(b, [])###
        if not flights_b:###
            return pyo.Constraint.Feasible  ###

        used = pyo.quicksum(m.k[(vtype, cid), (f, b)] for f in flights_b)
        cap = int(m.count[(vtype, cid)])
        return used <= cap


    m.FleetExclusive = pyo.Constraint(m.VC, m.B, rule=lambda m, vtype, cid, b: fleet_exclusive(m, vtype, cid, b))

    t = step(t, "constraints: FleetExclusive (VC x B)")

    # Total minibus cap with ferry reservation (optional)
    # def total_mini_cap_with_ferry(m, b):
    #     total_used = sum(
    #         m.k[("Mini", cid), (f, b)]
    #         for (vt, cid) in m.VC if vt == "Mini"
    #         #for f in m.F
    #         for f in fb_by_b.get(b, []) ###
    #     )
    #     total_cap = sum(int(m.count[("Mini", cid)]) for (vt, cid) in m.VC if vt == "Mini")
    #     reserve = int(ferry_mini_reserved.get(b, 0))
    #     return total_used <= total_cap - reserve
    # m.TotalMiniCapWithFerry = pyo.Constraint(m.B, rule=total_mini_cap_with_ferry)
    
    def total_mini_cap_with_ferry(m, b): ###
        flights_b = fb_by_b.get(b, [])###
        if not flights_b:###
            return pyo.Constraint.Feasible  # <-- otherwise 0 <= (cap-reserve) becomes bool###

        total_used = pyo.quicksum(###
            m.k[("Mini", cid), (f, b)]###
            for (vt, cid) in m.VC if vt == "Mini"###
            for f in flights_b###
        )
        total_cap = sum(int(m.count[("Mini", cid)]) for (vt, cid) in m.VC if vt == "Mini")###
        reserve = int(ferry_mini_reserved.get(b, 0))###
        return total_used <= total_cap - reserve###

    m.TotalMiniCapWithFerry = pyo.Constraint(m.B, rule=total_mini_cap_with_ferry)###

    t = step(t, "constraints: TotalMiniCapWithFerry")


    # ---------------------------------------------------------------------
    # Time capacity per bucket (minutes) WITH SPILLOVER + combined-only standby
    # ---------------------------------------------------------------------
    spill = _precompute_spill_vectors(jobs, tau, BUCKET_MINUTES, toggles)
    amb_active = spill["amb_active"]
    mini_active = spill["mini_active"]
    amb_vert = spill["amb_vert"]
    standby_vert = spill["standby_vert"]
    standby_horiz = spill["standby_horiz"]

    
    # ---------------------------------------------------------------------
    # Aircraft docking bottleneck for vertical ambulift work
    # ---------------------------------------------------------------------
    # Operational rule:
    # - Vertical ambulift work requires docking to the aircraft.
    # - Only ONE ambulift can dock to a given flight at a time.
    # Bucket approximation:
    # - In any bucket, vertical ambulift minutes for a flight <= BUCKET_MINUTES * 1

    MAX_DOCKED_AMB_PER_FLIGHT = int(getattr(toggles, "max_docked_amb_per_flight", 1) or 1)

    def amb_vert_dock_cap(m, f, b):
        ib = b_to_idx[b]
        used = 0.0

        # Only jobs belonging to this flight can consume docking time on this flight
        Jf = jobs_by_f.get(f, [])
        if not Jf:
            return pyo.Constraint.Feasible

        for j in Jf:
            if int(jobs.loc[j, "needs_vertical"]) != 1:
                continue

            # Vertical minutes spill across buckets based on job start bucket
            for k, mins in enumerate(amb_vert[j]):
                i0 = ib - k
                if i0 < 0:
                    break
                b0 = idx_to_b[i0]
                used += mins * A_at(m, j, b0)

        expr = used <= float(BUCKET_MINUTES) * float(MAX_DOCKED_AMB_PER_FLIGHT)
        # handle trivial bools (can happen in empty buckets)
        if expr is True:
            return pyo.Constraint.Feasible
        if expr is False:
            return pyo.Constraint.Infeasible
        return expr

    m.AmbVerticalDockCap = pyo.Constraint(m.FB, rule=lambda m, f, b: amb_vert_dock_cap(m, f, b))

    t = step(t, f"constraints: AmbVerticalDockCap (<= {MAX_DOCKED_AMB_PER_FLIGHT} docked amb per flight per bucket)")


    def amb_time_cap(m, b):
        """
        Ambulift minute capacity in bucket b.

        Components:
        - Ambulift horizontal active minutes if horizontal mode = Amb
        - Ambulift vertical active minutes if needs_vertical==1 (independent of horizontal mode)
        - Standby minutes on vertical side (departures), ONLY for combined jobs:
            indicator = (A - U_Amb) = 1 when job starts and horizontal != Amb.
        """
        
        ib = b_to_idx[b]
        used = 0.0

        for j in m.J:
            for k, mins in enumerate(amb_active[j]):
                i0 = ib - k
                if i0 < 0:
                    break
                b0 = idx_to_b[i0]
                used += mins * U_at(m, j, b0, "Amb")

            if int(jobs.loc[j, "needs_vertical"]) == 1:
                for k, mins in enumerate(amb_vert[j]):
                    i0 = ib - k
                    if i0 < 0:
                        break
                    b0 = idx_to_b[i0]
                    used += mins * A_at(m, j, b0)

                for k, mins in enumerate(standby_vert[j]):
                    i0 = ib - k
                    if i0 < 0:
                        break
                    b0 = idx_to_b[i0]
                    used += mins * (A_at(m, j, b0) - U_at(m, j, b0, "Amb"))

        
        available = BUCKET_MINUTES * N_AMB - float(spin_removed.get(b, 0.0))
        return _as_pyomo_constraint(used <= available)


        # ib = b_to_idx[b]
        # used = 0.0

        # for j in m.J:
        #     # 1) ambulift horizontal active minutes (if Amb chosen)
        #     for k, mins in enumerate(amb_active[j]):
        #         i0 = ib - k
        #         if i0 < 0:
        #             break
        #         b0 = idx_to_b[i0]
        #         used += mins * m.U[j, b0, "Amb"]

        #     # 2) vertical active minutes (if needs_vertical)
        #     if int(jobs.loc[j, "needs_vertical"]) == 1:
        #         for k, mins in enumerate(amb_vert[j]):
        #             i0 = ib - k
        #             if i0 < 0:
        #                 break
        #             b0 = idx_to_b[i0]
        #             used += mins * m.A[j, b0]

        #         # 3) vertical-side standby (dep only via precompute), combined-only:
        #         # A - U_Amb = 0 if horizontal=Amb; =A if horizontal!=Amb
        #         for k, mins in enumerate(standby_vert[j]):
        #             i0 = ib - k
        #             if i0 < 0:
        #                 break
        #             b0 = idx_to_b[i0]
        #             used += mins * (m.A[j, b0] - m.U[j, b0, "Amb"])

        # available = BUCKET_MINUTES * N_AMB - float(spin_removed.get(b, 0.0))
        # return used <= available
    
    t = step(t, "spill vectors precomputed")

    def mini_time_cap(m, b):
        """
        Minibus minute capacity in bucket b.

        Components:
        - Minibus horizontal active minutes if horizontal mode = Mini
        - Horizontal-side standby minutes (arrivals), only applicable when Mini is chosen.
          (If horizontal mode is Push, standby should be represented in staff/pusher model later.)
        """
        
        ib = b_to_idx[b]
        used = 0.0

        for j in m.J:
            for k, mins in enumerate(mini_active[j]):
                i0 = ib - k
                if i0 < 0:
                    break
                b0 = idx_to_b[i0]
                used += mins * U_at(m, j, b0, "Mini")

            for k, mins in enumerate(standby_horiz[j]):
                i0 = ib - k
                if i0 < 0:
                    break
                b0 = idx_to_b[i0]
                used += mins * U_at(m, j, b0, "Mini")

        
        available = BUCKET_MINUTES * N_MINI
        return _as_pyomo_constraint(used <= available)

        # ib = b_to_idx[b]
        # used = 0.0

        # for j in m.J:
        #     # 1) minibus active minutes (if Mini chosen)
        #     for k, mins in enumerate(mini_active[j]):
        #         i0 = ib - k
        #         if i0 < 0:
        #             break
        #         b0 = idx_to_b[i0]
        #         used += mins * m.U[j, b0, "Mini"]

        #     # 2) arrival-side standby (precomputed), only counts when Mini chosen
        #     for k, mins in enumerate(standby_horiz[j]):
        #         i0 = ib - k
        #         if i0 < 0:
        #             break
        #         b0 = idx_to_b[i0]
        #         used += mins * m.U[j, b0, "Mini"]

        # available = BUCKET_MINUTES * N_MINI
        # return used <= available

    m.AmbTimeCap = pyo.Constraint(m.B, rule=amb_time_cap)
    m.MiniTimeCap = pyo.Constraint(m.B, rule=mini_time_cap)

    t = step(t, "constraints: AmbTimeCap + MiniTimeCap")

    # -----------------------------
    # Staff constraints per bucket
    # -----------------------------
    def amb_used(m, b):
        #return sum(m.k[("Amb", cid), (f, b)] for (vt, cid) in m.VC if vt == "Amb" for f in m.F)
        return sum(m.k[("Amb", cid), (f, b)] for (vt, cid) in m.VC if vt == "Amb" for f in fb_by_b.get(b, [])) ###

    def mini_used(m, b):
        #return sum(m.k[("Mini", cid), (f, b)] for (vt, cid) in m.VC if vt == "Mini" for f in m.F)
        return sum(m.k[("Mini", cid), (f, b)] for (vt, cid) in m.VC if vt == "Mini" for f in fb_by_b.get(b, [])) ###

    def drv_staff(m, b):
        required = amb_used(m, b) + mini_used(m, b) + int(ferry_drv_reserved.get(b, 0))
        return m.H_drv[b] >= required

    def vehag_staff(m, b):
        required = amb_used(m, b) + mini_used(m, b)
        return m.H_vehag[b] >= required

    def push_staff(m, b):
        # Pushers required 1:1 for push jobs served in bucket b
        # required = sum(m.U[j, b, "Push"] for j in m.J)
        required = pyo.quicksum(U_at(m, j, b, "Push") for j in jb_by_b.get(b, []))
        return m.H_push[b] >= required

    m.DriverStaff = pyo.Constraint(m.B, rule=drv_staff)
    m.VehAgStaff = pyo.Constraint(m.B, rule=vehag_staff)
    m.PushStaff = pyo.Constraint(m.B, rule=push_staff)

    t = step(t, "constraints: staff (Drv/VehAg/Push)")

    # -----------------------------
    # Gate 7/8 lift bottleneck (bucketed)
    # -----------------------------
    J_lift = [j for j in jobs.index if int(jobs.loc[j, "lift_gate"]) == 1 and int(jobs.loc[j, "needs_wc"]) == 1]

    def lift_cap(m, b):
        if len(J_lift) == 0:
            return pyo.Constraint.Feasible
       # wch = pyo.quicksum(m.A[j, b] for j in J_lift)
        wch = pyo.quicksum(A_at(m, j, b) for j in J_lift)
        return _as_pyomo_constraint(wch * float(LIFT_CYCLE_MINS) <= float(LIFT_CAPACITY_MINS))

    m.LiftCap = pyo.Constraint(m.B, rule=lift_cap)

    
    t = step(t, "constraints: LiftCap")

    t = step(t, f"build_pyomo_model complete | total={(time.perf_counter()-t0):0.2f}s")


    return m