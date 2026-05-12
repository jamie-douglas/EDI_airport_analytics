
# scripts/prm_opt/pyomo_model_v2.py

"""
Scenario 2 optimisation (no pooling across flights).

This model determines fleet size, vehicle deployment, and staffing requirements
for PRM operations under fixed passenger demand, without pooling passengers
across different flights.

The model is designed for **fleet sizing** and **operational feasibility**
assessment under time-bucketed constraints.

-----------------------------------------------------------------------
Model components
-----------------------------------------------------------------------

The optimisation includes the following decision elements:

- Job-level horizontal mode choice:
    x[j, m] ∈ {Amb, Mini, Push}

- Job-level service start bucket:
    A[j, b] = 1 if passenger job j starts service in time bucket b
    (used to model response delay and SLA compliance)

- Per-flight, per-bucket vehicle allocation:
    k[(vtype, class_id), (f, b)] = number of vehicles of a given type/class
    allocated to flight f in bucket b

- Flight-level vertical ambulift visit anchor:
    V[f, b] = 1 if the ambulift arrives at the aircraft for flight f in bucket b.
    This represents the aircraft attachment constraint: only one ambulift can be
    docked to a flight at a time.

- Fleet and staff availability constraints:
    * Vehicle availability per bucket (fleet size limits)
    * Driver, vehicle-agent, and pusher staffing per bucket

- Vehicle physical capacity constraints:
    * Seat capacity
    * Wheelchair slot capacity (for horizontal vehicle use)

- Operational bottlenecks:
    * Gate 7/8 lift capacity
    * Ambulift spin locking (time removed from fleet availability)

- Objective:
    Minimise total vehicle usage and operating costs, with heavy penalties for SLA violations.

-----------------------------------------------------------------------
Key modelling choices
-----------------------------------------------------------------------

Passenger jobs remain the unit of demand:
- Each PRM job is an individual service requirement.
- SLA timing, service start, and mode eligibility are defined per job.

Vehicles are the unit of time consumption:
- Vehicle busy minutes represent *fleet utilisation*, not passenger effort.
- Multiple passengers on the same flight may share a single vehicle visit.

Vertical requirement:
- If needs_vertical == 1, an ambulift must be present at the aircraft.
- Vertical processing is treated as an aircraft-side service sequence, not a set of
  parallel passenger-level vehicle tasks.

Standby / coordination time:
- When a flight requires vertical service and the horizontal mode is not ambulift
  (i.e., horizontal is Mini or Push), operations require a coordination window.
- This model represents that window as additional vehicle/staff minutes at the flight level:
    * Departures: standby time is charged to the ambulift visit
    * Arrivals:   standby time is charged to the chosen horizontal resource (Mini or Push)

Vertical capacity effect (multiple cycles):
- If vertical wheelchair demand exceeds the effective ambulift wheelchair capacity per cycle,
  the same ambulift must perform multiple cycles sequentially.
- This is represented by inflating the ambulift trip duration by:
      (cycles - 1) * vertical_cycle_mins
  where cycles = ceil(vertical_wc_demand / amb_wccap_effective).

Sensitivity (Option C):
- vertical_cycle_mins is a user-controlled toggle to run sensitivity scenarios
  (e.g., 0 / 5 / 10 / 15 minutes).

-----------------------------------------------------------------------
Intended use
-----------------------------------------------------------------------

This model is intended for:
- Fleet sizing (ambulifts, minibuses)
- Peak-day and peak-hour vehicle requirements
- Staffing requirement estimation
- SLA feasibility and sensitivity analysis

It is not intended to produce detailed vehicle routing.
"""

from __future__ import annotations

import math
import time
import pandas as pd
import pyomo.environ as pyo

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
# Utility helpers
# ---------------------------------------------------------------------

# These helpers exist to translate real-world operational timing
# (minutes busy, standby, repeated cycles) into bucket-based constraints
# without introducing continuous-time variables.

def _split_mins(total_mins: float, bucket_mins: int, k_cap: int) -> list[float]:
    """Front-loaded by design: reflects vehicles being occupied ASAP after dispatch.."""
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


def _as_pyomo_constraint(expr):
    """Convert trivial Python booleans into valid Pyomo constraint returns."""
    if expr is True:
        return pyo.Constraint.Feasible
    if expr is False:
        return pyo.Constraint.Infeasible
    return expr


def _flight_level_tau(jobs: pd.DataFrame, tau_job: dict) -> dict[tuple[str, str], float]:
    """
    Convert passenger-indexed tau[(j,mode)] into a flight-level trip duration proxy.

    For each flight f and mode m, use the median tau[(j,m)] across jobs on that flight.
    
    This function allows multiple passengers on the same flight
    to share a single vehicle visit in fleet sizing.

    """
    modes = ["Amb", "Mini", "Push"]
    out: dict[tuple[str, str], float] = {}

    global_med = {}
    for m in modes:
        vals = [float(tau_job[(j, m)]) for j in jobs.index if (j, m) in tau_job]
        global_med[m] = float(pd.Series(vals).median()) if vals else 0.0

    for f, grp in jobs.groupby("flight_key"):
        idx = list(grp.index)
        for m in modes:
            vals = [float(tau_job[(j, m)]) for j in idx if (j, m) in tau_job]
            out[(f, m)] = float(pd.Series(vals).median()) if vals else float(global_med[m])

    return out


def _inflate_amb_tau_for_vertical_cycles(
    jobs: pd.DataFrame,
    jobs_by_f: dict[str, list[int]],
    tau_f: dict[tuple[str, str], float],
    *,
    amb_wccap_effective: int,
    vertical_cycle_mins: float,
) -> dict[tuple[str, str], float]:
    
    """
    Inflate ambulift trip time when vertical wheelchair demand
    exceeds the capacity of one ambulift cycle.

    Operational interpretation:
    - One ambulift arrives at the aircraft
    - If more wheelchairs than fit per cycle, it must perform repeat cycles
    - These repeats block the same ambulift sequentially in time

    This is a TIME effect, not an extra vehicle requirement.
    """

    out = dict(tau_f)
    C = max(1, int(amb_wccap_effective))

    for f, Jf in jobs_by_f.items():
        if not Jf:
            continue

        wch_vert = sum(
            1
            for j in Jf
            if int(jobs.loc[j, "needs_vertical"]) == 1 and int(jobs.loc[j, "needs_wc"]) == 1
        )
        if wch_vert <= 0:
            continue

        cycles = int(math.ceil(wch_vert / C))
        if cycles <= 1:
            continue

        out[(f, "Amb")] = float(out[(f, "Amb")]) + float(cycles - 1) * float(vertical_cycle_mins)

    return out


def _precompute_trip_spill_vectors(
    flights: list[str],
    tau_f: dict[tuple[str, str], float],
    bucket_minutes: int,
    toggles: PlanningToggles,
) -> dict[tuple[str, str], list[float]]:
    """Precompute per-flight spill vectors for trip busy minutes."""
    k_cap = int(getattr(toggles, "spill_bucket_cap", 12) or 12)
    spill: dict[tuple[str, str], list[float]] = {}
    for f in flights:
        spill[(f, "Amb")] = _split_mins(tau_f[(f, "Amb")], bucket_minutes, k_cap)
        spill[(f, "Mini")] = _split_mins(tau_f[(f, "Mini")], bucket_minutes, k_cap)
    return spill


def _build_trip_time_params(
    *,
    jobs: pd.DataFrame,
    jobs_by_f: dict[str, list[int]],
    flights: list[str],
    tau_job: dict,
    toggles: PlanningToggles,
    bucket_minutes: int,
    amb_wccap_eff: int,
) -> tuple[
    dict[tuple[str, str], float],
    dict[tuple[str, str], list[float]],
    dict[str, list[float]],
    dict[str, list[float]],
    dict[str, str],
]:
    """
    Build all trip-level time parameters used by the model:
      - flight-level tau_f (Amb/Mini/Push proxies)
      - ambulift vertical-cycle inflation (Option C sensitivity knob)
      - trip spill vectors for Amb and Mini
      - standby spill vectors (direction-specific)
      - flight direction map
    """
    tau_f = _flight_level_tau(jobs, tau_job)

    
    # Centralised construction of all time-related parameters.
    # This isolates timing assumptions from the main model logic.
    # user-controlled additional minutes per extra vertical cycle
    vertical_cycle_mins = float(getattr(toggles, "vertical_cycle_mins", 0.0) or 0.0)
    if vertical_cycle_mins > 0:
        tau_f = _inflate_amb_tau_for_vertical_cycles(
            jobs,
            jobs_by_f,
            tau_f,
            amb_wccap_effective=amb_wccap_eff,
            vertical_cycle_mins=vertical_cycle_mins,
        )

    spill_trip = _precompute_trip_spill_vectors(flights, tau_f, bucket_minutes, toggles)

    # Standby spill vectors: charged once per flight when vertical+horizontal differ
    standby_dep_vert = float(getattr(toggles, "standby_dep_vert_mins", 0.0) or 0.0)
    standby_arr_horiz = float(getattr(toggles, "standby_arr_horiz_mins", 0.0) or 0.0)
    k_cap = int(getattr(toggles, "spill_bucket_cap", 12) or 12)

    flight_dir: dict[str, str] = {}
    for f in flights:
        Jf = jobs_by_f.get(f, [])
        flight_dir[f] = str(jobs.loc[Jf[0], "dir"]) if Jf else "A"

    standby_amb_vec: dict[str, list[float]] = {}
    standby_horiz_vec: dict[str, list[float]] = {}
    for f in flights:
        if flight_dir[f] == "D":
            standby_amb_vec[f] = _split_mins(standby_dep_vert, bucket_minutes, k_cap)
            standby_horiz_vec[f] = []
        else:
            standby_amb_vec[f] = []
            standby_horiz_vec[f] = _split_mins(standby_arr_horiz, bucket_minutes, k_cap)

    return tau_f, spill_trip, standby_amb_vec, standby_horiz_vec, flight_dir


# ---------------------------------------------------------------------
# Model builder
# ---------------------------------------------------------------------
def build_pyomo_model(
    jobs: pd.DataFrame,
    tau: dict,
    spin_removed: dict,
    toggles: PlanningToggles,
):
    
    """
    Build and return a Pyomo ConcreteModel for Scenario 2 (no pooling).

    Input:
    - jobs: canonical job table from build_jobs()
    - tau: job-level busy minutes (Amb/Mini/Push)
    - spin_removed: ambulift minutes removed per bucket due to spin locking
    - toggles: planning assumptions and sensitivity parameters
    """

    t0 = time.perf_counter()
    t = t0

    m = pyo.ConcreteModel()
    t = step(t, "ConcreteModel created")

    # -----------------------------
    # Constants
    # -----------------------------
    
    # Note:
    # - BUCKET_MINUTES is fixed at model-build time
    # - All other values may be overridden via PlanningToggles

    BUCKET_MINUTES = 15
    M_BIG = 10_000

    MAX_LATE_MINS = int(getattr(toggles, "max_late_mins", 180) or 180)
    MAX_DOCKED_AMB_PER_FLIGHT = max(1, int(getattr(toggles, "max_docked_amb_per_flight", 1) or 1))

    ferry_mini_reserved = getattr(toggles, "ferry_mini_reserved", {}) or {}
    ferry_drv_reserved = getattr(toggles, "ferry_drv_reserved", {}) or {}

    t = step(t, "constants + toggles loaded")

    # -----------------------------
    # Bucket timeline
    # -----------------------------
    
    # The bucket timeline must cover:
    # - earliest release
    # - latest SLA/deadline
    # - additional slack to allow post-SLA service with penalty

    t_min = pd.to_datetime(jobs["t"]).min()
    s_max = pd.to_datetime(jobs["s"]).max()
    max_sla = float(pd.to_numeric(jobs["sla_limit"]).max())

    sla_min = pd.to_datetime(jobs["sla_start_time"]).dropna().min() if "sla_start_time" in jobs.columns else t_min
    deadline_max = (
        pd.to_datetime(jobs["hard_deadline_time"]).dropna().max()
        if "hard_deadline_time" in jobs.columns
        else s_max
    )

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
    
    # Indices:
    # J = jobs (passengers)
    # F = flights
    # B = time buckets
    # M = horizontal modes

    m.J = pyo.Set(initialize=list(jobs.index))
    m.B = pyo.Set(initialize=B_list, ordered=True)
    m.M = pyo.Set(initialize=["Amb", "Mini", "Push"])

    flights = sorted(jobs["flight_key"].unique())
    m.F = pyo.Set(initialize=flights)

    t = step(t, f"sets built | J={len(jobs):,} | F={len(flights):,}")

    # -----------------------------
    # SLA maps
    # -----------------------------
    release_b = {j: pd.to_datetime(jobs.loc[j, "t"]) for j in jobs.index}
    release_idx = {j: b_to_idx[release_b[j]] for j in jobs.index}

    L_j = {j: float(jobs.loc[j, "sla_limit"]) for j in jobs.index}

    sla_start_b = {}
    for j in jobs.index:
        ts = jobs.loc[j, "sla_start_time"] if "sla_start_time" in jobs.columns else pd.NaT
        if pd.isna(ts):
            ts = jobs.loc[j, "s"] if "s" in jobs.columns else jobs.loc[j, "t"]
        sla_start_b[j] = pd.to_datetime(ts).floor(f"{BUCKET_MINUTES}min")
    sla_start_idx = {j: b_to_idx[sla_start_b[j]] for j in jobs.index}

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
    # Build sparse JB (job feasible start buckets)
    # -----------------------------

    
    # JB (Job–Bucket) defines the exact service start buckets that are
    # FEASIBLE for each job.
    #
    # Construction logic:
    # - A job cannot start before its release time
    # - A job cannot start after its SLA window (or hard deadline for departures)
    #
    # JB massively reduces model size by:
    # - Avoiding variables for impossible (j,b) combinations
    # - Enforcing timing feasibility structurally, before optimisation
    #
    # Result:
    # - A[j,b] variables only exist where service is allowed


    
    # Enumerate all feasible (job, bucket) pairs.
    # These define the domain of the service-start decision

    latest_idx = {}
    for j in jobs.index:
        if str(jobs.loc[j, "dir"]) == "D" and hard_deadline_idx.get(j) is not None:
            latest_idx[j] = int(hard_deadline_idx[j])
        else:
            latest_time = sla_start_b[j] + pd.to_timedelta(float(L_j[j] + MAX_LATE_MINS), unit="m")
            latest_time = pd.to_datetime(latest_time).ceil(f"{BUCKET_MINUTES}min")
            latest_idx[j] = min(int(b_to_idx.get(latest_time, len(B_list) - 1)), len(B_list) - 1)

    jb_pairs = []
    jb_set = set()
    for j in jobs.index:
        lo = int(release_idx[j])
        hi = min(int(latest_idx[j]), len(B_list) - 1)
        for bi in range(lo, hi + 1):
            pair = (j, idx_to_b[bi])
            jb_pairs.append(pair)
            jb_set.add(pair)

    m.JB = pyo.Set(dimen=2, initialize=jb_pairs)


    
    # Convenience index maps:
    # - jb_by_j[j] = list of buckets job j can start in
    # - jb_by_b[b] = list of jobs that can start in bucket b
    #
    # Used for:
    # - Serve-once constraints
    # - SLA calculation
    # - Staff and Lift constraints

    jb_by_j = {j: [] for j in jobs.index}
    jb_by_b = {b: [] for b in B_list}
    for (j, b) in jb_pairs:
        jb_by_j[j].append(b)
        jb_by_b[b].append(j)

    t = step(t, f"JB set built | JB={len(jb_pairs):,}")

    # -----------------------------
    # Build sparse FB (flight feasible buckets)
    # -----------------------------

    
    # FB (Flight–Bucket) defines the buckets in which a flight
    # could require vehicle resources.
    #
    # Construction logic:
    # - A flight is active from the earliest release of its jobs
    # - Until the latest feasible service time of any of its jobs
    #
    # FB is used for:
    # - Vehicle allocation k[(vtype,class),(f,b)]
    # - Vertical visit anchors V[f,b]
    # - Fleet exclusivity and time-capacity constraints
    #
    # NOTE:
    # Flights do NOT require resources outside FB by construction.


    
    # Group job indices by flight.
    # This is the bridge between passenger-level demand and flight-level resources.
    jobs_by_f: dict[str, list[int]] = {f: [] for f in flights}
    for j in jobs.index:
        jobs_by_f[jobs.loc[j, "flight_key"]].append(j)

    
    # Enumerate feasible (flight, bucket) pairs.
    # Vehicles can ONLY be allocated in these buckets for a flight.
    fb_pairs = []
    for f in flights:
        Jf = jobs_by_f.get(f, [])
        if not Jf:
            continue
        lo = min(release_idx[j] for j in Jf)
        hi = min(max(latest_idx[j] for j in Jf), len(B_list) - 1)
        for bi in range(lo, hi + 1):
            fb_pairs.append((f, idx_to_b[bi]))

    m.FB = pyo.Set(dimen=2, initialize=fb_pairs)

    
    # Convenience index map:
    # - fb_by_b[b] = list of flights active in bucket b
    #
    # Used for:
    # - Fleet exclusivity per bucket
    # - Time-capacity constraints
    # - Staffing aggregation


    fb_by_b = {b: [] for b in B_list}
    for (f, b) in fb_pairs:
        fb_by_b[b].append(f)


    
    # Summary:
    # - JB controls WHEN individual passengers may be served
    # - FB controls WHEN flights may consume shared resources
    # Together, they enforce timing feasibility at two levels:
    # passenger-level precision and fleet-level scalability.

    t = step(t, f"FB set built | FB={len(fb_pairs):,}")

    # -----------------------------
    # Vehicle classes
    # -----------------------------

    
    # Vehicle classes allow heterogeneous fleets (different sizes / capacities)
    # while still enforcing total fleet availability per bucket.
    # Vehicles are not routed; only their aggregate availability is tracked.


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

    m.VC = pyo.Set(dimen=2, initialize=vclasses)
    m.seatcap = pyo.Param(m.VC, initialize=seatcap)
    m.wccap = pyo.Param(m.VC, initialize=wccap)
    m.count = pyo.Param(m.VC, initialize=count)

    N_AMB = sum(int(count[(vt, cid)]) for (vt, cid) in vclasses if vt == "Amb")
    N_MINI = sum(int(count[(vt, cid)]) for (vt, cid) in vclasses if vt == "Mini")

    # Effective ambulift wheelchair capacity per vertical cycle (can be overridden by a toggle)
    amb_wccap_override = getattr(toggles, "vertical_wccap", None)
    if amb_wccap_override is not None:
        amb_wccap_eff = int(amb_wccap_override)
    else:
        amb_wccap_eff = max(int(m.wccap[("Amb", cid)]) for (vt, cid) in m.VC if vt == "Amb")

    t = step(t, f"vehicle classes set | VC={len(vclasses):,} | N_AMB={N_AMB} | N_MINI={N_MINI}")

    # -----------------------------
    # Decision variables
    # -----------------------------

    
    # Decision variables fall into four conceptual groups:
    #
    # 1) Passenger decisions:
    #    - x[j, m] : horizontal mode choice
    #    - A[j, b] : service start bucket
    #    - y[j]    : SLA breach indicator
    #
    # 2) Vehicle deployment decisions:
    #    - k[(vtype, class_id), (f, b)] : vehicles assigned to a flight in a bucket
    #    - V[f, b]                     : ambulift visit anchor at the aircraft
    #
    # 3) Flight-bucket indicators (derived / linearisation helpers):
    #    - Z_*  : which horizontal resources are used
    #    - W_*  : combined (vertical + horizontal ≠ ambulift) indicators for standby
    #
    # 4) Staffing requirements:
    #    - H_drv[b], H_vehag[b], H_push[b]



    # Passenger decisions
    m.x = pyo.Var(m.J, m.M, domain=pyo.Binary)           # horizontal mode choice
    m.A = pyo.Var(m.JB, domain=pyo.Binary)               # service start bucket
    m.y = pyo.Var(m.J, domain=pyo.Binary)                # SLA breach

    # Linearisation: U[j,b,m] = A[j,b] AND x[j,m]
    m.U = pyo.Var(m.JB, m.M, domain=pyo.Binary)

    # Vehicle allocations (fleet deployment decisions)
    m.k = pyo.Var(m.VC, m.FB, domain=pyo.NonNegativeIntegers)

    # Flight-level vertical ambulift visit anchor
    m.V = pyo.Var(m.FB, domain=pyo.Binary)

    # Flight-bucket indicators for horizontal resources (used for standby)
    m.Z_Mini = pyo.Var(m.FB, domain=pyo.Binary)
    m.Z_Push = pyo.Var(m.FB, domain=pyo.Binary)
    m.Z_NotAmb = pyo.Var(m.FB, domain=pyo.Binary)

    # Combined-operation flags (used for standby charging)
    m.W_CombAny = pyo.Var(m.FB, domain=pyo.Binary)   # V AND (Mini or Push)
    m.W_CombMini = pyo.Var(m.FB, domain=pyo.Binary)  # V AND Mini
    m.W_CombPush = pyo.Var(m.FB, domain=pyo.Binary)  # V AND Push

    # Staffing headcount per bucket
    m.H_drv = pyo.Var(m.B, domain=pyo.NonNegativeIntegers)
    m.H_vehag = pyo.Var(m.B, domain=pyo.NonNegativeIntegers)
    m.H_push = pyo.Var(m.B, domain=pyo.NonNegativeIntegers)

    # Variable size summary (useful for build-time and memory debugging)
    n_x = len(jobs) * len(m.M)
    n_A = len(jb_pairs)
    n_U = len(jb_pairs) * len(m.M)
    n_k = len(vclasses) * len(fb_pairs)
    n_V = len(fb_pairs)
    n_Z = len(fb_pairs)
    n_W = len(fb_pairs)

    t = step(
        t,
        "vars created | "
        f"x={n_x:,} | JB={len(jb_pairs):,} | A={n_A:,} | U={n_U:,} | "
        f"k={n_k:,} | V={n_V:,} | Z={n_Z:,} | W={n_W:,}"
    )

    # -----------------------------
    # Objective
    # -----------------------------

    
    # Objective priorities (highest to lowest):
    #
    # 1) Avoid SLA breaches (very high penalty)
    # 2) Minimise vehicles deployed (fleet sizing)
    # 3) Discourage operationally undesirable modes (soft penalties)
    # 4) Minimise staff and vehicle operating costs


    BIG = 1000.0
    LAMBDA = 1e6

    def obj_rule(m):
        trips_cost = BIG * pyo.quicksum(m.k[vc, fb] for vc in m.VC for fb in m.FB)

        soft = pyo.quicksum(
            PENALTY["AMB_HORIZONTAL"] * m.x[j, "Amb"]
            + PENALTY["PUSH"] * m.x[j, "Push"]
            + PENALTY["TRANSFER"] * int(jobs.loc[j, "needs_vertical"]) * m.x[j, "Mini"]
            for j in m.J
        )

        sla_pen = LAMBDA * pyo.quicksum(m.y[j] for j in m.J)

        WAGE = {"Drv": 1.0, "VehAg": 1.0, "Push": 1.0}
        staff_reg = pyo.quicksum(
            WAGE["Drv"] * m.H_drv[b] + WAGE["VehAg"] * m.H_vehag[b] + WAGE["Push"] * m.H_push[b]
            for b in m.B
        )

        veh_capex = pyo.quicksum(
            float(VEHICLE_MODELS.get(cid, {}).get("capex_hr", 0.0))
            * pyo.quicksum(m.k[(vtype, cid), fb] for fb in m.FB)
            for (vtype, cid) in m.VC
        )

        return trips_cost + soft + sla_pen + staff_reg + veh_capex

    m.OBJ = pyo.Objective(rule=obj_rule, sense=pyo.minimize)
    t = step(t, "objective created")

    # -----------------------------
    # Core constraints: passenger mode + start bucket + SLA
    # -----------------------------
    m.OneMode = pyo.Constraint(m.J, rule=lambda m, j: pyo.quicksum(m.x[j, mm] for mm in m.M) == 1)

    # Linearisation constraints on sparse JB
    m.U_le_A = pyo.Constraint(m.JB, m.M, rule=lambda m, j, b, mm: m.U[j, b, mm] <= m.A[j, b])
    m.U_le_X = pyo.Constraint(m.JB, m.M, rule=lambda m, j, b, mm: m.U[j, b, mm] <= m.x[j, mm])
    m.U_ge_AND = pyo.Constraint(m.JB, m.M, rule=lambda m, j, b, mm: m.U[j, b, mm] >= m.A[j, b] + m.x[j, mm] - 1)
    t = step(t, "constraints: U linearisation (JB x M)")

    # Serve once within feasible window
    def serve_once(m, j):
        buckets = jb_by_j.get(j, [])
        if not buckets:
            return pyo.Constraint.Infeasible
        return pyo.quicksum(m.A[j, b] for b in buckets) == 1

    m.ServeOnce = pyo.Constraint(m.J, rule=serve_once)

    # Guardrail: no service before release time
    def no_service_before_release(m, j, b):
        if b_to_idx[b] < release_idx[j]:
            return m.A[j, b] == 0
        return pyo.Constraint.Skip

    m.NoServiceBeforeRelease = pyo.Constraint(m.JB, rule=no_service_before_release)

    # Departures: no service after hard deadline
    def no_service_after_deadline(m, j, b):
        idx = hard_deadline_idx.get(j, None)
        if idx is None:
            return pyo.Constraint.Skip
        if b_to_idx[b] > idx:
            return m.A[j, b] == 0
        return pyo.Constraint.Skip

    m.NoServiceAfterDeadline = pyo.Constraint(m.JB, rule=no_service_after_deadline)

    # SLA breach definition: delay <= L_j + M_BIG * y
    def sla_rule(m, j):
        buckets = jb_by_j.get(j, [])
        if not buckets:
            return pyo.Constraint.Infeasible
        delay = pyo.quicksum((b_to_idx[b] - sla_start_idx[j]) * BUCKET_MINUTES * m.A[j, b] for b in buckets)
        return delay <= L_j[j] + M_BIG * m.y[j]

    m.SLA = pyo.Constraint(m.J, rule=sla_rule)
    t = step(t, "constraints: passenger timing + SLA")

    # Eligibility rules
    def safety_rule(m, j):
        if int(jobs.loc[j, "safety_stand"]) == 1 and int(jobs.loc[j, "needs_vertical"]) == 1:
            if str(jobs.loc[j, "Airline Code"]) not in RYANAIR_CODES:
                return m.x[j, "Push"] == 0
        return pyo.Constraint.Skip

    m.SafetyStand = pyo.Constraint(m.J, rule=safety_rule)

    def no_amb_horizontal_domestic_arrivals(m, j):
        if str(jobs.loc[j, "class"]) == "Dom" and str(jobs.loc[j, "dir"]) == "A":
            return m.x[j, "Amb"] == 0
        return pyo.Constraint.Skip

    m.NoAmbDomesticArrivals = pyo.Constraint(m.J, rule=no_amb_horizontal_domestic_arrivals)
    t = step(t, "constraints: eligibility")

    # Safe accessors for sparse variables
    def A_at(m, j, b):
        return m.A[j, b] if (j, b) in jb_set else 0.0

    def U_at(m, j, b, mm):
        return m.U[j, b, mm] if (j, b) in jb_set else 0.0

    # -----------------------------
    # Flight-level vertical ambulift visit
    # -----------------------------

    
    # This models aircraft-side attachment of an ambulift.
    #
    # Key point:
    # - A visit is a TIME anchor, not a passenger assignment.
    # - Multiple passengers can be served during/after a single visit,
    #   subject to time and capacity constraints elsewhere.


    # Flights that have at least one vertical passenger require a vertical visit anchor.
    flight_has_vertical = {f: int((jobs.loc[jobs_by_f[f], "needs_vertical"] == 1).any()) for f in flights}

    # One (or more) vertical visit anchors per flight (default 1). If no vertical passengers, no visit.
    def vertical_visit_count(m, f):
        if flight_has_vertical.get(f, 0) == 0:
            return pyo.quicksum(m.V[f, b] for (ff, b) in m.FB if ff == f) == 0
        return pyo.quicksum(m.V[f, b] for (ff, b) in m.FB if ff == f) == MAX_DOCKED_AMB_PER_FLIGHT

    m.VerticalVisitCount = pyo.Constraint(m.F, rule=vertical_visit_count)

    # If a flight has a visit anchor in bucket b, at least one ambulift must be allocated then.
    def V_requires_ambulift(m, f, b):
        amb_cnt = pyo.quicksum(m.k[("Amb", cid), (f, b)] for (vt, cid) in m.VC if vt == "Amb")
        return amb_cnt >= m.V[f, b]

    m.VerticalVisitRequiresAmb = pyo.Constraint(m.FB, rule=lambda m, f, b: V_requires_ambulift(m, f, b))

    # Vertical passengers must start at or after the visit anchor for their flight.
    # This avoids forcing all vertical passengers into the exact same bucket while still
    # enforcing "ambulift is present at the aircraft before vertical service begins".
    #
    # Implementation: for each vertical job j and bucket b, A[j,b] <= sum_{b0 <= b} V[f,b0]
    # (i.e., if a passenger starts at b, the flight must have chosen an anchor at or before b).
    fb_buckets_by_f = {f: sorted([b for (ff, b) in fb_pairs if ff == f], key=lambda x: b_to_idx[x]) for f in flights}
    fb_prefix_by_f = {
        f: {b: [b0 for b0 in fb_buckets_by_f[f] if b_to_idx[b0] <= b_to_idx[b]] for b in fb_buckets_by_f[f]}
        for f in flights
    }

    def vertical_start_requires_prior_visit(m, j, b):
        if int(jobs.loc[j, "needs_vertical"]) != 1:
            return pyo.Constraint.Skip
        f = str(jobs.loc[j, "flight_key"])
        # sum of visit anchors up to and including b
        prefix = fb_prefix_by_f[f].get(b, [])
        if not prefix:
            return m.A[j, b] == 0
        return m.A[j, b] <= pyo.quicksum(m.V[f, b0] for b0 in prefix)

    m.VerticalStartRequiresVisit = pyo.Constraint(m.JB, rule=vertical_start_requires_prior_visit)
    t = step(t, "constraints: vertical visit")

    # -----------------------------
    # Flight-bucket indicators for standby (Mini / Push)
    # -----------------------------
    # Z_Mini[f,b] >= U[j,b,Mini] for any passenger j on flight f
    def zmini_ge(m, j, b):
        f = str(jobs.loc[j, "flight_key"])
        return m.Z_Mini[f, b] >= m.U[j, b, "Mini"]

    m.ZMini_ge_U = pyo.Constraint(m.JB, rule=zmini_ge)

    def zpush_ge(m, j, b):
        f = str(jobs.loc[j, "flight_key"])
        return m.Z_Push[f, b] >= m.U[j, b, "Push"]

    m.ZPush_ge_U = pyo.Constraint(m.JB, rule=zpush_ge)

    # Z_NotAmb is true if Mini or Push is used (in that flight-bucket)
    def znotamb_ge_mini(m, f, b): return m.Z_NotAmb[f, b] >= m.Z_Mini[f, b]
    def znotamb_ge_push(m, f, b): return m.Z_NotAmb[f, b] >= m.Z_Push[f, b]
    def znotamb_le_sum(m, f, b): return m.Z_NotAmb[f, b] <= m.Z_Mini[f, b] + m.Z_Push[f, b]

    m.ZNotAmb_ge_Mini = pyo.Constraint(m.FB, rule=znotamb_ge_mini)
    m.ZNotAmb_ge_Push = pyo.Constraint(m.FB, rule=znotamb_ge_push)
    m.ZNotAmb_le_Sum = pyo.Constraint(m.FB, rule=znotamb_le_sum)

    # Combined flags (AND linearisations)
    def wcombany_1(m, f, b): return m.W_CombAny[f, b] <= m.V[f, b]
    def wcombany_2(m, f, b): return m.W_CombAny[f, b] <= m.Z_NotAmb[f, b]
    def wcombany_3(m, f, b): return m.W_CombAny[f, b] >= m.V[f, b] + m.Z_NotAmb[f, b] - 1
    m.WCombAny_1 = pyo.Constraint(m.FB, rule=wcombany_1)
    m.WCombAny_2 = pyo.Constraint(m.FB, rule=wcombany_2)
    m.WCombAny_3 = pyo.Constraint(m.FB, rule=wcombany_3)

    def wcombmini_1(m, f, b): return m.W_CombMini[f, b] <= m.V[f, b]
    def wcombmini_2(m, f, b): return m.W_CombMini[f, b] <= m.Z_Mini[f, b]
    def wcombmini_3(m, f, b): return m.W_CombMini[f, b] >= m.V[f, b] + m.Z_Mini[f, b] - 1
    m.WCombMini_1 = pyo.Constraint(m.FB, rule=wcombmini_1)
    m.WCombMini_2 = pyo.Constraint(m.FB, rule=wcombmini_2)
    m.WCombMini_3 = pyo.Constraint(m.FB, rule=wcombmini_3)

    def wcombpush_1(m, f, b): return m.W_CombPush[f, b] <= m.V[f, b]
    def wcombpush_2(m, f, b): return m.W_CombPush[f, b] <= m.Z_Push[f, b]
    def wcombpush_3(m, f, b): return m.W_CombPush[f, b] >= m.V[f, b] + m.Z_Push[f, b] - 1
    m.WCombPush_1 = pyo.Constraint(m.FB, rule=wcombpush_1)
    m.WCombPush_2 = pyo.Constraint(m.FB, rule=wcombpush_2)
    m.WCombPush_3 = pyo.Constraint(m.FB, rule=wcombpush_3)

    t = step(t, "constraints: standby indicators")

    # -----------------------------
    # Vehicle physical capacity (seat + wheelchair)
    # -----------------------------
    # Minibus capacity constraints remain passenger-based (shared within k).
    def mini_seat_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        demand = pyo.quicksum(U_at(m, j, b, "Mini") for j in Jf)
        supply = pyo.quicksum(
            m.seatcap[("Mini", cid)] * m.k[("Mini", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Mini"
        )
        return demand <= supply

    def mini_wc_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        demand = pyo.quicksum(int(jobs.loc[j, "needs_wc"]) * U_at(m, j, b, "Mini") for j in Jf)
        supply = pyo.quicksum(
            m.wccap[("Mini", cid)] * m.k[("Mini", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Mini"
        )
        return demand <= supply

    # Ambulift capacity constraints apply to horizontal ambulift usage only.
    # Vertical wheelchair sequencing is represented through trip time inflation.
    def amb_seat_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        demand = pyo.quicksum(
            U_at(m, j, b, "Amb")
            for j in Jf
            if int(jobs.loc[j, "needs_vertical"]) == 0
        )
        supply = pyo.quicksum(
            m.seatcap[("Amb", cid)] * m.k[("Amb", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Amb"
        )
        return demand <= supply

    def amb_wc_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        demand = pyo.quicksum(
            int(jobs.loc[j, "needs_wc"]) * U_at(m, j, b, "Amb")
            for j in Jf
            if int(jobs.loc[j, "needs_vertical"]) == 0
        )
        supply = pyo.quicksum(
            m.wccap[("Amb", cid)] * m.k[("Amb", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Amb"
        )
        return demand <= supply

    m.MiniSeatCap = pyo.Constraint(m.FB, rule=lambda m, f, b: mini_seat_cap(m, f, b))
    m.MiniWcCap = pyo.Constraint(m.FB, rule=lambda m, f, b: mini_wc_cap(m, f, b))
    m.AmbSeatCap = pyo.Constraint(m.FB, rule=lambda m, f, b: amb_seat_cap(m, f, b))
    m.AmbWcCap = pyo.Constraint(m.FB, rule=lambda m, f, b: amb_wc_cap(m, f, b))

    t = step(t, "constraints: per-flight capacity (seat/wc)")

    # -----------------------------
    # Fleet exclusivity per bucket (cannot allocate more vehicles than exist)
    # -----------------------------
    def fleet_exclusive(m, vtype, cid, b):
        flights_b = fb_by_b.get(b, [])
        if not flights_b:
            return pyo.Constraint.Feasible
        used = pyo.quicksum(m.k[(vtype, cid), (f, b)] for f in flights_b)
        cap = int(m.count[(vtype, cid)])
        return used <= cap

    m.FleetExclusive = pyo.Constraint(m.VC, m.B, rule=lambda m, vtype, cid, b: fleet_exclusive(m, vtype, cid, b))

    # Total minibus cap with ferry reservation
    def total_mini_cap_with_ferry(m, b):
        flights_b = fb_by_b.get(b, [])
        if not flights_b:
            return pyo.Constraint.Feasible
        total_used = pyo.quicksum(
            m.k[("Mini", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Mini"
            for f in flights_b
        )
        total_cap = sum(int(m.count[("Mini", cid)]) for (vt, cid) in m.VC if vt == "Mini")
        reserve = int(ferry_mini_reserved.get(b, 0))
        return total_used <= total_cap - reserve

    m.TotalMiniCapWithFerry = pyo.Constraint(m.B, rule=total_mini_cap_with_ferry)
    t = step(t, "constraints: FleetExclusive + TotalMiniCapWithFerry")

    # -----------------------------
    # Trip-level time capacity per bucket (vehicle-based)
    # -----------------------------

    
    # Vehicle time capacity is enforced at the FLEET level.
    #
    # Busy minutes are aggregated across all flights in a bucket.
    # This captures utilisation and bottlenecks without routing vehicles explicitly.



    tau_f, spill_trip, standby_amb_vec, standby_horiz_vec, flight_dir = _build_trip_time_params(
        jobs=jobs,
        jobs_by_f=jobs_by_f,
        flights=flights,
        tau_job=tau,
        toggles=toggles,
        bucket_minutes=BUCKET_MINUTES,
        amb_wccap_eff=amb_wccap_eff,
    )

    def amb_alloc(m, f, b):
        return pyo.quicksum(m.k[("Amb", cid), (f, b)] for (vt, cid) in m.VC if vt == "Amb")

    def mini_alloc(m, f, b):
        return pyo.quicksum(m.k[("Mini", cid), (f, b)] for (vt, cid) in m.VC if vt == "Mini")

    def amb_time_cap(m, b):
        ib = b_to_idx[b]
        used = 0.0

        # Trip busy minutes from allocated ambulifts
        for f in flights:
            vec = spill_trip[(f, "Amb")]
            for k_idx, mins in enumerate(vec):
                i0 = ib - k_idx
                if i0 < 0:
                    break
                b0 = idx_to_b[i0]
                if (f, b0) in m.FB:
                    used += float(mins) * amb_alloc(m, f, b0)

            # Departure standby: ambulift waits early when vertical + (Mini or Push)
            vec_sb = standby_amb_vec.get(f, [])
            for k_idx, mins in enumerate(vec_sb):
                i0 = ib - k_idx
                if i0 < 0:
                    break
                b0 = idx_to_b[i0]
                if (f, b0) in m.FB:
                    used += float(mins) * m.W_CombAny[f, b0]

        available = BUCKET_MINUTES * N_AMB - float(spin_removed.get(b, 0.0))
        return _as_pyomo_constraint(used <= available)

    def mini_time_cap(m, b):
        ib = b_to_idx[b]
        used = 0.0

        for f in flights:
            vec = spill_trip[(f, "Mini")]
            for k_idx, mins in enumerate(vec):
                i0 = ib - k_idx
                if i0 < 0:
                    break
                b0 = idx_to_b[i0]
                if (f, b0) in m.FB:
                    used += float(mins) * mini_alloc(m, f, b0)

            # Arrival standby: minibus waits early when vertical + Mini
            vec_sb = standby_horiz_vec.get(f, [])
            for k_idx, mins in enumerate(vec_sb):
                i0 = ib - k_idx
                if i0 < 0:
                    break
                b0 = idx_to_b[i0]
                if (f, b0) in m.FB:
                    used += float(mins) * m.W_CombMini[f, b0]

        available = BUCKET_MINUTES * N_MINI
        return _as_pyomo_constraint(used <= available)

    m.AmbTimeCap = pyo.Constraint(m.B, rule=amb_time_cap)
    m.MiniTimeCap = pyo.Constraint(m.B, rule=mini_time_cap)
    t = step(t, "constraints: AmbTimeCap + MiniTimeCap (trip-based)")

    # -----------------------------
    # Staff constraints per bucket
    # -----------------------------

    
    # Staffing constraints translate vehicle deployment into headcount needs.
    # One driver and one vehicle-agent are required per dispatched vehicle.
    # Pushers are required per passenger using Push mode.


    def amb_used(m, b):
        flights_b = fb_by_b.get(b, [])
        return pyo.quicksum(amb_alloc(m, f, b) for f in flights_b)

    def mini_used(m, b):
        flights_b = fb_by_b.get(b, [])
        return pyo.quicksum(mini_alloc(m, f, b) for f in flights_b)

    def drv_staff(m, b):
        required = amb_used(m, b) + mini_used(m, b) + int(ferry_drv_reserved.get(b, 0))
        return m.H_drv[b] >= required

    def vehag_staff(m, b):
        required = amb_used(m, b) + mini_used(m, b)
        return m.H_vehag[b] >= required

    def push_staff(m, b):
        # One pusher per push passenger starting in bucket b
        required = pyo.quicksum(U_at(m, j, b, "Push") for j in jb_by_b.get(b, []))
        return m.H_push[b] >= required

    m.DriverStaff = pyo.Constraint(m.B, rule=drv_staff)
    m.VehAgStaff = pyo.Constraint(m.B, rule=vehag_staff)
    m.PushStaff = pyo.Constraint(m.B, rule=push_staff)
    t = step(t, "constraints: staff")

    # -----------------------------
    # Gate 7/8 lift bottleneck (bucketed)
    # -----------------------------

    
    # This constrains START rates through the physical lift at Gates 7/8.
    #
    # Notes:
    # - Independent of ambulift fleet size
    # - Limits how many wheelchair passengers can START service per bucket
    # - Prevents unrealistic clustering at the lift


    J_lift = [j for j in jobs.index if int(jobs.loc[j, "lift_gate"]) == 1 and int(jobs.loc[j, "needs_wc"]) == 1]

    def lift_cap(m, b):
        if len(J_lift) == 0:
            return pyo.Constraint.Feasible
        wch = pyo.quicksum(A_at(m, j, b) for j in J_lift)
        return _as_pyomo_constraint(wch * float(LIFT_CYCLE_MINS) <= float(LIFT_CAPACITY_MINS))

    m.LiftCap = pyo.Constraint(m.B, rule=lift_cap)
    t = step(t, "constraints: LiftCap")

    t = step(t, f"build_pyomo_model complete | total={(time.perf_counter() - t0):0.2f}s")
    return m
