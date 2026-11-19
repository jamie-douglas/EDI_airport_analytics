# scripts/prm_opt/pyomo_model.py

"""
Scenario 2 optimisation (no pooling across flights).


Implements Model Scope components:
- job-level horizontal mode decisions x[j,m]
- service bucket assignment A[j,b] to model response delay and SLA
- vehicle time availability per bucket (fleet)
- spin locking removes ambulift minutes per bucket
- vehicle physical capacity (wheelchair slot capacity)
- Gate 7/8 lift bottleneck
- heterogeneous fleet capacities via vehicle classes
- per-flight per-bucket vehicle assignment counts (k) to prevent over/under allocation
- objective: minimise vehicles dispatched + soft penalties (NO COSTS) + SLA penalty

Key modelling choices (aligned to your latest intent):
- If needs_vertical==1, the vertical component ALWAYS consumes Ambulift resource/time.
- Horizontal mode choice can still be Amb, Mini, or Push.
  (So vertical+Mini and vertical+Push are allowed if other rules allow.)


Performance changes:
- Build *minimal* service bucket set B = union of feasible service windows
- Build *minimal* (job,bucket) set JB and define A only on JB
- Build *minimal* (flight,bucket) set FB and define k only on FB
- Allow jobs to be served late up to max_late_mins (still penalised by y[j]


"""

import pyomo.environ as pyo
import pandas as pd

from .config import (
    RYANAIR_CODES, PENALTY,
    LIFT_CYCLE_MINS, LIFT_CAPACITY_MINS,
    PlanningToggles,
)
from .params import build_vehicle_classes


def build_pyomo_model(
    jobs,
    tau,
    spin_removed,
    toggles: PlanningToggles,
):
    
    
    """
        jobs: DataFrame from build_jobs. Required columns:
        - flight_key
        - t : release bucket (prepositioned)
        - s : scheduled bucket (original job start bucket)
        - needs_wc, needs_vertical, safety_stand, lift_gate
        - sla_limit (already includes sla_buffer_mins)
        - Airline Code, dir, class
        tau: dict[(j, mode)->minutes] for modes: Amb, Mini, Push
        spin_removed: dict[bucket_timestamp -> minutes] removed from ambulift capacity
                    (indexed using SERVICE buckets)
        """



    m = pyo.ConcreteModel()

    
    # -----------------------------
    # Constants
    # -----------------------------
    BUCKET_MINUTES = 15
    M_BIG = 10_000

    
    # Allow service beyond SLA on extreme peaks (still penalised)
    max_late_mins = int(getattr(toggles, "max_late_mins", 0) or 0)


    
    # OPTIONAL placeholders for later (do nothing unless provided)
    ferry_mini_reserved = getattr(toggles, "ferry_mini_reserved", {}) or {}
    ferry_drv_reserved = getattr(toggles, "ferry_drv_reserved", {}) or {}

    
    # -----------------------------
    # SLA params
    # -----------------------------
    # L_j = SLA minutes window (already includes sla_buffer_mins logic from build_jobs)
    L_j = {j: float(jobs.loc[j, "sla_limit"]) for j in jobs.index}

    # -----------------------------
    # Build MINIMAL service bucket set B
    # -----------------------------
    # For each job j, allow service in:
    #   b ∈ [t(j), s(j) + L_j + max_late]
    #
    # We do NOT build a continuous season-long timeline.

    # floor inputs to bucket boundaries
    t_rel = pd.to_datetime(jobs["t"]).dt.floor(f"{BUCKET_MINUTES}min")
    s_ref = pd.to_datetime(jobs["s"]).dt.floor(f"{BUCKET_MINUTES}min")

    # base time for integer indexing
    t0 = min(t_rel.min(), s_ref.min())

    # bucket index helper
    def to_bucket_idx(ts: pd.Timestamp) -> int:
        return int(((ts - t0).total_seconds() / 60.0) // BUCKET_MINUTES)

    def idx_to_ts(i: int) -> pd.Timestamp:
        return t0 + pd.to_timedelta(i * BUCKET_MINUTES, unit="m")

    needed_bucket_idx = set()
    JB_list = []          # feasible (job, bucket) pairs
    FB_idx_set = set()    # feasible (flight, bucket_idx) pairs

    # precompute release and latest-service bucket indices per job
    release_idx = {}
    latest_idx = {}

    for j in jobs.index:
        t_j = t_rel.loc[j]
        s_j = s_ref.loc[j]

        start_i = to_bucket_idx(t_j)

        # latest allowed bucket index = s + SLA + max_late
        latest_time = s_j + pd.to_timedelta(L_j[j] + max_late_mins, unit="m")
        end_i = to_bucket_idx(latest_time)

        release_idx[j] = start_i
        latest_idx[j] = end_i

        # add buckets and build feasible pairs
        f = str(jobs.loc[j, "flight_key"])
        for i in range(start_i, end_i + 1):
            needed_bucket_idx.add(i)
            FB_idx_set.add((f, i))

    # create actual timestamps for B
    B_list = [idx_to_ts(i) for i in sorted(needed_bucket_idx)]
    b_to_idx = {b: k for k, b in enumerate(B_list)}  # local ordering index (0..|B|-1)

    # build JB_list using timestamps that exist in B_list
    B_set = set(B_list)
    for j in jobs.index:
        f = str(jobs.loc[j, "flight_key"])
        for i in range(release_idx[j], latest_idx[j] + 1):
            b = idx_to_ts(i)
            if b in B_set:
                JB_list.append((j, b))

    # build FB list
    FB_list = [(f, idx_to_ts(i)) for (f, i) in sorted(FB_idx_set) if idx_to_ts(i) in B_set]

    
    # # -----------------------------
    # # Build SERVICE bucket timeline
    # # -----------------------------
    # # Due to t/s split:
    # # - jobs["t"] can be earlier than jobs["s"] (preposition)
    # # - model must still allow service at the normal "s" times
    # # - model must allow delays up to SLA (or breaches)
    # #
    # # So we build a continuous bucket timeline from min(t) to max(s) + slack.
    # t_min = pd.to_datetime(jobs["t"]).min()
    # s_max = pd.to_datetime(jobs["s"]).max()
    # max_sla = float(pd.to_numeric(jobs["sla_limit"]).max())
    # slack_buckets = int((max_sla // BUCKET_MINUTES) + 2)
    # end_time = s_max + pd.to_timedelta(slack_buckets * BUCKET_MINUTES, unit="m")

    # B_list = list(pd.date_range(start=t_min, end=end_time, freq=f"{BUCKET_MINUTES}min"))
    # b_to_idx = {b: i for i, b in enumerate(B_list)}

    # -----------------------------
    # Sets
    # -----------------------------
    m.J = pyo.Set(initialize=list(jobs.index))
    m.B = pyo.Set(initialize=B_list, ordered=True)

    
    # feasible job-bucket pairs only
    m.JB = pyo.Set(dimen=2, initialize=JB_list)

    #Horizontal models
    m.M = pyo.Set(initialize=["Amb", "Mini", "Push"])

    # Flight-time indexing (no pooling across flights)
    flights = sorted(jobs["flight_key"].unique())
    m.F = pyo.Set(initialize=flights)

    # Vehicle assignment index: flight x service bucket
    #fb_pairs = [(f, b) for f in flights for b in B_list]
    m.FB = pyo.Set(dimen=2, initialize=FB_list) ##fb_pairs)

    
    # convenient: feasible buckets per job (Python dict)
    JB_by_j = {j: [] for j in jobs.index}
    for (j, b) in JB_list:
        JB_by_j[j].append(b)


    
    # # -----------------------------
    # # SLA parameters
    # # -----------------------------
    # release_b = {j: pd.to_datetime(jobs.loc[j, "t"]) for j in jobs.index}
    # release_idx = {j: b_to_idx[release_b[j]] for j in jobs.index}

    # # Use the job-specific SLA limit already computed in build_jobs
    # L_j = {j: float(jobs.loc[j, "sla_limit"]) for j in jobs.index}


    # -----------------------------
    # Vehicle classes (heterogeneous fleet)
    # -----------------------------

    classes = build_vehicle_classes(include_future=False)

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


    # -----------------------------
    # Decision variables
    # -----------------------------
    m.x = pyo.Var(m.J, m.M, domain=pyo.Binary)  # job horizontal mode


    ##m.A = pyo.Var(m.J, m.B, domain=pyo.Binary)   # service bucket assignment
    m.A = pyo.Var(m.JB, domain=pyo.Binary) #service assignment only on feasible JB
    m.y = pyo.Var(m.J, domain=pyo.Binary)        # SLA breach indicator
    m.late_mins = pyo.Var(m.J, domain=pyo.NonNegativeReals)

    # number of vehicles of each class assigned to each (flight, bucket)
    m.k = pyo.Var(m.VC, m.FB, domain=pyo.NonNegativeIntegers)

    #Staffing headcount per service bucket
    # Rule: Drivers >= vehicles used (+ ferry reserved); VehAg same; Pushers 1:1 per push job
    m.H_drv = pyo.Var(m.B, domain=pyo.NonNegativeIntegers)
    m.H_vehag = pyo.Var(m.B, domain=pyo.NonNegativeIntegers)
    m.H_push = pyo.Var(m.B, domain=pyo.NonNegativeIntegers)


    # -----------------------------
    # Objective (NO COSTS)
    # -----------------------------
    BIG = 1000.0  # prioritise minimising vehicles dispatched
    LAMBDA = 1e6   # SLA breaches are extremely undesirable

    def obj_rule(m):
        trips = BIG * sum(m.k[vc, fb] for vc in m.VC for fb in m.FB)

        soft = sum(
            PENALTY["AMB_HORIZONTAL"] * m.x[j, "Amb"] +
            PENALTY["PUSH"] * m.x[j, "Push"] +
            # transfer penalty: vertical required but horizontal moved by Mini
            PENALTY["TRANSFER"] * int(jobs.loc[j, "needs_vertical"]) * m.x[j, "Mini"]
            for j in m.J
        )

        
        sla_pen = LAMBDA * sum(m.y[j] for j in m.J)

        # Small regularizer so staff doesn't float arbitrarily high
        STAFF_W = 1.0
        staff_reg = STAFF_W * sum(m.H_drv[b] + m.H_vehag[b] + m.H_push[b] for b in m.B)

        return trips + soft + sla_pen + staff_reg



    m.OBJ = pyo.Objective(rule=obj_rule, sense=pyo.minimize)

    # -----------------------------
    # Core constraints
    # -----------------------------

    # One mode per job
    m.OneMode = pyo.Constraint(m.J, rule=lambda m, j: sum(m.x[j, mm] for mm in m.M) == 1)

    
    # Serve once (and only after release)
    # def serve_once(m, j):
    #     return sum(m.A[j, b] for b in m.B if b_to_idx[b] >= release_idx[j]) == 1
    
    def serve_once(m, j):
        return sum(m.A[j, b] for b in JB_by_j[j]) == 1

    m.ServeOnce = pyo.Constraint(m.J, rule=serve_once)

    # # No release before service
    # def no_service_before_release(m, j, b):
    #     if b_to_idx[b] < release_idx[j]
    #         return m.A[j, b] == 0
    #     return pyo.Constraint.Skip
    # m.NoServiceBeforeRelease = pyo.Constraint(m.J, m.B, rule=no_service_before_release)

    
    # SLA delay calculation uses scheduled s(j) as the reference clock
    # We compute delay in minutes relative to s(j).
    # delay_j = sum( (b - s_j) * A[j,b] )
    # but only for b in feasible buckets.
    def delay_expr(j):
        s_j = pd.to_datetime(jobs.loc[j, "s"]).floor(f"{BUCKET_MINUTES}min")
        s_i = to_bucket_idx(s_j)
        # convert each b to index and compute minutes difference
        return sum(
            (to_bucket_idx(b) - s_i) * BUCKET_MINUTES * m.A[j, b]
            for b in JB_by_j[j]
        )


    def late_def(m, j):
        return m.late_mins[j] >= delay_expr(j) - L_j[j]
    
    m.LateDef = pyo.Constraint(m.J, rule=late_def)

    # SLA breach indicator: delay <= SLA + M*y
    def sla_rule(m, j):
        return delay_expr(j) <= L_j[j] + M_BIG * m.y[j]
    m.SLA = pyo.Constraint(m.J, rule=sla_rule)

    # Hard cap: do not allow jobs to be served later than SLA + max_late
    # (This keeps the feasible window meaningful and bounded.)
    if max_late_mins > 0:
        def max_late_cap(m, j):
            return delay_expr(j) <= L_j[j] + max_late_mins
        m.MaxLateCap = pyo.Constraint(m.J, rule=max_late_cap)


    # # SLA breach definition: delay <= L_j + M*y
    # def sla_rule(m, j):
    #     delay = sum((b_to_idx[b] - release_idx[j]) * BUCKET_MINUTES * m.A[j, b] for b in m.B)
    #     return delay <= L_j[j] + M_BIG * m.y[j]
    # m.SLA = pyo.Constraint(m.J, rule=sla_rule)

    # Safety stands: disallow push unless Ryanair
    def safety_rule(m, j):
        if int(jobs.loc[j, "safety_stand"]) == 1 and str(jobs.loc[j, "Airline Code"]) not in RYANAIR_CODES:
            return m.x[j, "Push"] == 0
        return pyo.Constraint.Skip
    m.SafetyStand = pyo.Constraint(m.J, rule=safety_rule)


    # Domestic ARRIVALS restriction:
    def no_amb_horizontal_domestic_arrivals(m, j):
        # build_jobs provides:
        #   jobs["class"] in {"Dom","Int"} derived from Sector
        #   jobs["dir"] in {"A","D"}
        if str(jobs.loc[j, "class"]) == "Dom" and str(jobs.loc[j, "dir"]) == "A":
            return m.x[j, "Amb"] == 0
        return pyo.Constraint.Skip

    m.NoAmbDomesticArrivals = pyo.Constraint(m.J, rule=no_amb_horizontal_domestic_arrivals)


    
# -----------------------------
    # Helper: jobs per flight
    # -----------------------------
    jobs_by_f = {f: [] for f in flights}
    for j in jobs.index:
        jobs_by_f[jobs.loc[j, "flight_key"]].append(j)

    
    # Helper: safe access to A(j,b) (0 if (j,b) not feasible)
    JB_set = set(JB_list)

    def A_jb(j, b):
        if (j, b) in JB_set:
            return m.A[j, b]
        return 0.0


    # -----------------------------
    # Vehicle physical capacity (seat + wheelchair)
    # Uses horizontal choice x[j,m] AND service assignment A[j,b]
    # -----------------------------
    def mini_seat_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        #demand = sum(m.x[j, "Mini"] * m.A[j, b] for j in Jf)
        demand = sum(m.x[j, "Mini"] * A_jb(j, b) for j in Jf)
        supply = sum(
            m.seatcap[("Mini", cid)] * m.k[("Mini", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Mini"
        )
        return demand <= supply

    def mini_wc_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        #demand = sum(int(jobs.loc[j, "needs_wc"]) * m.x[j, "Mini"] * m.A[j, b] for j in Jf)
        demand = sum(int(jobs.loc[j, "needs_wc"]) * m.x[j, "Mini"] * A_jb(j, b) for j in Jf)
        supply = sum(
            m.wccap[("Mini", cid)] * m.k[("Mini", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Mini"
        )
        return demand <= supply

    def amb_seat_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        # Vertical component always requires an Ambulift slot when served
        #vertical = sum(int(jobs.loc[j, "needs_vertical"]) * m.A[j, b] for j in Jf)
        #horizontal_amb = sum(m.x[j, "Amb"] * m.A[j, b] for j in Jf)
        vertical = sum(int(jobs.loc[j, "needs_vertical"]) * A_jb(j, b) for j in Jf) 
        horizontal_amb = sum(m.x[j, "Amb"] * A_jb(j, b) for j in Jf)                 
        demand = vertical + horizontal_amb
        supply = sum(
            m.seatcap[("Amb", cid)] * m.k[("Amb", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Amb"
        )
        return demand <= supply

    def amb_wc_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        #vertical_wc = sum(int(jobs.loc[j, "needs_wc"]) * int(jobs.loc[j, "needs_vertical"]) * m.A[j, b] for j in Jf)
        #horizontal_wc = sum(int(jobs.loc[j, "needs_wc"]) * m.x[j, "Amb"] * m.A[j, b] for j in Jf)
        vertical_wc = sum(int(jobs.loc[j, "needs_wc"]) * int(jobs.loc[j, "needs_vertical"]) * A_jb(j, b) for j in Jf) 
        horizontal_wc = sum(int(jobs.loc[j, "needs_wc"]) * m.x[j, "Amb"] * A_jb(j, b) for j in Jf)                   
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

    # -----------------------------
    # Fleet exclusivity per bucket
    # each vehicle can only be assigned to one flight per service bucket
    # -----------------------------
    # def fleet_exclusive(m, vtype, cid, b):
    #     used = sum(m.k[(vtype, cid), (f, b)] for f in m.F)
    #     cap = int(m.count[(vtype, cid)])
    #     if vtype == "Mini":
    #         cap = cap - int(ferry_mini_reserved.get(b, 0))
    #     return used <= cap

    # m.FleetExclusive = pyo.Constraint(m.VC, m.B, rule=lambda m, vtype, cid, b: fleet_exclusive(m, vtype, cid, b))
    
    def fleet_exclusive(m, vtype, cid, b):
        used = sum(
            m.k[(vtype, cid), (f, b)]
            for f in m.F
            if (f, b) in m.FB        # <-- CHANGED: FILTER TO FEASIBLE PAIRS
        )
        cap = int(m.count[(vtype, cid)])
        if vtype == "Mini":
            cap = cap - int(ferry_mini_reserved.get(b, 0))
        return used <= cap

    m.FleetExclusive = pyo.Constraint(m.VC, m.B, rule=lambda m, vtype, cid, b: fleet_exclusive(m, vtype, cid, b))


    # # -----------------------------
    # # Time capacity per bucket (minutes)
    # # -----------------------------
    # def amb_time_cap(m, b):
    #     used = 0.0
    #     for j in m.J:
    #         base = float(tau[(j, "Amb")])

    #         # Ambulift horizontal if chosen
    #         used += base * m.x[j, "Amb"] * m.A[j, b]

    #         # Vertical ALWAYS consumes ambulift time when served
    #         if int(jobs.loc[j, "needs_vertical"]) == 1:
    #             used += base * m.A[j, b]
    #             # Handover overlap only if horizontal is Mini
    #             used += float(toggles.handover_mins) * m.x[j, "Mini"] * m.A[j, b]

    #     available = BUCKET_MINUTES * N_AMB - float(spin_removed.get(b, 0.0))
    #     return used <= available

    # def mini_time_cap(m, b):
    #     used = 0.0
    #     for j in m.J:
    #         base = float(tau[(j, "Mini")])
    #         used += base * m.x[j, "Mini"] * m.A[j, b]

    #         if int(jobs.loc[j, "needs_vertical"]) == 1:
    #             used += float(toggles.handover_mins) * m.x[j, "Mini"] * m.A[j, b]

    #     available = BUCKET_MINUTES * N_MINI
    #     return used <= available

    # m.AmbTimeCap = pyo.Constraint(m.B, rule=amb_time_cap)
    # m.MiniTimeCap = pyo.Constraint(m.B, rule=mini_time_cap)

    
    def amb_time_cap(m, b):
        used = 0.0
        for j in m.J:
            base = float(tau[(j, "Amb")])
            used += base * m.x[j, "Amb"] * A_jb(j, b)
            if int(jobs.loc[j, "needs_vertical"]) == 1:
                used += base * A_jb(j, b)
                used += float(toggles.handover_mins) * m.x[j, "Mini"] * A_jb(j, b)

        available = BUCKET_MINUTES * N_AMB - float(spin_removed.get(b, 0.0))
        return used <= available

    def mini_time_cap(m, b):
        used = 0.0
        for j in m.J:
            base = float(tau[(j, "Mini")])
            used += base * m.x[j, "Mini"] * A_jb(j, b)
            if int(jobs.loc[j, "needs_vertical"]) == 1:
                used += float(toggles.handover_mins) * m.x[j, "Mini"] * A_jb(j, b)
        available = BUCKET_MINUTES * N_MINI
        return used <= available

    m.AmbTimeCap = pyo.Constraint(m.B, rule=amb_time_cap)
    m.MiniTimeCap = pyo.Constraint(m.B, rule=mini_time_cap)


    # # -----------------------------
    # # Staff constraints per bucket
    # # -----------------------------
    # def amb_used(m, b):
    #     return sum(m.k[("Amb", cid), (f, b)] for (vt, cid) in m.VC if vt == "Amb" for f in m.F)

    # def mini_used(m, b):
    #     return sum(m.k[("Mini", cid), (f, b)] for (vt, cid) in m.VC if vt == "Mini" for f in m.F)

    # def drv_staff(m, b):
    #     required = amb_used(m, b) + mini_used(m, b) + int(ferry_drv_reserved.get(b, 0))
    #     return m.H_drv[b] >= required

    # def vehag_staff(m, b):
    #     required = amb_used(m, b) + mini_used(m, b)
    #     return m.H_vehag[b] >= required

    # def push_staff(m, b):
    #     required = sum(m.x[j, "Push"] * m.A[j, b] for j in m.J)
    #     return m.H_push[b] >= required

    # m.DriverStaff = pyo.Constraint(m.B, rule=drv_staff)
    # m.VehAgStaff = pyo.Constraint(m.B, rule=vehag_staff)
    # m.PushStaff = pyo.Constraint(m.B, rule=push_staff)

    
    # -----------------------------
    # Staff constraints per bucket
    # -----------------------------
    def amb_used(m, b):
        return sum(
            m.k[("Amb", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Amb"
            for f in m.F
            if (f, b) in m.FB
        )

    def mini_used(m, b):
        return sum(
            m.k[("Mini", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Mini"
            for f in m.F
            if (f, b) in m.FB
        )

    def drv_staff(m, b):
        required = amb_used(m, b) + mini_used(m, b) + int(ferry_drv_reserved.get(b, 0))
        return m.H_drv[b] >= required

    def vehag_staff(m, b):
        required = amb_used(m, b) + mini_used(m, b)
        return m.H_vehag[b] >= required

    def push_staff(m, b):
        required = sum(m.x[j, "Push"] * A_jb(j, b) for j in m.J)
        return m.H_push[b] >= required

    m.DriverStaff = pyo.Constraint(m.B, rule=drv_staff)
    m.VehAgStaff = pyo.Constraint(m.B, rule=vehag_staff)
    m.PushStaff = pyo.Constraint(m.B, rule=push_staff)


    # -----------------------------
    # Gate 7/8 lift bottleneck
    # bucketed by service bucket + lift_gate indicator
    # -----------------------------
    J_lift = [j for j in m.J if int(jobs.loc[j, "lift_gate"]) == 1 and int(jobs.loc[j, "needs_wc"]) == 1]
    
    def lift_cap(m, b):

        # If there are no such jobs at all, this constraint is always feasible
        if len(J_lift) == 0:
            return pyo.Constraint.Feasible

        # Otherwise, sum the wheelchair load served in bucket b
        #wch = pyo.quicksum(m.A[j, b] for j in J_lift)
        wch = pyo.quicksum(A_jb(j, b) for j in J_lift)

        return wch * float(LIFT_CYCLE_MINS) <= float(LIFT_CAPACITY_MINS)

    m.LiftCap = pyo.Constraint(m.B, rule=lift_cap)


    return m
