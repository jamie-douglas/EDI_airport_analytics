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

"""

import pyomo.environ as pyo
import pandas as pd

from .config import (
    RYANAIR_CODES, PENALTY,
    LIFT_CYCLE_MINS, LIFT_CAPACITY_MINS, VEHICLE_MODELS,
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

    
    # OPTIONAL placeholders for later (do nothing unless provided)
    ferry_mini_reserved = getattr(toggles, "ferry_mini_reserved", {}) or {}
    ferry_drv_reserved = getattr(toggles, "ferry_drv_reserved", {}) or {}

    max_late_mins = int(getattr(toggles, "max_late_mins", 0) or 0)


    
    # -----------------------------
    # Build SERVICE bucket timeline
    # -----------------------------
    # Due to t/s split:
    # - jobs["t"] can be earlier than jobs["s"] (preposition)
    # - model must still allow service at the normal "s" times
    # - model must allow delays up to SLA (or breaches)
    #
    # So we build a continuous bucket timeline from min(t) to max(s) + slack.
    t_min = pd.to_datetime(jobs["t"]).min()
    s_max = pd.to_datetime(jobs["s"]).max()
    max_sla = float(pd.to_numeric(jobs["sla_limit"]).max())
    # slack_buckets = int((max_sla // BUCKET_MINUTES) + 2)
    
    # extend horizon to allow late service beyond SLA when needed
    extra_mins = max_sla + float(max_late_mins) ###
    slack_buckets = int((extra_mins // BUCKET_MINUTES) + 2)###

    end_time = s_max + pd.to_timedelta(slack_buckets * BUCKET_MINUTES, unit="m")

    B_list = list(pd.date_range(start=t_min, end=end_time, freq=f"{BUCKET_MINUTES}min"))
    b_to_idx = {b: i for i, b in enumerate(B_list)}

    # -----------------------------
    # Sets
    # -----------------------------
    m.J = pyo.Set(initialize=list(jobs.index))
    m.B = pyo.Set(initialize=B_list, ordered=True)
    #Horizontal models
    m.M = pyo.Set(initialize=["Amb", "Mini", "Push"])

    # Flight-time indexing (no pooling across flights)
    flights = sorted(jobs["flight_key"].unique())
    m.F = pyo.Set(initialize=flights)

    # Vehicle assignment index: flight x service bucket
    fb_pairs = [(f, b) for f in flights for b in B_list]
    m.FB = pyo.Set(dimen=2, initialize=fb_pairs)

    
    # -----------------------------
    # SLA parameters
    # -----------------------------
    release_b = {j: pd.to_datetime(jobs.loc[j, "t"]) for j in jobs.index}
    release_idx = {j: b_to_idx[release_b[j]] for j in jobs.index}

    # Use the job-specific SLA limit already computed in build_jobs
    L_j = {j: float(jobs.loc[j, "sla_limit"]) for j in jobs.index}


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


    # -----------------------------
    # Decision variables
    # -----------------------------
    m.x = pyo.Var(m.J, m.M, domain=pyo.Binary)  # job horizontal mode
    m.A = pyo.Var(m.J, m.B, domain=pyo.Binary)   # service bucket assignment
    m.y = pyo.Var(m.J, domain=pyo.Binary)        # SLA breach indicator

    m.U = pyo.Var(m.J, m.B, m.M, domain=pyo.Binary)  ### NEW: U[j,b,m] = A[j,b] AND x[j,m]

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

        future_vehicle_keys = [
            (vtype, c["class_id"])
            for vtype, lst in classes.items()
            for c in lst
            if VEHICLE_MODELS.get(c["class_id"].replace("_C", "_"), {}).get("capex_hr", 0) > 0
        ]

        FUTURE_VEHICLE_PENALTY = sum(
            VEHICLE_MODELS.get(cid.replace("_C", "_"), {}).get("capex_hr", 0) *
            sum(m.k[(vtype, cid), fb] for fb in m.FB)
            for (vtype, cid) in future_vehicle_keys
        )

        return trips + soft + sla_pen + staff_reg + FUTURE_VEHICLE_PENALTY



    m.OBJ = pyo.Objective(rule=obj_rule, sense=pyo.minimize)

    # -----------------------------
    # Core constraints
    # -----------------------------

    # One mode per job
    m.OneMode = pyo.Constraint(m.J, rule=lambda m, j: sum(m.x[j, mm] for mm in m.M) == 1)

    
    ### -new block----------------------------
    ### LINEARISATION: U[j,b,m] = A[j,b] AND x[j,m]
    ### -----------------------------
    def u_le_a(m, j, b, mm):
        return m.U[j, b, mm] <= m.A[j, b]
    m.U_le_A = pyo.Constraint(m.J, m.B, m.M, rule=u_le_a)

    def u_le_x(m, j, b, mm):
        return m.U[j, b, mm] <= m.x[j, mm]
    m.U_le_X = pyo.Constraint(m.J, m.B, m.M, rule=u_le_x)

    def u_ge_and(m, j, b, mm):
        return m.U[j, b, mm] >= m.A[j, b] + m.x[j, mm] - 1
    m.U_ge_AND = pyo.Constraint(m.J, m.B, m.M, rule=u_ge_and)


    #--------------old-------------------------------------
    # Serve once (and only after release)
    def serve_once(m, j):
        return sum(m.A[j, b] for b in m.B if b_to_idx[b] >= release_idx[j]) == 1
    m.ServeOnce = pyo.Constraint(m.J, rule=serve_once)

    # No release before service
    def no_service_before_release(m, j, b):
        if b_to_idx[b] < release_idx[j]:
            return m.A[j, b] == 0
        return pyo.Constraint.Skip
    m.NoServiceBeforeRelease = pyo.Constraint(m.J, m.B, rule=no_service_before_release)

    # SLA breach definition: delay <= L_j + M*y
    def sla_rule(m, j):
        delay = sum((b_to_idx[b] - release_idx[j]) * BUCKET_MINUTES * m.A[j, b] for b in m.B)
        return delay <= L_j[j] + M_BIG * m.y[j]
    m.SLA = pyo.Constraint(m.J, rule=sla_rule)

    
    if max_late_mins >= 0:###
        def max_late_cap(m, j):###
            delay = sum((b_to_idx[b] - release_idx[j]) * BUCKET_MINUTES * m.A[j, b] for b in m.B)###
            return delay <= L_j[j] + max_late_mins###
        m.MaxLateCap = pyo.Constraint(m.J, rule=max_late_cap)###


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

    # -----------------------------
    # Vehicle physical capacity (seat + wheelchair)
    # Uses horizontal choice x[j,m] AND service assignment A[j,b]
    # -----------------------------
    def mini_seat_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        #demand = sum(m.x[j, "Mini"] * m.A[j, b] for j in Jf)
        demand = sum(m.U[j, b, "Mini"] for j in Jf) ###
        supply = sum(
            m.seatcap[("Mini", cid)] * m.k[("Mini", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Mini"
        )
        return demand <= supply

    def mini_wc_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        #demand = sum(int(jobs.loc[j, "needs_wc"]) * m.x[j, "Mini"] * m.A[j, b] for j in Jf)
        demand = sum(int(jobs.loc[j, "needs_wc"]) * m.U[j, b, "Mini"] for j in Jf) ###
        supply = sum(
            m.wccap[("Mini", cid)] * m.k[("Mini", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Mini"
        )
        return demand <= supply

    def amb_seat_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        # Vertical component always requires an Ambulift slot when served
        vertical = sum(int(jobs.loc[j, "needs_vertical"]) * m.A[j, b] for j in Jf)
        # horizontal_amb = sum(m.x[j, "Amb"] * m.A[j, b] for j in Jf)
        horizontal_amb = sum(m.U[j, b, "Amb"] for j in Jf) ###
        demand = vertical + horizontal_amb
        supply = sum(
            m.seatcap[("Amb", cid)] * m.k[("Amb", cid), (f, b)]
            for (vt, cid) in m.VC if vt == "Amb"
        )
        return demand <= supply

    def amb_wc_cap(m, f, b):
        Jf = jobs_by_f.get(f, [])
        vertical_wc = sum(int(jobs.loc[j, "needs_wc"]) * int(jobs.loc[j, "needs_vertical"]) * m.A[j, b] for j in Jf)
        #horizontal_wc = sum(int(jobs.loc[j, "needs_wc"]) * m.x[j, "Amb"] * m.A[j, b] for j in Jf)
        horizontal_wc = sum(int(jobs.loc[j, "needs_wc"]) * m.U[j, b, "Amb"] for j in Jf) ###
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
    def fleet_exclusive(m, vtype, cid, b):
        used = sum(m.k[(vtype, cid), (f, b)] for f in m.F)
        cap = int(m.count[(vtype, cid)])
        if vtype == "Mini":
            cap = cap - int(ferry_mini_reserved.get(b, 0))
        return used <= cap

    m.FleetExclusive = pyo.Constraint(m.VC, m.B, rule=lambda m, vtype, cid, b: fleet_exclusive(m, vtype, cid, b))

    # -----------------------------
    # Time capacity per bucket (minutes)
    # This is the part you liked — kept and made consistent with A[j,b]
    # -----------------------------
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

    
    def amb_time_cap(m, b):###
        used = 0.0###
        for j in m.J:###
            base = float(tau[(j, "Amb")])###

            # Ambulift horizontal if chosen AND served in bucket b
            # (replaces: m.x[j,"Amb"] * m.A[j,b])
            used += base * m.U[j, b, "Amb"]###

            # Vertical ALWAYS consumes ambulift time when served (this stays linear)
            if int(jobs.loc[j, "needs_vertical"]) == 1:###
                used += base * m.A[j, b]###

                # Handover overlap only if horizontal is Mini AND served in bucket b
                # (replaces: m.x[j,"Mini"] * m.A[j,b])
                used += float(toggles.handover_mins) * m.U[j, b, "Mini"]###

        available = BUCKET_MINUTES * N_AMB - float(spin_removed.get(b, 0.0))###
        return used <= available###


    # def mini_time_cap(m, b):
    #     used = 0.0
    #     for j in m.J:
    #         base = float(tau[(j, "Mini")])
    #         used += base * m.x[j, "Mini"] * m.A[j, b]

    #         if int(jobs.loc[j, "needs_vertical"]) == 1:
    #             used += float(toggles.handover_mins) * m.x[j, "Mini"] * m.A[j, b]

    #     available = BUCKET_MINUTES * N_MINI
    #     return used <= available

    
    
    def mini_time_cap(m, b): ###
        used = 0.0###
        for j in m.J:###
            base = float(tau[(j, "Mini")])###

            # Minibus horizontal if chosen AND served in bucket b
            # (replaces: m.x[j,"Mini"] * m.A[j,b])
            used += base * m.U[j, b, "Mini"]###

            # If vertical is needed and horizontal is Mini, include handover overlap
            # (replaces: m.x[j,"Mini"] * m.A[j,b])
            if int(jobs.loc[j, "needs_vertical"]) == 1:###
                used += float(toggles.handover_mins) * m.U[j, b, "Mini"]###

        available = BUCKET_MINUTES * N_MINI###
        return used <= available###
    
    m.AmbTimeCap = pyo.Constraint(m.B, rule=amb_time_cap)
    m.MiniTimeCap = pyo.Constraint(m.B, rule=mini_time_cap)


    # -----------------------------
    # Staff constraints per bucket
    # -----------------------------
    def amb_used(m, b):
        return sum(m.k[("Amb", cid), (f, b)] for (vt, cid) in m.VC if vt == "Amb" for f in m.F)

    def mini_used(m, b):
        return sum(m.k[("Mini", cid), (f, b)] for (vt, cid) in m.VC if vt == "Mini" for f in m.F)

    def drv_staff(m, b):
        required = amb_used(m, b) + mini_used(m, b) + int(ferry_drv_reserved.get(b, 0))
        return m.H_drv[b] >= required

    def vehag_staff(m, b):
        required = amb_used(m, b) + mini_used(m, b)
        return m.H_vehag[b] >= required

    # def push_staff(m, b):
    #     required = sum(m.x[j, "Push"] * m.A[j, b] for j in m.J)
    #     return m.H_push[b] >= required
    
    def push_staff(m, b):###
        # Pushers are required 1:1 for push jobs served in bucket b
        # (replaces: m.x[j,"Push"] * m.A[j,b])
        required = sum(m.U[j, b, "Push"] for j in m.J)###
        return m.H_push[b] >= required###


    m.DriverStaff = pyo.Constraint(m.B, rule=drv_staff)
    m.VehAgStaff = pyo.Constraint(m.B, rule=vehag_staff)
    m.PushStaff = pyo.Constraint(m.B, rule=push_staff)

        # -----------------------------
    # Gate 7/8 lift bottleneck
    # bucketed by service bucket + lift_gate indicator
    # -----------------------------

    # PRECOMPUTE ONLY THE JOBS THAT CAN EVER USE THE LIFT
    J_lift = [j for j in m.J if int(jobs.loc[j, "lift_gate"]) == 1 and int(jobs.loc[j, "needs_wc"]) == 1]

    def lift_cap(m, b):
        # If there are no lift jobs at all, constraint is always feasible
        if len(J_lift) == 0:
            return pyo.Constraint.Feasible

        # This sum contains Pyomo vars (m.A[j,b]) so it will never simplify to a Python bool
        wch = pyo.quicksum(m.A[j, b] for j in J_lift)

        return wch * float(LIFT_CYCLE_MINS) <= float(LIFT_CAPACITY_MINS)

    m.LiftCap = pyo.Constraint(m.B, rule=lift_cap)

    # --- 80% ultilisation constraint to prevent over-reliance on future vehicles (can be relaxed if needed) ---
    CURRENT_UTIL_THRESHOLD = 0.8

    for vtype, lst in classes.items():
        current_cids = [c["class_id"] for c in lst if VEHICLE_MODELS.get(c["class_id"].replace("_C", "_"), {}).get("capex_hr", 0) == 0]
        future_cids = [c["class_id"] for c in lst if VEHICLE_MODELS.get(c["class_id"].replace("_C", "_"), {}).get("capex_hr", 0) > 0]
        for b in m.B:
            def future_vehicle_constraint(m, vtype=vtype, current_cids=current_cids, future_cids=future_cids, b=b):
                current_used = sum(
                    m.k[(vtype, cid), (f, b)]
                    for cid in current_cids for f in m.F if (f, b) in m.FB
                )
                future_used = sum(
                    m.k[(vtype, cid), (f, b)]
                    for cid in future_cids for f in m.F if (f, b) in m.FB
                )
                total_current = sum(
                    m.count[(vtype, cid)]
                    for cid in current_cids
                )
                return future_used <= 1e6 * (current_used >= CURRENT_UTIL_THRESHOLD * total_current)
            setattr(m, f"FutureVehicleConstraint_{vtype}_{str(b)}", pyo.Constraint(rule=future_vehicle_constraint))

    return m