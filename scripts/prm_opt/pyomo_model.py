# scripts/prm_opt/pyomo_model.py

"""
Scenario 2 optimisation (no pooling across flights).

Implements Model Scope components:
- job-level horizontal mode decisions x[j,m]  
- vehicle time availability per bucket (fleet)  
- spin locking removes ambulift minutes per bucket  
- vehicle physical capacity (wheelchair slot capacity)  
- Gate 7/8 lift bottleneck  

We also include:
- heterogeneous fleet capacities via vehicle classes
- per-flight per-bucket vehicle assignment counts (k) to prevent over/under allocation
- objective: minimise vehicles dispatched + soft penalties (NO COSTS)
"""

import pyomo.environ as pyo

from .config import (
    SAFETY_STANDS, RYANAIR_CODES, PENALTY,
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
    jobs: DataFrame from build_jobs (must include flight_key, t, needs_wc, needs_vertical, safety_stand, lift_gate)
    tau: dict[(j, mode)->minutes] for modes: Amb, Mini, Push
    spin_removed: dict[t -> minutes] removed from ambulift capacity
    """

    m = pyo.ConcreteModel()

    # -----------------------------
    # Sets
    # -----------------------------
    m.J = pyo.Set(initialize=list(jobs.index))
    m.T = pyo.Set(initialize=sorted(jobs["t"].unique()))
    m.M = pyo.Set(initialize=["Amb", "Mini", "Push"])

    # Flight-time indexing (no pooling across flights)
    flights = sorted(jobs["flight_key"].unique())
    m.F = pyo.Set(initialize=flights)
    ft_pairs = sorted(set(zip(jobs["flight_key"], jobs["t"])))
    m.FT = pyo.Set(dimen=2, initialize=ft_pairs)

    # Vehicle classes (heterogeneous fleet)
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

    # -----------------------------
    # Decision variables
    # -----------------------------
    m.x = pyo.Var(m.J, m.M, domain=pyo.Binary)  # job horizontal mode

    # number of vehicles of each class assigned to each (flight, bucket)
    m.k = pyo.Var(m.VC, m.FT, domain=pyo.NonNegativeIntegers)

    # -----------------------------
    # Objective (NO COSTS)
    # -----------------------------
    BIG = 1000.0  # prioritise minimising vehicles dispatched

    def obj_rule(m):
        trips = BIG * sum(m.k[vc, ft] for vc in m.VC for ft in m.FT)

        soft = sum(
            PENALTY["AMB_HORIZONTAL"] * m.x[j, "Amb"] +
            PENALTY["PUSH"] * m.x[j, "Push"] +
            # transfer penalty: vertical required but horizontal moved by Mini
            PENALTY["TRANSFER"] * int(jobs.loc[j, "needs_vertical"]) * m.x[j, "Mini"]
            for j in m.J
        )
        return trips + soft

    m.OBJ = pyo.Objective(rule=obj_rule, sense=pyo.minimize)

    # -----------------------------
    # Core constraints
    # -----------------------------

    # One mode per job
    m.OneMode = pyo.Constraint(m.J, rule=lambda m, j: sum(m.x[j, mm] for mm in m.M) == 1)

    # Vertical jobs cannot be push
    def no_push_vertical(m, j):
        if int(jobs.loc[j, "needs_vertical"]) == 1:
            return m.x[j, "Push"] == 0
        return pyo.Constraint.Skip
    m.NoPushVertical = pyo.Constraint(m.J, rule=no_push_vertical)

    # Safety stands: disallow push unless Ryanair
    def safety_rule(m, j):
        if int(jobs.loc[j, "safety_stand"]) == 1 and str(jobs.loc[j, "Airline Code"]) not in RYANAIR_CODES:
            return m.x[j, "Push"] == 0
        return pyo.Constraint.Skip
    m.SafetyStand = pyo.Constraint(m.J, rule=safety_rule)

    
    # --------------------------------------------------
    # Domestic ARRIVALS restriction (as requested):
    # Ambulift cannot be used as the HORIZONTAL mode for DOMESTIC ARRIVALS.
    # This only restricts x[j,"Amb"] when class=="Dom" and dir=="A".
    # It does NOT remove or alter the vertical component logic.
    # --------------------------------------------------
    def no_amb_horizontal_domestic_arrivals(m, j):
        # build_jobs provides:
        #   jobs["class"] in {"Dom","Int"} derived from Sector
        #   jobs["dir"] in {"A","D"}
        if str(jobs.loc[j, "class"]) == "Dom" and str(jobs.loc[j, "dir"]) == "A":
            return m.x[j, "Amb"] == 0
        return pyo.Constraint.Skip

    m.NoAmbDomesticArrivals = pyo.Constraint(m.J, rule=no_amb_horizontal_domestic_arrivals)


    # Index jobs by (flight, t)
    jobs_by_ft = {ft: [] for ft in ft_pairs}
    for j in jobs.index:
        ft = (jobs.loc[j, "flight_key"], jobs.loc[j, "t"])
        jobs_by_ft[ft].append(j)

    # -----------------------------
    # Vehicle physical capacity (seat + wheelchair)
    # -----------------------------
    # Demand for Mini is jobs assigned to Mini.
    # Demand for Amb includes:
    #   - vertical requirement (ambulift needed even if horizontal is Mini)
    #   - plus any jobs assigned Amb for horizontal.

    def mini_seat_cap(m, f, t):
        Jft = jobs_by_ft.get((f, t), [])
        demand = sum(m.x[j, "Mini"] for j in Jft)
        supply = sum(
            m.seatcap[("Mini", cid)] * m.k[("Mini", cid), (f, t)]
            for (vt, cid) in m.VC if vt == "Mini"
        )
        return demand <= supply

    def mini_wc_cap(m, f, t):
        Jft = jobs_by_ft.get((f, t), [])
        demand = sum(int(jobs.loc[j, "needs_wc"]) * m.x[j, "Mini"] for j in Jft)
        supply = sum(
            m.wccap[("Mini", cid)] * m.k[("Mini", cid), (f, t)]
            for (vt, cid) in m.VC if vt == "Mini"
        )
        return demand <= supply

    def amb_seat_cap(m, f, t):
        Jft = jobs_by_ft.get((f, t), [])
        vertical_count = sum(int(jobs.loc[j, "needs_vertical"]) for j in Jft)
        horizontal_amb = sum(m.x[j, "Amb"] for j in Jft)
        demand = vertical_count + horizontal_amb
        supply = sum(
            m.seatcap[("Amb", cid)] * m.k[("Amb", cid), (f, t)]
            for (vt, cid) in m.VC if vt == "Amb"
        )
        return demand <= supply

    def amb_wc_cap(m, f, t):
        Jft = jobs_by_ft.get((f, t), [])
        vertical_wc = sum(int(jobs.loc[j, "needs_wc"]) * int(jobs.loc[j, "needs_vertical"]) for j in Jft)
        horizontal_wc = sum(int(jobs.loc[j, "needs_wc"]) * m.x[j, "Amb"] for j in Jft)
        demand = vertical_wc + horizontal_wc
        supply = sum(
            m.wccap[("Amb", cid)] * m.k[("Amb", cid), (f, t)]
            for (vt, cid) in m.VC if vt == "Amb"
        )
        return demand <= supply

    m.MiniSeatCap = pyo.Constraint(m.FT, rule=lambda m, f, t: mini_seat_cap(m, f, t))
    m.MiniWcCap = pyo.Constraint(m.FT, rule=lambda m, f, t: mini_wc_cap(m, f, t))
    m.AmbSeatCap = pyo.Constraint(m.FT, rule=lambda m, f, t: amb_seat_cap(m, f, t))
    m.AmbWcCap = pyo.Constraint(m.FT, rule=lambda m, f, t: amb_wc_cap(m, f, t))

    # Fleet exclusivity per bucket: each vehicle can only be assigned to one flight in that bucket
    def fleet_exclusive(m, vtype, cid, t):
        return sum(
            m.k[(vtype, cid), (f, t)]
            for (f, tt) in m.FT if tt == t
        ) <= m.count[(vtype, cid)]
    m.FleetExclusive = pyo.Constraint(m.VC, m.T, rule=lambda m, vtype, cid, t: fleet_exclusive(m, vtype, cid, t))

    # -----------------------------
    # Time capacity per bucket (minutes)
    # Includes spin lock and handover overlap when transfer occurs
    # -----------------------------
    BUCKET_MINUTES = 15

    N_AMB = sum(int(m.count[(vt, cid)]) for (vt, cid) in vclasses if vt == "Amb")
    N_MINI = sum(int(m.count[(vt, cid)]) for (vt, cid) in vclasses if vt == "Mini")

    def amb_time_cap(m, t):
        used = 0.0
        for j in m.J:
            if jobs.loc[j, "t"] != t:
                continue
            base = float(tau[(j, "Amb")])
            used += base * m.x[j, "Amb"]  # ambulift horizontal use
            if int(jobs.loc[j, "needs_vertical"]) == 1:
                used += base  # vertical component tie-up proxy
                used += toggles.handover_mins * m.x[j, "Mini"]  # transfer overlap
        available = BUCKET_MINUTES * N_AMB - float(spin_removed.get(t, 0.0))
        return used <= available

    def mini_time_cap(m, t):
        used = 0.0
        for j in m.J:
            if jobs.loc[j, "t"] != t:
                continue
            base = float(tau[(j, "Mini")])
            used += base * m.x[j, "Mini"]
            if int(jobs.loc[j, "needs_vertical"]) == 1:
                used += toggles.handover_mins * m.x[j, "Mini"]
        available = BUCKET_MINUTES * N_MINI

        # DEFERRED PLACEHOLDER:
        # available -= ferry_removed.get(t, 0.0)

        return used <= available

    m.AmbTimeCap = pyo.Constraint(m.T, rule=amb_time_cap)
    m.MiniTimeCap = pyo.Constraint(m.T, rule=mini_time_cap)

    # -----------------------------
    # Gate 7/8 lift bottleneck
    # Conservative bucket constraint using lift_gate indicator
    # -----------------------------
    def lift_cap(m, t):
        wch = 0.0
        for j in m.J:
            if jobs.loc[j, "t"] != t:
                continue
            if int(jobs.loc[j, "lift_gate"]) == 1:
                # If horizontal mini is used for a WCH passenger, this represents "minibus strategy"
                # For now we conservatively count total WCH load against lift capacity.
                wch += float(int(jobs.loc[j, "needs_wc"]))
        return wch * LIFT_CYCLE_MINS <= LIFT_CAPACITY_MINS

    m.LiftCap = pyo.Constraint(m.T, rule=lift_cap)

    # =========================================================
    # DEFERRED PLACEHOLDERS (COMMENTS ONLY)
    # =========================================================
    # - Staff constraints (Drivers / Vehicle Agents / Pushers) per Model Scope
    # - Break windows
    # - ω scenarios and SLA response-time constraints

    return m
