# scripts/prm_opt/optimise_prm_fleet_v2.py

from __future__ import annotations

import math
import time

from dataclasses import dataclass
from itertools import combinations
from typing import Dict, List, Tuple, Any

import numpy as np
import pandas as pd

import pyomo.environ as pyo

from modules.utils.progress import step

"""
Pyomo + HiGHS PRM fleet optimisation model V2.

Core design
-----------
- Flight-level model.
- No time buckets.
- Fixed service windows.
- Timeline overlap constraints:
    if a vehicle is occupied from t_start to t_end, it cannot be assigned
    to another overlapping interval.
- Physical vehicle assignment.
- Existing ambulifts are fixed and cannot be purchased.
- Existing minibuses are fixed.
- Future minibuses can be purchased if required.
- P100/P90 demand shaping happens upstream in build_flights_v2.py.

Main decisions
--------------
x[f,m]              = chosen service mode for flight f
amb[f,r,m]          = ambulift r assigned to flight f in vertical mode m
mini[f,r,m]         = minibus r assigned to flight f in minibus mode m
use[r]              = current/future vehicle used at least once
buy[r]              = future minibus purchased
N_staff             = peak staff requirement over event timeline
"""


# ---------------------------------------------------------------------
# Default durations / config
# ---------------------------------------------------------------------

@dataclass
class OptimiserConfig:
    tau_amb_solo_mins: float = 19.0
    tau_amb_comb_mins: float = 14.0
    tau_mini_mins: float = 14.0
    tau_push_mins: float = 15.0
    handover_buffer_mins: float = 5.0
    arrival_sla_target_pct: float = 0.98
    arrival_sla_target_mins: int = 20
    boarding_offset_mins: int = 40

    staff_amb: int = 2
    staff_mini: int = 2
    staff_per_prm_push: float = 1.0

    max_future_copies_per_model: int = 5

    # Objective weights.
    penalty_shortfall: float = 1_000_000.0
    penalty_future_buy: float = 100_000.0
    penalty_current_use: float = 1.0
    penalty_staff_minues: float = 1.0

    solver_name_preferred: str = "appsi_highs"
    show_solver_log: bool = True


# ---------------------------------------------------------------------
# Vehicle expansion
# ---------------------------------------------------------------------

def expand_vehicle_models(
    vehicle_models: Dict[str, Dict[str, Any]],
    max_future_copies_per_model: int = 5,
) -> pd.DataFrame:
    """
    Convert VEHICLE_MODELS dictionary into one row per physical/candidate vehicle.

    Existing vehicles stay as-is.
    Future vehicles are expanded into candidate copies:
        MB_EV_10__FUT_01
        MB_EV_10__FUT_02
        etc.
    """

    rows = []

    for vehicle_id, spec in vehicle_models.items():
        is_future = bool(spec.get("is_future", False))

        if not is_future:
            rows.append(
                {
                    "vehicle_id": vehicle_id,
                    "base_model": vehicle_id,
                    "type": spec["type"],
                    "seatcap": int(spec["seatcap"]),
                    "wccap": int(spec["wccap"]),
                    "staff": int(spec.get("staff", 0)),
                    "capex_hr": float(spec.get("capex_hr", 0.0)),
                    "is_future": 0,
                }
            )
        else:
            for i in range(1, int(max_future_copies_per_model) + 1):
                rows.append(
                    {
                        "vehicle_id": f"{vehicle_id}__FUT_{i:02d}",
                        "base_model": vehicle_id,
                        "type": spec["type"],
                        "seatcap": int(spec["seatcap"]),
                        "wccap": int(spec["wccap"]),
                        "staff": int(spec.get("staff", 0)),
                        "capex_hr": float(spec.get("capex_hr", 0.0)),
                        "is_future": 1,
                    }
                )

    df = pd.DataFrame(rows)

    if len(df) == 0:
        raise ValueError("No vehicles supplied.")

    if not set(df["type"].unique()).issubset({"Amb", "Mini"}):
        raise ValueError("Vehicle type must be 'Amb' or 'Mini'.")

    # Do not allow future ambulifts in V2.
    future_amb = df[(df["is_future"] == 1) & (df["type"] == "Amb")]
    if len(future_amb) > 0:
        raise ValueError("V2 does not allow future ambulift purchases.")

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------

def _minutes(ts: pd.Timestamp, origin: pd.Timestamp) -> float:
    return float((pd.to_datetime(ts) - origin).total_seconds() / 60.0)


def _ceil_div(a: int | float, b: int | float) -> int:
    a = float(a)
    b = float(b)
    if b <= 0:
        return 10**9
    return int(math.ceil(a / b))


def _intervals_overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> bool:
    """
    Half-open interval overlap:
        [start, end)
    Touching endpoints are allowed:
        end == start means no overlap.
    """
    return (a_start < b_end) and (b_start < a_end)


# ---------------------------------------------------------------------
# Duration and interval preparation
# ---------------------------------------------------------------------

def prepare_model_data(
    flights: pd.DataFrame,
    vehicle_models: Dict[str, Dict[str, Any]],
    config: OptimiserConfig,
) -> Dict[str, Any]:
    """
    Prepare all Pyomo sets, parameters, interval options and overlap pairs.
    """

    if flights is None or len(flights) == 0:
        raise ValueError("flights dataframe is empty.")

    fdf = flights.copy()

    t0 = time.perf_counter()
    t = t0

    t = step(
        t,
        f"prepare_model_data started | flights={len(flights):,}"
    )

    required_cols = [
        "flight_key",
        "arr_dep",
        "scheduled_time",
        "service_anchor_time",
        "P_total",
        "D_WCHC",
        "D_WCHS",
        "D_wc",
        "D_seat",
        "Remote",
        "EffRemote",
        "NeedVertical",
        "Domestic",
        "Safety",
    ]

    missing = [c for c in required_cols if c not in fdf.columns]
    if missing:
        raise ValueError(f"flights dataframe missing columns: {missing}")

    fdf["scheduled_time"] = pd.to_datetime(fdf["scheduled_time"])
    fdf["service_anchor_time"] = pd.to_datetime(fdf["service_anchor_time"])

    # Stable sorted order.
    fdf = fdf.sort_values(["service_anchor_time", "flight_key"]).reset_index(drop=True)

    origin = fdf["service_anchor_time"].min().floor("D")

    fdf["anchor_min"] = fdf["service_anchor_time"].apply(lambda x: _minutes(x, origin))

    vehicles = expand_vehicle_models(
        vehicle_models,
        max_future_copies_per_model=config.max_future_copies_per_model,
    )

    t = step(
        t,
        f"vehicle expansion complete | vehicles={len(vehicles):,}"
    )

    flights_list = fdf["flight_key"].tolist()
    modes = ["SA", "CM", "CP", "SM", "SP"]

    vertical_modes = ["SA", "CM", "CP"]
    minibus_modes = ["CM", "SM"]
    pusher_modes = ["CP", "SP"]

    amb_vehicles = vehicles.loc[vehicles["type"] == "Amb", "vehicle_id"].tolist()
    mini_vehicles = vehicles.loc[vehicles["type"] == "Mini", "vehicle_id"].tolist()

    vehicle_type = vehicles.set_index("vehicle_id")["type"].to_dict()
    vehicle_seatcap = vehicles.set_index("vehicle_id")["seatcap"].to_dict()
    vehicle_wccap = vehicles.set_index("vehicle_id")["wccap"].to_dict()
    vehicle_capex = vehicles.set_index("vehicle_id")["capex_hr"].to_dict()
    vehicle_is_future = vehicles.set_index("vehicle_id")["is_future"].to_dict()
    vehicle_base_model = vehicles.set_index("vehicle_id")["base_model"].to_dict()

    flight_params = fdf.set_index("flight_key").to_dict("index")

    # Demand parameters.
    D_seat = {f: int(flight_params[f]["D_seat"]) for f in flights_list}
    D_wc = {f: int(flight_params[f]["D_wc"]) for f in flights_list}
    P_total = {f: int(flight_params[f]["P_total"]) for f in flights_list}

    NeedVertical = {f: int(flight_params[f]["NeedVertical"]) for f in flights_list}
    Remote = {f: int(flight_params[f]["Remote"]) for f in flights_list}
    Arrival = {f: 1 if str(flight_params[f]["arr_dep"]) == "A" else 0 for f in flights_list}
    Domestic = {f: int(flight_params[f]["Domestic"]) for f in flights_list}
    Safety = {f: int(flight_params[f]["Safety"]) for f in flights_list}
    arrival_sla_target = {f: config.arrival_sla_target_mins for f in flights_list}

    # Precompute ambulift trips and assignment intervals.
    amb_trips = {}
    amb_duration = {}
    amb_interval = {}

    for f in flights_list:
        anchor = float(flight_params[f]["anchor_min"])
        is_arr = Arrival[f] == 1

        for r in amb_vehicles:
            trips = max(
                _ceil_div(D_seat[f], vehicle_seatcap[r]),
                _ceil_div(D_wc[f], vehicle_wccap[r]),
            )

            # If no demand, still at least 1 technical trip if assigned.
            trips = max(1, trips)

            amb_trips[(f, r)] = trips

            for m in vertical_modes:
                if m == "SA":
                    dur = trips * config.tau_amb_solo_mins
                else:
                    dur = trips * config.tau_amb_comb_mins

                amb_duration[(f, r, m)] = float(dur)

                if is_arr:
                    start = anchor
                    end = anchor + dur
                else:
                    end = anchor
                    start = anchor - dur

                amb_interval[(f, r, m)] = (float(start), float(end))

    t = step(
        t,
        (
            f"ambulift intervals built | "
            f"flights={len(flights_list):,} "
            f"ambulifts={len(amb_vehicles):,}"
        )
    )

    # For minibus intervals we deliberately avoid time buckets.
    # CM minibus timing is conservative: it uses the longest possible
    # ambulift-combined duration for the flight, so the minibus will not be
    # under-reserved if a slower/smaller ambulift is selected.
    mini_interval = {}
    mode_duration_for_staff = {}

    for f in flights_list:
        anchor = float(flight_params[f]["anchor_min"])
        is_arr = Arrival[f] == 1

        max_comb_amb_dur = 0.0
        if amb_vehicles:
            max_comb_amb_dur = max(
                amb_duration[(f, r, "CM")]
                for r in amb_vehicles
            )

        # SA staff/full interval uses max solo ambulift duration.
        max_solo_amb_dur = 0.0
        if amb_vehicles:
            max_solo_amb_dur = max(
                amb_duration[(f, r, "SA")]
                for r in amb_vehicles
            )

        mode_duration_for_staff[(f, "SA")] = max_solo_amb_dur
        mode_duration_for_staff[(f, "CM")] = (
            max_comb_amb_dur
            + config.handover_buffer_mins
            + config.tau_mini_mins
        )
        mode_duration_for_staff[(f, "CP")] = (
            max_comb_amb_dur
            + config.handover_buffer_mins
            + config.tau_push_mins
        )
        mode_duration_for_staff[(f, "SM")] = config.tau_mini_mins
        mode_duration_for_staff[(f, "SP")] = config.tau_push_mins

        for m in minibus_modes:
            if m == "SM":
                dur = config.tau_mini_mins

                if is_arr:
                    start = anchor
                    end = anchor + dur
                else:
                    end = anchor
                    start = anchor - dur

            elif m == "CM":
                # Whole minibus occupied interval:
                # around handover and minibus transit.
                mini_dur = config.handover_buffer_mins + config.tau_mini_mins

                if is_arr:
                    amb_finish = anchor + max_comb_amb_dur
                    start = amb_finish - config.handover_buffer_mins
                    end = amb_finish + config.tau_mini_mins
                else:
                    # For departure, service finishes at boarding target.
                    # Conservative minibus interval is the final buffer+mini block.
                    end = anchor
                    start = anchor - mini_dur

            mini_interval[(f, m)] = (float(start), float(end))

    t = step(
        t,
        (
            f"minibus intervals built | "
            f"minibuses={len(mini_vehicles):,}"
        )
    )

    # Staff intervals use whole flight service interval by mode.
    staff_interval = {}

    for f in flights_list:
        anchor = float(flight_params[f]["anchor_min"])
        is_arr = Arrival[f] == 1

        for m in modes:
            dur = float(mode_duration_for_staff[(f, m)])

            if is_arr:
                start = anchor
                end = anchor + dur
            else:
                end = anchor
                start = anchor - dur

            staff_interval[(f, m)] = (float(start), float(end))
    
    t = step(
        t,
        "staff intervals built"
    )

    # Overlap pairs.
    amb_overlap_pairs = []
    for r in amb_vehicles:
        options = [(f, r, m, amb_interval[(f, r, m)]) for f in flights_list for m in vertical_modes]
        for (f1, r1, m1, int1), (f2, r2, m2, int2) in combinations(options, 2):
            if f1 == f2:
                continue
            if _intervals_overlap(int1[0], int1[1], int2[0], int2[1]):
                amb_overlap_pairs.append((f1, r, m1, f2, r, m2))

    t = step(
        t,
        f"ambulift overlaps built | overlaps={len(amb_overlap_pairs):,}"
    )

    mini_overlap_pairs = []
    for r in mini_vehicles:
        options = [(f, r, m, mini_interval[(f, m)]) for f in flights_list for m in minibus_modes]
        for (f1, r1, m1, int1), (f2, r2, m2, int2) in combinations(options, 2):
            if f1 == f2:
                continue
            if _intervals_overlap(int1[0], int1[1], int2[0], int2[1]):
                mini_overlap_pairs.append((f1, r, m1, f2, r, m2))

    t = step(
        t,
        f"minibus overlaps built | overlaps={len(mini_overlap_pairs):,}"
    )

    # --------------------------------------------------
    # Staff event matrix deliberately NOT built here
    # --------------------------------------------------
    # We do not build staff_active[(event, flight, mode)] because for month-scale
    # runs this becomes too large and causes MemoryError.
    #
    # Staff still affects optimisation through staff_minutes[(f, m)] in the
    # objective. Peak staff is calculated after solve from the chosen solution.

    event_points = []
    staff_active = {}

    t = step(
        t,
        "staff event matrix skipped; staff cost will use mode-level staff minutes"
    )


    # Staff by mode.
    staff_req = {}
    for f in flights_list:
        staff_req[(f, "SA")] = config.staff_amb
        staff_req[(f, "SM")] = config.staff_mini
        staff_req[(f, "CM")] = config.staff_amb + config.staff_mini
        staff_req[(f, "CP")] = config.staff_amb + config.staff_per_prm_push * P_total[f]
        staff_req[(f, "SP")] = config.staff_per_prm_push * P_total[f]

    # --------------------------------------------------
    # Staff minutes by flight and mode
    # --------------------------------------------------
    # This is the staff cost used in the objective.
    #
    # Example:
    #   8 PRMs by solo pusher:
    #       staff = 8
    #       duration = tau_push
    #
    #   8 PRMs by solo minibus:
    #       staff = 2
    #       duration = tau_mini
    #
    # This is what makes the optimiser prefer a minibus for high-volume PRM
    # flights where appropriate.

    staff_minutes = {}

    for f in flights_list:
        for m in modes:
            duration = float(mode_duration_for_staff[(f, m)])
            staff_minutes[(f, m)] = float(staff_req[(f, m)]) * duration

    t = step(
        t,
        "staff requirements and staff-minutes calculated"
    )

    t = step(
        t,
        (
            "prepare_model_data complete | "
            f"total={(time.perf_counter() - t0):.2f}s"
        )
    )

    return {
        "origin": origin,
        "flights_df": fdf,
        "vehicles_df": vehicles,
        "F": flights_list,
        "M": modes,
        "VERTICAL_MODES": vertical_modes,
        "MINIBUS_MODES": minibus_modes,
        "PUSHER_MODES": pusher_modes,
        "R_AMB": amb_vehicles,
        "R_MINI": mini_vehicles,
        "R": vehicles["vehicle_id"].tolist(),
        "vehicle_type": vehicle_type,
        "vehicle_seatcap": vehicle_seatcap,
        "vehicle_wccap": vehicle_wccap,
        "vehicle_capex": vehicle_capex,
        "vehicle_is_future": vehicle_is_future,
        "vehicle_base_model": vehicle_base_model,
        "D_seat": D_seat,
        "D_wc": D_wc,
        "P_total": P_total,
        "NeedVertical": NeedVertical,
        "Remote": Remote,
        "Arrival": Arrival,
        "Domestic": Domestic,
        "Safety": Safety,
        "arrival_sla_target": arrival_sla_target,
        "amb_trips": amb_trips,
        "amb_interval": amb_interval,
        "mini_interval": mini_interval,
        "staff_interval": staff_interval,
        "amb_overlap_pairs": amb_overlap_pairs,
        "mini_overlap_pairs": mini_overlap_pairs,
        "event_points": event_points,
        "staff_active": staff_active,
        "staff_req": staff_req,
        "staff_minutes": staff_minutes,
        "config": config,
    }


# ---------------------------------------------------------------------
# Build Pyomo model
# ---------------------------------------------------------------------

def build_pyomo_model(data: Dict[str, Any]) -> pyo.ConcreteModel:
    cfg: OptimiserConfig = data["config"]

    t0 = time.perf_counter()
    t = t0

    model = pyo.ConcreteModel()

    F = data["F"]
    M = data["M"]
    VERT = data["VERTICAL_MODES"]
    MINI_MODES = data["MINIBUS_MODES"]
    R_AMB = data["R_AMB"]
    R_MINI = data["R_MINI"]
    R = data["R"]

    model.F = pyo.Set(initialize=F)
    model.M = pyo.Set(initialize=M)
    model.VERTICAL_MODES = pyo.Set(initialize=VERT)
    model.MINIBUS_MODES = pyo.Set(initialize=MINI_MODES)
    model.R_AMB = pyo.Set(initialize=R_AMB)
    model.R_MINI = pyo.Set(initialize=R_MINI)
    model.R = pyo.Set(initialize=R)

    t = step(
        t,
        (
            f"sets built | "
            f"F={len(F):,} "
            f"AMB={len(R_AMB):,} "
            f"MINI={len(R_MINI):,}"
        )
    )


    # Decision variables.
    model.x = pyo.Var(model.F, model.M, domain=pyo.Binary)

    model.amb = pyo.Var(
        model.F,
        model.R_AMB,
        model.VERTICAL_MODES,
        domain=pyo.Binary,
    )

    model.mini = pyo.Var(
        model.F,
        model.R_MINI,
        model.MINIBUS_MODES,
        domain=pyo.Binary,
    )

    model.use = pyo.Var(model.R, domain=pyo.Binary)

    future_vehicles = [r for r in R if data["vehicle_is_future"][r] == 1]
    model.R_FUTURE = pyo.Set(initialize=future_vehicles)

    model.buy = pyo.Var(model.R_FUTURE, domain=pyo.Binary)

    model.short_amb = pyo.Var(model.F, model.VERTICAL_MODES, domain=pyo.NonNegativeReals)
    model.short_mini_seat = pyo.Var(model.F, model.MINIBUS_MODES, domain=pyo.NonNegativeReals)
    model.short_mini_wc = pyo.Var(model.F, model.MINIBUS_MODES, domain=pyo.NonNegativeReals)

    # Peak staff is calculated after solve.
    # Staff still affects optimisation through staff_minutes in the objective.
    model.N_staff = pyo.Param(initialize=0.0, mutable=False)

    n_x = len(F) * len(M)

    n_amb = (
        len(F)
        * len(R_AMB)
        * len(VERT)
    )

    n_mini = (
        len(F)
        * len(R_MINI)
        * len(MINI_MODES)
    )

    t = step(
        t,
        (
            "variables built | "
            f"x={n_x:,} "
            f"amb={n_amb:,} "
            f"mini={n_mini:,}"
        )
    )

    # ---------------------------------------------------------
    # Mode selection
    # ---------------------------------------------------------

    def one_mode_rule(mdl, f):
        return sum(mdl.x[f, mm] for mm in mdl.M) == 1

    model.one_mode = pyo.Constraint(model.F, rule=one_mode_rule)

    # ---------------------------------------------------------
    # Vertical requirement alignment
    # ---------------------------------------------------------

    def vertical_alignment_rule(mdl, f):
        return sum(mdl.x[f, mm] for mm in mdl.VERTICAL_MODES) == data["NeedVertical"][f]

    model.vertical_alignment = pyo.Constraint(model.F, rule=vertical_alignment_rule)

    # ---------------------------------------------------------
    # Pusher remote restriction
    # ---------------------------------------------------------

    def pusher_remote_rule(mdl, f):
        # CP + SP <= 1 - Remote
        return mdl.x[f, "CP"] + mdl.x[f, "SP"] <= 1 - data["Remote"][f]

    model.pusher_remote = pyo.Constraint(model.F, rule=pusher_remote_rule)

    # ---------------------------------------------------------
    # Domestic arrival solo ambulift exclusion
    # ---------------------------------------------------------

    def domestic_arrival_sa_rule(mdl, f):
        return mdl.x[f, "SA"] <= 1 - (data["Arrival"][f] * data["Domestic"][f])

    model.domestic_arrival_sa = pyo.Constraint(model.F, rule=domestic_arrival_sa_rule)

    # ---------------------------------------------------------
    # Safety stand CP restriction
    # ---------------------------------------------------------

    def safety_cp_rule(mdl, f):
        return mdl.x[f, "CP"] <= 1 - data["Safety"][f]

    model.safety_cp = pyo.Constraint(model.F, rule=safety_cp_rule)

    # ---------------------------------------------------------
    # Ambulift assignment for vertical modes
    # ---------------------------------------------------------

    def amb_assignment_rule(mdl, f, mm):
        return (
            sum(mdl.amb[f, r, mm] for r in mdl.R_AMB)
            + mdl.short_amb[f, mm]
            == mdl.x[f, mm]
        )

    model.amb_assignment = pyo.Constraint(model.F, model.VERTICAL_MODES, rule=amb_assignment_rule)

    # ---------------------------------------------------------
    # Minibus capacity for CM / SM
    # ---------------------------------------------------------

    def mini_seat_capacity_rule(mdl, f, mm):
        return (
            sum(
                data["vehicle_seatcap"][r] * mdl.mini[f, r, mm]
                for r in mdl.R_MINI
            )
            + mdl.short_mini_seat[f, mm]
            >= data["D_seat"][f] * mdl.x[f, mm]
        )

    model.mini_seat_capacity = pyo.Constraint(
        model.F,
        model.MINIBUS_MODES,
        rule=mini_seat_capacity_rule,
    )

    def mini_wc_capacity_rule(mdl, f, mm):
        return (
            sum(
                data["vehicle_wccap"][r] * mdl.mini[f, r, mm]
                for r in mdl.R_MINI
            )
            + mdl.short_mini_wc[f, mm]
            >= data["D_wc"][f] * mdl.x[f, mm]
        )

    model.mini_wc_capacity = pyo.Constraint(
        model.F,
        model.MINIBUS_MODES,
        rule=mini_wc_capacity_rule,
    )

    def mini_only_if_mode_rule(mdl, f, r, mm):
        return mdl.mini[f, r, mm] <= mdl.x[f, mm]

    model.mini_only_if_mode = pyo.Constraint(
        model.F,
        model.R_MINI,
        model.MINIBUS_MODES,
        rule=mini_only_if_mode_rule,
    )

    # ---------------------------------------------------------
    # Vehicle activation and future purchase
    # ---------------------------------------------------------

    def use_current_amb_rule(mdl, f, r, mm):
        return mdl.amb[f, r, mm] <= mdl.use[r]

    model.use_current_amb = pyo.Constraint(
        model.F,
        model.R_AMB,
        model.VERTICAL_MODES,
        rule=use_current_amb_rule,
    )

    def use_current_mini_rule(mdl, f, r, mm):
        return mdl.mini[f, r, mm] <= mdl.use[r]

    model.use_current_mini = pyo.Constraint(
        model.F,
        model.R_MINI,
        model.MINIBUS_MODES,
        rule=use_current_mini_rule,
    )

    # Future minibuses can only be used if bought.
    future_mini = [r for r in R_MINI if data["vehicle_is_future"][r] == 1]

    def future_mini_buy_rule(mdl, f, r, mm):
        if r not in future_mini:
            return pyo.Constraint.Skip
        return mdl.mini[f, r, mm] <= mdl.buy[r]

    model.future_mini_buy = pyo.Constraint(
        model.F,
        model.R_MINI,
        model.MINIBUS_MODES,
        rule=future_mini_buy_rule,
    )

    def future_use_buy_rule(mdl, r):
        return mdl.use[r] <= mdl.buy[r]

    model.future_use_buy = pyo.Constraint(
        model.R_FUTURE,
        rule=future_use_buy_rule,
    )

    # ---------------------------------------------------------
    # Timeline no-overlap constraints
    # ---------------------------------------------------------

    model.AMB_OVERLAPS = pyo.Set(
        dimen=6,
        initialize=data["amb_overlap_pairs"],
    )

    def amb_overlap_rule(mdl, f1, r1, m1, f2, r2, m2):
        return mdl.amb[f1, r1, m1] + mdl.amb[f2, r2, m2] <= 1

    model.amb_no_overlap = pyo.Constraint(
        model.AMB_OVERLAPS,
        rule=amb_overlap_rule,
    )

    t = step(
        t,
        (
            "ambulift overlap constraints built | "
            f"{len(data['amb_overlap_pairs']):,}"
        )
    )

    model.MINI_OVERLAPS = pyo.Set(
        dimen=6,
        initialize=data["mini_overlap_pairs"],
    )

    def mini_overlap_rule(mdl, f1, r1, m1, f2, r2, m2):
        return mdl.mini[f1, r1, m1] + mdl.mini[f2, r2, m2] <= 1

    model.mini_no_overlap = pyo.Constraint(
        model.MINI_OVERLAPS,
        rule=mini_overlap_rule,
    )

    t = step(
        t,
        (
            "minibus overlap constraints built | "
            f"{len(data['mini_overlap_pairs']):,}"
        )
    )   

    # ---------------------------------------------------------
    # Peak staff is not a Pyomo constraint in V2A
    # ---------------------------------------------------------
    # Staff affects optimisation via staff-minutes in the objective.
    # Peak staff is calculated after solve for reporting.

    model.EVENTS = pyo.Set(initialize=[])

    # ---------------------------------------------------------
    # Objective
    # ---------------------------------------------------------

    def objective_rule(mdl):
        shortfall_penalty = cfg.penalty_shortfall * (
            sum(mdl.short_amb[f, mm] for f in mdl.F for mm in mdl.VERTICAL_MODES)
            + sum(mdl.short_mini_seat[f, mm] for f in mdl.F for mm in mdl.MINIBUS_MODES)
            + sum(mdl.short_mini_wc[f, mm] for f in mdl.F for mm in mdl.MINIBUS_MODES)
        )

        future_purchase_penalty = cfg.penalty_future_buy * (
            sum(data["vehicle_capex"][r] * mdl.buy[r] for r in mdl.R_FUTURE)
        )

        current_use_penalty = cfg.penalty_current_use * (
            sum(data["vehicle_capex"][r] * mdl.use[r] for r in mdl.R)
        )

        staff_penalty = cfg.penalty_staff_minutes * (
            sum(
                data["staff_minutes"][(f, mm)] * mdl.x[f, mm]
                for f in mdl.F
                for mm in mdl.M
            )
        )

        return (
            shortfall_penalty
            + future_purchase_penalty
            + current_use_penalty
            + staff_penalty
        )

    model.obj = pyo.Objective(rule=objective_rule, sense=pyo.minimize)

    t = step(
        t,
        (
            "build_pyomo_model complete | "
            f"total={(time.perf_counter()-t0):.2f}s"
        )
    )

    return model


# ---------------------------------------------------------------------
# Solve
# ---------------------------------------------------------------------

def solve_model(model: pyo.ConcreteModel, config: OptimiserConfig):
    """
    Solve with HiGHS.

    Tries appsi_highs first, then highs.
    """

    solver_names = [config.solver_name_preferred, "highs"]

    last_error = None

    for solver_name in solver_names:
        try:
            solver = pyo.SolverFactory(solver_name)
            if solver is None or not solver.available(False):
                continue

            print("\n" + "=" * 80)
            print("Starting HiGHS solve")
            print("=" * 80)

            print(
                f"Flights={len(model.F):,} "
                f"Vehicles={len(model.R):,}"
            )

            results = solver.solve(model, tee=config.show_solver_log)
            return results

        except Exception as e:
            last_error = e
            continue

    raise RuntimeError(
        "Could not solve model with HiGHS. "
        "Check that pyomo and highspy are installed. "
        f"Last error: {last_error}"
    )


# ---------------------------------------------------------------------
# Extract outputs
# ---------------------------------------------------------------------

def extract_solution(
    model: pyo.ConcreteModel,
    data: Dict[str, Any],
) -> Dict[str, pd.DataFrame]:
    """
    Extract the main output tables.
    """

    fdf = data["flights_df"].copy()
    vehicles = data["vehicles_df"].copy()

    F = data["F"]
    M = data["M"]
    R_AMB = data["R_AMB"]
    R_MINI = data["R_MINI"]

    # Mode selected.
    mode_rows = []

    for f in F:
        chosen = None
        for m in M:
            if pyo.value(model.x[f, m]) > 0.5:
                chosen = m
                break

        mode_rows.append(
            {
                "flight_key": f,
                "chosen_mode": chosen,
            }
        )

    df_modes = pd.DataFrame(mode_rows)

    # Vehicle assignments.
    assignment_rows = []

    for f in F:
        for r in R_AMB:
            for m in data["VERTICAL_MODES"]:
                if pyo.value(model.amb[f, r, m]) > 0.5:
                    s, e = data["amb_interval"][(f, r, m)]
                    assignment_rows.append(
                        {
                            "flight_key": f,
                            "vehicle_id": r,
                            "vehicle_type": "Amb",
                            "vehicle_group": (
                                f"Amb_{data['vehicle_seatcap'][r]}seat_"
                                f"{data['vehicle_wccap'][r]}wc"
                            ),
                            "mode": m,
                            "interval_start_min": s,
                            "interval_end_min": e,
                            "trips": data["amb_trips"][(f, r)],
                            "seatcap": data["vehicle_seatcap"][r],
                            "wccap": data["vehicle_wccap"][r],
                            "is_future": data["vehicle_is_future"][r],
                            "base_model": data["vehicle_base_model"][r],
                        }
                    )

        for r in R_MINI:
            for m in data["MINIBUS_MODES"]:
                if pyo.value(model.mini[f, r, m]) > 0.5:
                    s, e = data["mini_interval"][(f, m)]
                    assignment_rows.append(
                        {
                            "flight_key": f,
                            "vehicle_id": r,
                            "vehicle_type": "Mini",
                            "vehicle_group": (
                                f"Mini_{data['vehicle_seatcap'][r]}seat_"
                                f"{data['vehicle_wccap'][r]}wc"
                            ),
                            "mode": m,
                            "interval_start_min": s,
                            "interval_end_min": e,
                            "trips": np.nan,
                            "seatcap": data["vehicle_seatcap"][r],
                            "wccap": data["vehicle_wccap"][r],
                            "is_future": data["vehicle_is_future"][r],
                            "base_model": data["vehicle_base_model"][r],
                        }
                    )

    df_assign = pd.DataFrame(assignment_rows)

    if len(df_assign) > 0:
        origin = data["origin"]

        df_assign["service_start"] = df_assign["interval_start_min"].apply(
            lambda x: origin + pd.to_timedelta(x, unit="m")
        )
        df_assign["service_end"] = df_assign["interval_end_min"].apply(
            lambda x: origin + pd.to_timedelta(x, unit="m")
        )

    # Flight-level output.
    df_flight_out = fdf.merge(df_modes, on="flight_key", how="left")

    # --------------------------------------------------
    # SLA REPORTING
    # --------------------------------------------------

    df_flight_out["arrival_breach"] = 0
    df_flight_out["departure_breach"] = 0

    arrival_mask = (
        df_flight_out["arr_dep"] == "A"
    )

    departure_mask = (
        df_flight_out["arr_dep"] == "D"
    )

    arrival_flights = int(arrival_mask.sum())
    arrival_breaches = int(df_flight_out["arrival_breach"].sum())

    departure_flights = int(departure_mask.sum())
    departure_breaches = int(df_flight_out["departure_breach"].sum())

    arrival_flight_sla_pct = (
        100.0
        if arrival_flights == 0
        else 100.0 * (
            1.0
            - arrival_breaches / arrival_flights
        )
    )

    departure_sla_pct = (
        100.0
        if departure_flights == 0
        else 100.0 * (
            1.0
            - departure_breaches / departure_flights
        )
    )

    if len(df_assign) > 0:
        amb_map = (
            df_assign[df_assign["vehicle_type"] == "Amb"]
            .groupby("flight_key")["vehicle_id"]
            .apply(lambda s: ",".join(s.astype(str)))
            .rename("assigned_ambulifts")
            .reset_index()
        )

        mini_map = (
            df_assign[df_assign["vehicle_type"] == "Mini"]
            .groupby("flight_key")["vehicle_id"]
            .apply(lambda s: ",".join(s.astype(str)))
            .rename("assigned_minibuses")
            .reset_index()
        )

        df_flight_out = df_flight_out.merge(amb_map, on="flight_key", how="left")
        df_flight_out = df_flight_out.merge(mini_map, on="flight_key", how="left")
    else:
        df_flight_out["assigned_ambulifts"] = None
        df_flight_out["assigned_minibuses"] = None

    # Shortfalls.
    short_rows = []

    for f in F:
        for m in data["VERTICAL_MODES"]:
            val = pyo.value(model.short_amb[f, m])
            if val > 1e-6:
                short_rows.append(
                    {
                        "flight_key": f,
                        "mode": m,
                        "shortfall_type": "ambulift",
                        "shortfall_value": val,
                    }
                )

        for m in data["MINIBUS_MODES"]:
            val_seat = pyo.value(model.short_mini_seat[f, m])
            val_wc = pyo.value(model.short_mini_wc[f, m])

            if val_seat > 1e-6:
                short_rows.append(
                    {
                        "flight_key": f,
                        "mode": m,
                        "shortfall_type": "minibus_seat",
                        "shortfall_value": val_seat,
                    }
                )

            if val_wc > 1e-6:
                short_rows.append(
                    {
                        "flight_key": f,
                        "mode": m,
                        "shortfall_type": "minibus_wc",
                        "shortfall_value": val_wc,
                    }
                )

    df_short = pd.DataFrame(short_rows)

    # Fleet summary.
    fleet_rows = []

    for r in data["R"]:
        used = int(pyo.value(model.use[r]) > 0.5)
        bought = 0
        if r in list(model.R_FUTURE):
            bought = int(pyo.value(model.buy[r]) > 0.5)

        fleet_rows.append(
            {
                "vehicle_id": r,
                "base_model": data["vehicle_base_model"][r],
                "vehicle_type": data["vehicle_type"][r],
                "vehicle_group": (
                    f"{data['vehicle_type'][r]}_"
                    f"{data['vehicle_seatcap'][r]}seat_"
                    f"{data['vehicle_wccap'][r]}wc"
                ),
                "seatcap": data["vehicle_seatcap"][r],
                "wccap": data["vehicle_wccap"][r],
                "is_future": data["vehicle_is_future"][r],
                "used": used,
                "bought": bought,
                "capex_hr": data["vehicle_capex"][r],
            }
        )

    df_fleet = pd.DataFrame(fleet_rows)

    # --------------------------------------------------
    # Fleet utilisation / requirement summary
    # --------------------------------------------------
    # This answers:
    #   - how many vehicles of each group exist?
    #   - how many were used at least once?
    #   - what was the peak concurrent usage?
    #   - what is the gap versus current fleet?

    fleet_util_rows = []

    if len(df_fleet) > 0:
        fleet_groups = sorted(df_fleet["vehicle_group"].dropna().unique())
    else:
        fleet_groups = []

    for vehicle_group in fleet_groups:

        fleet_g = df_fleet[df_fleet["vehicle_group"] == vehicle_group].copy()

        vehicle_type = fleet_g["vehicle_type"].iloc[0]
        seatcap = int(fleet_g["seatcap"].iloc[0])
        wccap = int(fleet_g["wccap"].iloc[0])

        current_available = int(
            fleet_g.loc[fleet_g["is_future"].astype(int) == 0].shape[0]
        )

        future_candidates_available = int(
            fleet_g.loc[fleet_g["is_future"].astype(int) == 1].shape[0]
        )

        used_at_least_once = int(
            fleet_g["used"].fillna(0).astype(int).sum()
        )

        future_bought = int(
            fleet_g["bought"].fillna(0).astype(int).sum()
            if "bought" in fleet_g.columns
            else 0
        )

        # Peak concurrent usage from the actual assignment intervals.
        peak_concurrent_used = 0

        if len(df_assign) > 0 and "vehicle_group" in df_assign.columns:
            assign_g = df_assign[
                df_assign["vehicle_group"] == vehicle_group
            ].copy()

            if len(assign_g) > 0:
                event_points = sorted(
                    set(assign_g["interval_start_min"].tolist())
                    | set(assign_g["interval_end_min"].tolist())
                )

                for tau in event_points:
                    active = assign_g[
                        (assign_g["interval_start_min"] <= tau)
                        & (tau < assign_g["interval_end_min"])
                    ]

                    concurrent = int(active["vehicle_id"].nunique())

                    if concurrent > peak_concurrent_used:
                        peak_concurrent_used = concurrent

        required_for_schedule = peak_concurrent_used

        gap_vs_current = max(
            0,
            required_for_schedule - current_available,
        )

        surplus_vs_current = max(
            0,
            current_available - required_for_schedule,
        )

        fleet_util_rows.append(
            {
                "vehicle_group": vehicle_group,
                "vehicle_type": vehicle_type,
                "seatcap": seatcap,
                "wccap": wccap,

                "current_available": current_available,
                "future_candidates_available": future_candidates_available,

                "used_at_least_once": used_at_least_once,
                "peak_concurrent_used": peak_concurrent_used,
                "required_for_schedule": required_for_schedule,

                "gap_vs_current": gap_vs_current,
                "surplus_vs_current": surplus_vs_current,

                "future_bought": future_bought,
            }
        )

    df_fleet_utilisation = pd.DataFrame(fleet_util_rows)

    # --------------------------------------------------
    # Fleet requirement summary
    # --------------------------------------------------

    fleet_req_rows = []

    for base_model in sorted(df_fleet["base_model"].unique()):

        tmp = df_fleet[
            df_fleet["base_model"] == base_model
        ]

        available = len(tmp)

        used = int(
            tmp["used"].fillna(0).astype(int).sum()
        )

        bought = int(
            tmp["bought"].fillna(0).astype(int).sum()
            if "bought" in tmp.columns
            else 0
        )

        fleet_req_rows.append(
            {
                "base_model": base_model,
                "vehicle_type": tmp["vehicle_type"].iloc[0],
                "available": available,
                "used": used,
                "unused": available - used,
                "future_bought": bought,
                "gap_to_full_utilisation": max(
                    0,
                    available - used
                ),
            }
        )

    df_fleet_requirements = pd.DataFrame(
        fleet_req_rows
    )

    # --------------------------------------------------
    # Staff summary calculated post-solve
    # --------------------------------------------------

    chosen_mode = {
        row["flight_key"]: row["chosen_mode"]
        for _, row in df_modes.iterrows()
    }

    staff_jobs = []

    for f in F:
        m_chosen = chosen_mode.get(f)

        if m_chosen is None:
            continue

        if (f, m_chosen) not in data["staff_interval"]:
            continue

        s, e = data["staff_interval"][(f, m_chosen)]

        staff_jobs.append(
            {
                "flight_key": f,
                "chosen_mode": m_chosen,
                "interval_start_min": s,
                "interval_end_min": e,
                "staff_required": float(data["staff_req"][(f, m_chosen)]),
                "staff_minutes": float(data["staff_minutes"][(f, m_chosen)]),
            }
        )

    df_staff_jobs = pd.DataFrame(staff_jobs)

    staff_rows = []

    if len(df_staff_jobs) > 0:
        staff_event_points = sorted(
            set(df_staff_jobs["interval_start_min"].tolist())
            | set(df_staff_jobs["interval_end_min"].tolist())
        )

        for tau in staff_event_points:
            active = df_staff_jobs[
                (df_staff_jobs["interval_start_min"] <= tau)
                & (tau < df_staff_jobs["interval_end_min"])
            ]

            staff_required = float(active["staff_required"].sum())

            staff_rows.append(
                {
                    "event_min": tau,
                    "event_time": data["origin"] + pd.to_timedelta(tau, unit="m"),
                    "staff_required": staff_required,
                    "active_flights": int(active["flight_key"].nunique()),
                }
            )

    df_staff = pd.DataFrame(staff_rows)

    if len(df_staff) > 0:
        final_peak_staff = float(df_staff["staff_required"].max())
        df_staff["N_staff_peak"] = final_peak_staff
    else:
        final_peak_staff = 0.0

    if len(df_staff_jobs) > 0:
        df_staff_jobs["N_staff_peak"] = final_peak_staff
    else:
        df_staff_jobs = pd.DataFrame(
            columns=[
                "flight_key",
                "chosen_mode",
                "interval_start_min",
                "interval_end_min",
                "staff_required",
                "staff_minutes",
                "N_staff_peak",
            ]
        )

    df_sla = pd.DataFrame(
    [{
        "arrival_flights": arrival_flights,
        "arrival_breaches": arrival_breaches,
        "arrival_flight_sla_pct": arrival_flight_sla_pct,
        "arrival_target_pct": (
            data["config"].arrival_sla_target_pct * 100
        ),

        "departure_flights": departure_flights,
        "departure_breaches": departure_breaches,
        "departure_sla_pct": departure_sla_pct,
    }]
    )

    return {
        "flight_assignments": df_flight_out,
        "vehicle_schedule": df_assign,
        "fleet_summary": df_fleet,
        "fleet_requirements": df_fleet_requirements,
        "fleet_utilisation": df_fleet_utilisation,
        "staff_summary": df_staff,
        "staff_jobs": df_staff_jobs,
        "shortfalls": df_short,
        "sla_summary": df_sla,
    }

# ---------------------------------------------------------------------
# Convenience runner
# ---------------------------------------------------------------------

def optimise_prm_fleet_v2(
    flights: pd.DataFrame,
    vehicle_models: Dict[str, Dict[str, Any]],
    config: OptimiserConfig | None = None,
) -> Dict[str, pd.DataFrame]:
    """
    End-to-end optimisation runner.

    Example:
        outputs = optimise_prm_fleet_v2(
            flights=df_flights_model,
            vehicle_models=VEHICLE_MODELS,
            config=OptimiserConfig(),
        )
    """

    if config is None:
        config = OptimiserConfig()

    data = prepare_model_data(
        flights=flights,
        vehicle_models=vehicle_models,
        config=config,
    )

    model = build_pyomo_model(data)
    results = solve_model(model, config)

    outputs = extract_solution(model, data)
    outputs["solver_results"] = pd.DataFrame(
        [
            {
                "solver_status": str(results.solver.status),
                "termination_condition": str(results.solver.termination_condition),
                "objective_value": pyo.value(model.obj),
            }
        ]
    )

    return outputs