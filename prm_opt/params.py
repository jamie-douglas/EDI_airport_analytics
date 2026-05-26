# scripts/prm_opt/params.py

"""

Build optimisation parameters τ and capacity adjustments.

Implements:
- τ (minutes busy) per job and mode)
- spin locked minutes per time bucket (ambulift minutes removed)
- fleet capacity classes (seat and wc)
- handover minutes (combined jobs)
- break factor adjustment
"""

from __future__ import annotations
from collections import defaultdict
from typing import Dict, Tuple, Any, List

import pandas as pd
import math
import numpy as np
from .config import PlanningToggles, VEHICLE_MODELS



def build_tau_from_jobs(
    jobs: pd.DataFrame,
    toggles: PlanningToggles,
) -> Dict[Tuple[int, str], float]:
    """
    τ_{j,m}: busy minutes for job j and mode m.

    Resources currently USED by optimisation:
      - "Amb"   : Ambulift vehicle busy minutes
      - "Mini"  : Minibus vehicle busy minutes
      - "Push"  : Pusher busy minutes (walking support)


    
    Notes:
      - base_duration_mins already includes stochastic service time
      - transfer overlap added in Pyomo when Mini + vertical is chosen

    """

    tau: Dict[Tuple[int, str], float] = {}

    for j, row in jobs.iterrows():
        base = float(row["base_duration_mins"])

        # -------------------------
        # Vehicle / mode resources
        # -------------------------
        
        tau[(j, "Amb")]  = float(row.get("tau_amb_mins", base))
        tau[(j, "Mini")] = float(row.get("tau_mini_mins", base))
        tau[(j, "Push")] = float(row.get("tau_push_mins", base))


        # -------------------------
        # DEFERRED staff resources
        # -------------------------
        # These are kept to preserve model scope completeness,
        # but are NOT yet consumed by the optimisation.
        
        tau[(j, "Driver")] = tau[(j, "Amb")]
        tau[(j, "VehAg")]  = tau[(j, "Amb")]


    return tau






# def build_spin_minutes(
#     jobs: pd.DataFrame,
#     spin_lock_threshold_mins: int = 50,
#     bucket_minutes: int = 15,
#     n_ambulifts: int = 11,   # <-- pass your real fleet count
# ) -> Dict[Any, float]:
#     """
#     Spin lock capacity removed per SERVICE time bucket.

#     Interpretation:
#     - Each spin event locks 1 ambulift for spin_lock_threshold_mins.
#     - That lock applies across multiple buckets (ceil(lock / bucket_minutes)).
#     - We translate locked ambulifts into removed minutes:
#         removed_minutes[b] = locked_amb[b] * bucket_minutes
#     - Cap removed minutes so it can never exceed total fleet minutes in a bucket.
#     """

#     # Use 's' (scheduled) if present so spin timing isn't shifted by preposition.
#     bucket_col = "s" if "s" in jobs.columns else "t"

#     # Ensure timestamps
#     s_ts = pd.to_datetime(jobs[bucket_col]).dt.floor(f"{bucket_minutes}min")

#     # We'll build locked ambulifts per bucket first
#     locked_amb: Dict[pd.Timestamp, int] = {}

#     # Number of buckets a spin occupies
#     lock_buckets = int(math.ceil(spin_lock_threshold_mins / bucket_minutes))

#     # For each job marked as spin, lock an ambulift for lock_buckets starting at its bucket time
#     # (This assumes each spin marker corresponds to an ambulift that becomes unavailable)
#     for start_time in s_ts[jobs["is_spin"] == 1]:
#         for k in range(lock_buckets):
#             b = start_time + pd.to_timedelta(k * bucket_minutes, unit="m")
#             locked_amb[b] = locked_amb.get(b, 0) + 1

#     # Convert locked ambulifts -> minutes removed and cap to physical max
#     cap_minutes = float(n_ambulifts * bucket_minutes)
#     spin_removed: Dict[Any, float] = {}

#     for b, locked in locked_amb.items():
#         removed = float(locked * bucket_minutes)
#         spin_removed[b] = min(removed, cap_minutes)

#     return spin_removed


def build_spin_minutes(
    jobs: pd.DataFrame,
    spin_lock_threshold_mins: int = 50,
    bucket_minutes: int = 15,
    n_ambulifts: int = 11,
) -> Dict[Any, float]:
    """
    Spin lock capacity removed per SERVICE time bucket.

    FIX: count spin events per FLIGHT (flight_key), not per passenger job.
    Otherwise many passengers on the same spin flight will 'lock' many ambulifts
    and can saturate the entire fleet.
    """
    if "is_spin" not in jobs.columns or jobs["is_spin"].sum() == 0:
        return {}

    bucket_col = "s" if "s" in jobs.columns else "t"
    s_ts = pd.to_datetime(jobs[bucket_col]).dt.floor(f"{bucket_minutes}min")

    # Number of buckets a spin occupies
    lock_buckets = int(math.ceil(spin_lock_threshold_mins / bucket_minutes))

    # ---- KEY FIX: one spin event per flight_key ----
    if "flight_key" in jobs.columns:
        spin_flights = jobs.index[jobs["is_spin"] == 1]
        # choose one representative start bucket per flight (min is fine)
        spin_start_by_flight = (
            pd.DataFrame({"flight_key": jobs.loc[spin_flights, "flight_key"], "start": s_ts.loc[spin_flights]})
            .groupby("flight_key")["start"]
            .min()
        )
        spin_starts = spin_start_by_flight.values
    else:
        # fallback: unique start buckets (still better than per-passenger)
        spin_starts = s_ts[jobs["is_spin"] == 1].unique()

    locked_amb = defaultdict(int)

    for start_time in spin_starts:
        for k in range(lock_buckets):
            b = pd.Timestamp(start_time) + pd.to_timedelta(k * bucket_minutes, unit="m")
            locked_amb[b] += 1

    cap_minutes = float(n_ambulifts * bucket_minutes)
    spin_removed = {}
    for b, locked in locked_amb.items():
        removed = float(locked * bucket_minutes)
        spin_removed[b] = min(removed, cap_minutes)
    return spin_removed



def build_vehicle_classes(include_future: bool = False) -> Dict[str, List[Dict]]:
    """
    Build heterogeneous capacity classes from VEHICLE_MODELS.

    Current fleet:
      - each physical vehicle is its own class_id, count=1
    Future options:
      - each model type is a class_id, count=1000 (effectively unlimited)
    """
    if not VEHICLE_MODELS:
        raise ValueError("VEHICLE_MODELS is empty in config.py. Populate fleet registry before running optimisation.")

    classes: Dict[str, List[Dict]] = {"Amb": [], "Mini": []}

    # Current vehicles: each entry is one physical unit unless marked is_future=True
    for model_num, spec in VEHICLE_MODELS.items():
        vtype = spec["type"]
        if not bool(spec.get("is_future", False)):
            classes[vtype].append({
                "class_id": model_num,
                "seatcap": int(spec["seatcap"]),
                "wccap": int(spec["wccap"]),
                "count": 1,
            })

    if include_future:
        # Future vehicles: group by model type, unlimited count
        future_models: Dict[tuple, Dict] = {}
        for model_num, spec in VEHICLE_MODELS.items():
            vtype = spec["type"]
            if bool(spec.get("is_future", False)):
                key = (vtype, model_num)
                if key not in future_models:
                    future_models[key] = {
                        "class_id": model_num,
                        "seatcap": int(spec["seatcap"]),
                        "wccap": int(spec["wccap"]),
                        "count": 2,
                    }
        for (vtype, _), v in future_models.items():
            classes[vtype].append(v)

    return {k: v for k, v in classes.items() if v}



# =========================================================
# DEFERRED PLACEHOLDERS (COMMENTS ONLY)
# =========================================================

# def build_ferry_minutes(...):
#     """
#     Deferred: remove minibus minutes in buckets where minibuses
#     are used to ferry ambulift drivers.
#     """
#     raise NotImplementedError

#
# def build_staff_break_minutes(...):
#     """
#     Deferred: driver / agent break constraints.
#     """
#     raise NotImplementedError
