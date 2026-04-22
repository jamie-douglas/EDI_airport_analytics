
"""
prm_opt.params
--------------
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
import numpy as np
from .config import PlanningToggles, VEHICLE_MODELS



def build_tau_from_jobs(
    jobs: pd.DataFrame,
    toggles: PlanningToggles,
) -> Dict[Tuple[int, str], float]:
    """
    τ_{j,r}: busy minutes for job j and resource r.

    Resources currently USED by optimisation:
      - "Amb"   : Ambulift vehicle busy minutes
      - "Mini"  : Minibus vehicle busy minutes
      - "Push"  : Pusher busy minutes (walking support)

    Resources DEFERRED (kept for future staff optimisation):
      - "Driver"
      - "VehAg"

    Notes:
    - base_duration_mins is the observed S25 job duration.
    - handover_overlap_mins is NOT baked into τ; it is added conditionally
      in the optimisation when a transfer (Amb + Mini) is chosen.
    """

    tau: Dict[Tuple[int, str], float] = {}

    for j, row in jobs.iterrows():
        base = float(row["base_duration_mins"])

        # -------------------------
        # Vehicle / mode resources
        # -------------------------
        tau[(j, "Amb")] = base
        tau[(j, "Mini")] = base
        tau[(j, "Push")] = base

        # -------------------------
        # DEFERRED staff resources
        # -------------------------
        # These are kept to preserve model scope completeness,
        # but are NOT yet consumed by the optimisation.
        tau[(j, "Driver")] = base
        tau[(j, "VehAg")] = base

    return tau




def build_spin_minutes(
    jobs: pd.DataFrame,
    spin_lock_threshold_mins: int = 50,
) -> Dict[Any, float]:
    """
    Spin lock capacity removed per time bucket.

    Model Scope mapping:
    - ambulift minutes unavailable due to spins/lock rules. 
    """
    spin_removed: Dict[Any, float] = {}
    for t, grp in jobs.groupby("t"):
        spin_count = int(grp["is_spin"].sum())
        spin_removed[t] = float(spin_count * spin_lock_threshold_mins)
    return spin_removed


def build_vehicle_classes(include_future: bool = False) -> Dict[str, List[Dict]]:
    """
    Build capacity classes from VEHICLE_MODELS by (type, seatcap, wccap).
    Respects heterogeneous vehicle capacities.
    """
    def is_existing(spec): return float(spec.get("capex_hr", 0.0)) == 0.0

    buckets = defaultdict(lambda: defaultdict(int))
    for spec in VEHICLE_MODELS.values():
        if (not include_future) and (not is_existing(spec)):
            continue
        vtype = spec["type"]  # "Amb" or "Mini"
        seat = int(spec["seatcap"])
        wc = int(spec["wccap"])
        buckets[vtype][(seat, wc)] += 1

    classes: Dict[str, List[Dict]] = {}
    for vtype, combos in buckets.items():
        out = []
        for idx, ((seat, wc), cnt) in enumerate(sorted(combos.items())):
            out.append({"class_id": f"{vtype}_C{idx}", "seatcap": seat, "wccap": wc, "count": int(cnt)})
        classes[vtype] = out

    return classes


# =========================================================
# DEFERRED PLACEHOLDERS (COMMENTS ONLY)
# =========================================================

# def build_ferry_minutes(jobs, shifts, ferry_time_minutes):
#     """
#     Deferred: remove minibus minutes in buckets where minibuses are used to ferry ambulift drivers.
#     This will subtract from minibus time capacity, similar to spin_removed for ambulifts.
#     """
#     raise NotImplementedError
#
# def sample_delay_minutes(std_mins=15):
#     """
#     Deferred: ω scenario delay model (arrival variability). Used once SLA constraints are activated.
#     """
#     raise NotImplementedError
#
# def build_staff_break_minutes(...):
#     """
#     Deferred: staff/break constraints for drivers/agents/pushers (Model Scope staff capacity block).
#     """
#     raise NotImplementedError
