# scripts/prm_opt/build_jobs.py
"""

Build the canonical PRM job table (J) used by the optimiser.


This file enforces:
- Correct vertical logic (SSR AND effective remote)
- Gate 7/8 identification (Scenario 2 & 3)
- SLA thresholds (with buffer)
- Stand-zone mapping for batching (Scenario 3)
- Domestic/International class derived from Sector

"""

from __future__ import annotations
import pandas as pd
import numpy as np

from .config import (
    PlanningToggles,
    SAFETY_STANDS,
    LIFT_STANDS,
    VERTICAL_EXCEPTIONS,
    STAND_ZONES,
)


def build_jobs(
    df_prm_master: pd.DataFrame,
    bucket: str = "15min",
    toggles: PlanningToggles = PlanningToggles(),
) -> pd.DataFrame:
    """
    Build job-level optimisation table.

    Each row = one PRM job j.

    Required by Model Scope:
    - j ∈ J
    - f(j)
    - d(j)
    - t(j)
    - vert(j), wc(j)
    - lift gate indicator
    """

    x = df_prm_master.copy()

    
    # --------------------------------------------------
    # Defensive fill for flags coming from left joins
    # --------------------------------------------------
    for col in [
        "Has Own Chair",
        "IsEffectiveRemote",
        "PRM Flight Count",
        "Turnaround PRM Count",
        "Concurrent Stress",
    ]:
        if col in x.columns:
            x[col] = x[col].fillna(0)

            
    # --------------------------------------------------
    # SSR numeric (used by policy_s1)
    # --------------------------------------------------
    if "SSR numeric" not in x.columns:
        x["SSR numeric"] = x["SSR Code"].map(
            {"WCHC": 3, "WCHS": 2}
        ).fillna(1)


    
    
    # Fallback: if Job Start Time missing, use Scheduled Flight DT
    if "Scheduled Flight DT" in x.columns:
        x["Job Start Time"] = x["Job Start Time"].fillna(x["Scheduled Flight DT"])

    x = x.dropna(subset=["Job Start Time"]).copy()



   
    # -------------------------
    # Time bucket (with optional preposition)
    # -------------------------
    # We interpret Job Start Time as the "release" moment for SLA.
    # Preposition shifts this earlier to represent vehicles needing to be in place
    # before the service is expected/required.
    prepos = np.where(
        x["A/D"] == "A",
        toggles.preposition_arrival_mins,
        toggles.preposition_departure_mins,
    )

    x["release_time"] = x["Job Start Time"] - pd.to_timedelta(prepos, unit="m")
    x["t"] = x["release_time"].dt.floor(bucket)
    x["s"] = x["Job Start Time"].dt.floor(bucket)

    
    
    # -------------------------
    # SLA start time (works for S25 + S26)
    # -------------------------
    # S25 columns: "Chocks DT", "Scheduled Flight DT"
    # S26 columns: "Chocks_Est", "ScheduledDateTime_Local"
    if "Chocks DT" in x.columns:
        chocks_col = "Chocks DT"
    elif "Chocks_Est" in x.columns:
        chocks_col = "Chocks_Est"
    else:
        chocks_col = None

    if "Scheduled Flight DT" in x.columns:
        sched_col = "Scheduled Flight DT"
    elif "ScheduledDateTime_Local" in x.columns:
        sched_col = "ScheduledDateTime_Local"
    else:
        sched_col = "Job Start Time"  # fallback

    if chocks_col is not None:
        x["sla_start_time"] = np.where(
            x["A/D"] == "A",
            pd.to_datetime(x[chocks_col]).fillna(pd.to_datetime(x[sched_col])),
            pd.to_datetime(x[sched_col]),
        )
    else:
        x["sla_start_time"] = pd.to_datetime(x[sched_col])

    
    x["sla_start_time"] = x["sla_start_time"].fillna(x["Job Start Time"])
    x["sla_start_time"] = pd.to_datetime(x["sla_start_time"])


   
    # -------------------------
    # Hard deadline time (departures): never earlier than release_time
    # -------------------------
    dep_buffer = int(getattr(toggles, "dep_boarding_buffer_mins", 0) or 0)

    # Choose scheduled column name robustly (S25 vs S26)
    if "Scheduled Flight DT" in x.columns:
        sched_col = "Scheduled Flight DT"
    elif "ScheduledDateTime_Local" in x.columns:
        sched_col = "ScheduledDateTime_Local"
    else:
        sched_col = "Job Start Time"

    sched = pd.to_datetime(x[sched_col])

    # "Raw" deadline: scheduled minus buffer (buffer=0 means scheduled)
    deadline_raw = sched - pd.to_timedelta(dep_buffer, unit="m")

    # Ensure feasibility: deadline cannot be earlier than release_time
    x["hard_deadline_time"] = np.where(
        x["A/D"] == "D",
        np.maximum(pd.to_datetime(deadline_raw), pd.to_datetime(x["release_time"])),
        pd.NaT,
    )
    x["hard_deadline_time"] = pd.to_datetime(x["hard_deadline_time"])



    # -------------------------
    # Direction
    # -------------------------
    x["dir"] = x["A/D"]

    
    # -------------------------
    # Stand zone for batching
    # -------------------------
    x["zone"] = x["Stand"].astype(str).map(STAND_ZONES).fillna("UNK")



    
    # -------------------------
    # Class (Dom / Int / CTA) from Sector
    # -------------------------
    sec = x["Sector"].astype(str).str.upper()
    x["class"] = np.where(
        sec.str.contains("DOM"),
        "Dom",
        np.where(sec.isin(["IRISH", "NIRISH"]), "CTA", "Int"),
    )


    # -------------------------
    # Policy / forecasting features (must exist for S25 + S26)
    # -------------------------
    # If these are missing you’ll get silent failures in policy_s1.
    required = [
        "Concurrent Stress",
        "Turnaround PRM Count",
        "IsArrival",
        "PRM Flight Count",
    ]
    for c in required:
        if c not in x.columns:
            raise ValueError(f"Missing required column for jobs: {c}")


    # -------------------------
    # Wheelchair slot (k_j)
    # -------------------------
    x["Has Own Chair"] = x["Has Own Chair"].fillna(0)

    x["needs_wc"] = (
        (x["SSR Code"] == "WCHC") |
        ((x["SSR Code"] == "WCHS") & (x["Has Own Chair"] == 1))
    ).astype(int)

    # -------------------------
    # Effective remote
    # -------------------------
    x["is_effective_remote"] = x["IsEffectiveRemote"].astype(int)

    # -------------------------
    # Vertical requirement
    # -------------------------
    base_vertical = (
        x["SSR Code"].isin(["WCHC", "WCHS"]) &
        (x["is_effective_remote"] == 1)
    )

    exception_vertical = x.apply(
        lambda r: (r["Airline Code"], str(r["Stand"])) in VERTICAL_EXCEPTIONS,
        axis=1,
    )

    x["needs_vertical"] = (base_vertical | exception_vertical).astype(int)

    # -------------------------
    # Safety stand
    # -------------------------
    x["safety_stand"] = x["Stand"].astype(str).isin(SAFETY_STANDS).astype(int)

    # -------------------------
    # Stand 7/8 bottleneck flag
    # -------------------------
    
    x["stand_clean"] = (
        x["Stand"].astype(str)
        .str.replace("-T1", "", regex=False)
        .str.strip()
    )
    x["lift_gate"] = x["stand_clean"].isin(LIFT_STANDS).astype(int)


    # -------------------------
    # SLA limits with buffer
    # -------------------------
    x["sla_limit"] = np.where(
        x["dir"] == "A",
        20 - toggles.sla_buffer_mins,
        30 - toggles.sla_buffer_mins,
    )

    # -------------------------
    # Base duration (minutes busy)
    # -------------------------
    x["base_duration_mins"] = (
        x["Job End Time"] - x["Job Start Time"]
    ).dt.total_seconds() / 60
    x["base_duration_mins"] = x["base_duration_mins"].clip(lower=0)

    # -------------------------
    # Spin indicator
    # -------------------------
    
    spin_thr = int(getattr(toggles, "spin_turnaround_threshold_mins", 60) or 60)

    # Quick turnaround based on Minutes on Chocks (S25 only)
    quick_turn = False
    if "Minutes on Chocks" in x.columns:
        quick_turn = (x["Minutes on Chocks"].fillna(9999).astype(float) <= spin_thr)

    # Arrival leg vertical exists (job-level proxy)
    arr_has_vertical = (x["dir"] == "A") & (x["needs_vertical"] == 1)

    # Departure leg vertical exists (from ingest-supplied turnaround vertical count)
    dep_has_vertical = (x.get("Turnaround Vertical Count", 0).fillna(0).astype(float) > 0)

    # Spin triggers ONLY on ARRIVAL rows (prevents double counting)
    x["is_spin"] = ( (x["dir"] == "A") & quick_turn & arr_has_vertical & dep_has_vertical ).astype(int)


    # -------------------------
    # Stable flight key (f)
    # -------------------------
    x["flight_key"] = (
        x["Airline Code"].astype(str)
        + "_"
        + x["Flight Number"].astype(str)
        + "_"
        + x["s"].astype(str)
    )

    
    jobs = x[
        [
            "Passenger ID",
            "flight_key",
            "release_time",
            "t",
            "s",
            "dir",
            "zone",
            "Stand",
            "class",
            "Airline Code",
            "needs_wc",
            "needs_vertical",
            "safety_stand",
            "lift_gate",
            "sla_limit",
            "base_duration_mins",
            "is_spin",
            "SSR numeric",
            "SSR Code",
            "IsEffectiveRemote",
            "IsArrival",
            "Turnaround PRM Count",
            "Turnaround Vertical Count",
            "Concurrent Stress",
            "PRM Flight Count",
            "Has Own Chair",
            "IsAdhoc",
            "Chocks DT",
            "Scheduled Flight DT",
            "sla_start_time",
            "hard_deadline_time",
            "tau_amb_mins",
            "tau_mini_mins",
            "tau_push_mins",
            "Minutes on Chocks",
        ]
    ].reset_index(drop=True)


    jobs.index.name = "j"
    return jobs