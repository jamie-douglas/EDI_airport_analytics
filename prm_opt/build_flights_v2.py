# scripts/prm_opt/build_flights_v2.py

from __future__ import annotations

import numpy as np
import pandas as pd

from prm_opt.sector import normalise_sector
from prm_opt.config import SAFETY_STANDS


"""
Build flight-level optimisation input for PRM fleet model V2.

This replaces build_jobs.py for the new model.

Input
-----
Passenger-level dataframe from ingest_s25_v2.py or ingest_s26_v2.py.

Output
------
One row per flight.

Key V2 logic
------------
- No time buckets.
- Optimisation unit is flight f, not passenger job j.
- Keeps both:
    is_remote             -> pushers cannot operate
    is_effective_remote   -> WCHC/WCHS require vertical asset
- Creates boarding_target_time = scheduled_time - boarding_offset_mins
  for departures.
- Supports:
    demand_mode="p100"
    demand_mode="p90"
- P90 is stratified/category-aware so WCHC and own-chair passengers are
  not randomly lost.
"""


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _clean_stand(s: object) -> str:
    out = str(s).upper().strip()
    if out in {"", "NAN", "NONE"}:
        return ""
    out = out.replace("-T1", "")
    return out.strip()


def _make_category(row: pd.Series) -> str:
    ssr = str(row.get("SSR Code", "OTHER")).upper().strip()
    has_own = int(row.get("Has Own Chair", 0) or 0)

    if ssr == "WCHC":
        return "WCHC_OWN"
    if ssr == "WCHS" and has_own == 1:
        return "WCHS_OWN"
    if ssr == "WCHS":
        return "WCHS_NO_OWN"
    if ssr == "WCHR":
        return "WCHR"
    return "OTHER"


def _demand_priority(row: pd.Series) -> tuple:
    """
    Used only if we have to rank rows within a stratum.
    Lower tuple sorts first.
    Keeps operationally important demand first.
    """
    ssr = str(row.get("SSR Code", "OTHER")).upper()
    has_own = int(row.get("Has Own Chair", 0) or 0)

    if ssr == "WCHC":
        severity = 0
    elif ssr == "WCHS" and has_own == 1:
        severity = 1
    elif ssr == "WCHS":
        severity = 2
    elif ssr == "WCHR":
        severity = 3
    else:
        severity = 4

    return (severity,)


# ---------------------------------------------------------------------
# P90 stratified shaping
# ---------------------------------------------------------------------

def apply_p90_stratified(
    passengers: pd.DataFrame,
    quantile: float = 0.90,
    strata_cols: tuple[str, ...] = ("demand_category", "A/D"),
    cap_floor: int = 1,
    return_caps: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    """
    Stratified P90 demand shaping.

    This caps demand within each flight and stratum, instead of capping
    total PRMs blindly.

    Why:
    - WCHC must not disappear.
    - Own-chair / wheelchair tie-down demand must not disappear.
    - WCHS vertical demand must be preserved proportionally.

    Method:
    1. Count passengers per flight per stratum.
    2. Calculate q-th percentile cap per stratum.
    3. Keep up to cap within each flight/stratum.
    4. Deterministic retaining order prioritises operational severity.

    This is the V2 equivalent of the old p90_stratified logic, but it
    occurs before flight-level aggregation.
    """

    if len(passengers) == 0:
        if return_caps:
            return passengers.copy(), pd.DataFrame()
        return passengers.copy()

    x = passengers.copy()

    x["demand_category"] = x.apply(_make_category, axis=1)

    strata_cols = tuple(c for c in strata_cols if c in x.columns)
    if len(strata_cols) == 0:
        strata_cols = ("demand_category",)

    quantile = float(quantile)
    quantile = min(max(quantile, 0.0), 1.0)

    grp_cols = ["flight_key", *strata_cols]

    counts = (
        x.groupby(grp_cols)
        .size()
        .rename("n")
        .reset_index()
    )

    caps = (
        counts
        .groupby(list(strata_cols))["n"]
        .quantile(quantile)
        .apply(lambda v: int(max(cap_floor, np.ceil(v))))
        .rename("_p90_cap")
        .reset_index()
    )

    x = x.merge(caps, on=list(strata_cols), how="left")
    x["_p90_cap"] = x["_p90_cap"].fillna(cap_floor).astype(int)

    x["_severity_rank"] = x.apply(lambda r: _demand_priority(r)[0], axis=1)

    # Stable deterministic ordering.
    sort_cols = [
        "flight_key",
        *strata_cols,
        "_severity_rank",
        "Passenger ID",
    ]
    sort_cols = [c for c in sort_cols if c in x.columns]

    x = x.sort_values(sort_cols).copy()
    x["_rank_in_stratum"] = x.groupby(grp_cols).cumcount() + 1

    out = x[x["_rank_in_stratum"] <= x["_p90_cap"]].copy()

    out.drop(
        columns=[
            c for c in ["_p90_cap", "_severity_rank", "_rank_in_stratum"]
            if c in out.columns
        ],
        inplace=True,
    )

    if return_caps:
        return out.reset_index(drop=True), caps

    return out.reset_index(drop=True)


# ---------------------------------------------------------------------
# Build flight-level table
# ---------------------------------------------------------------------

def build_flights_v2(
    df_passengers: pd.DataFrame,
    demand_mode: str = "p100",
    p90_quantile: float = 0.90,
    boarding_offset_mins: int = 40,
    p90_strata: tuple[str, ...] = ("demand_category", "A/D"),
) -> pd.DataFrame:
    """
    Convert passenger-level PRM rows into one row per flight.

    demand_mode:
        "p100" -> use all passengers
        "p90"  -> apply stratified/category-aware P90 shaping first
    """

    if df_passengers is None or len(df_passengers) == 0:
        return pd.DataFrame()

    x = df_passengers.copy()

    required = [
        "flight_key",
        "Passenger ID",
        "A/D",
        "Scheduled Flight DT",
        "SSR Code",
        "Has Own Chair",
    ]

    missing = [c for c in required if c not in x.columns]
    if missing:
        raise ValueError(f"build_flights_v2 missing required columns: {missing}")

    x["Scheduled Flight DT"] = pd.to_datetime(x["Scheduled Flight DT"])
    x["A/D"] = x["A/D"].astype(str).str.strip()
    x["SSR Code"] = x["SSR Code"].fillna("OTHER").astype(str).str.upper().str.strip()
    x["Has Own Chair"] = pd.to_numeric(x["Has Own Chair"], errors="coerce").fillna(0).astype(int)
    x["demand_category"] = x.apply(_make_category, axis=1)

    demand_mode = str(demand_mode or "p100").lower().strip()

    if demand_mode == "p90":
        x = apply_p90_stratified(
            x,
            quantile=p90_quantile,
            strata_cols=p90_strata,
            cap_floor=1,
            return_caps=False,
        )
    elif demand_mode == "p100":
        pass
    else:
        raise ValueError("demand_mode must be 'p100' or 'p90'")

    # Category indicators.
    x["_is_wchc"] = (x["SSR Code"] == "WCHC").astype(int)
    x["_is_wchs"] = (x["SSR Code"] == "WCHS").astype(int)
    x["_is_wchr"] = (x["SSR Code"] == "WCHR").astype(int)
    x["_is_other"] = (~x["SSR Code"].isin(["WCHC", "WCHS", "WCHR"])).astype(int)

    # Wheelchair-space demand:
    # WCHC and WCHS own-chair take wheelchair capacity.
    # Everyone else takes seated capacity.
    x["_is_wc"] = x["Has Own Chair"].astype(int)
    x["_is_seat"] = 1 - x["_is_wc"]

    # --------------------------------------------------
    # Vertical-demand indicators
    # --------------------------------------------------
    # Needs vertical is separate from wheelchair capacity.
    #
    # Vertical demand:
    #   Effective remote + WCHC/WCHS
    #
    # Wheelchair demand:
    #   WCHC + WCHS own-chair
    #
    # This allows:
    #   SA    = ambulift can carry all PRMs
    #   CM/CP = ambulift only handles vertical component

    if "is_effective_remote" in x.columns:
        x["_eff_remote_row"] = (
            pd.to_numeric(
                x["is_effective_remote"],
                errors="coerce",
            )
            .fillna(0)
            .astype(int)
        )
    else:
        x["_eff_remote_row"] = 0

    x["_is_vertical_prm"] = (
        (x["_eff_remote_row"] == 1)
        & (x["SSR Code"].isin(["WCHC", "WCHS"]))
    ).astype(int)

    x["_is_vert_wc"] = (
        x["_is_vertical_prm"]
        * x["_is_wc"]
    ).astype(int)

    x["_is_vert_seat"] = (
        x["_is_vertical_prm"]
        * x["_is_seat"]
    ).astype(int)

    first_cols = [
        "Flight ID",
        "Airline Code",
        "Flight Number",
        "A/D",
        "Sector",
        "CountryName",
        "Stand",
        "Scheduled Flight DT",
        "Pax",
        "is_remote",
        "is_effective_remote",
        "turn_pair_id",
        "paired_flight_key",
        "is_spin_candidate",
        "source",
        "is_forecast",
        "penetration_base",
        "penetration_uplift",
        "penetration_effective",
        "Chocks DT",
        "Minutes on Chocks",
    ]

    agg = {}

    for c in first_cols:
        if c in x.columns:
            agg[c] = "first"

    agg.update(
        {
            "Passenger ID": "nunique",
            "_is_wchc": "sum",
            "_is_wchs": "sum",
            "_is_wchr": "sum",
            "_is_other": "sum",
            "_is_wc": "sum",
            "_is_seat": "sum",
            "_is_vertical_prm": "sum",
            "_is_vert_wc": "sum",
            "_is_vert_seat": "sum",
        }
    )

    flights = (
        x.groupby("flight_key", dropna=False)
        .agg(agg)
        .reset_index()
    )

    flights.rename(
        columns={
            "Passenger ID": "P_total",
            "_is_wchc": "D_WCHC",
            "_is_wchs": "D_WCHS",
            "_is_wchr": "D_WCHR",
            "_is_other": "D_OTHER",
            "_is_wc": "D_wc",
            "_is_seat": "D_seat",
            "_is_vertical_prm": "D_vert_total",
            "_is_vert_wc": "D_vert_wc",
            "_is_vert_seat": "D_vert_seat",
            "Scheduled Flight DT": "scheduled_time",
            "A/D": "arr_dep",
            "is_remote": "Remote",
            "is_effective_remote": "EffRemote",
        },
        inplace=True,
    )

    # Defensive fills.
    for c in [
        "D_WCHC",
        "D_WCHS",
        "D_WCHR",
        "D_OTHER",
        "D_wc",
        "D_seat",
        "D_vert_total",
        "D_vert_wc",
        "D_vert_seat",
        "P_total",
    ]:
        if c in flights.columns:
            flights[c] = (
                pd.to_numeric(
                    flights[c],
                    errors="coerce",
                )
                .fillna(0)
                .astype(int)
            )
        else:
            flights[c] = 0


    for c in ["Remote", "EffRemote", "is_spin_candidate"]:
        if c in flights.columns:
            flights[c] = pd.to_numeric(flights[c], errors="coerce").fillna(0).astype(int)
        else:
            flights[c] = 0

    if "Stand" in flights.columns:
        flights["Stand"] = flights["Stand"].apply(_clean_stand)
    else:
        flights["Stand"] = ""

    if "Sector" in flights.columns:
        flights["Sector"] = flights["Sector"].apply(normalise_sector)
    else:
        flights["Sector"] = "UNK"

    flights["Arrival"] = (flights["arr_dep"] == "A").astype(int)

    # Domestic flag: use Sector if available.
    flights["Domestic"] = flights["Sector"].astype(str).str.upper().isin(
        ["DOMESTIC", "DOM", "UK", "GB"]
    ).astype(int)

    flights["Safety"] = flights["Stand"].astype(str).isin(SAFETY_STANDS).astype(int)

    # Important V2 business rule:
    # EffRemote is for WCHC/WCHS needing ambulift/vertical asset.
    flights["NeedVertical"] = (
        (flights["EffRemote"] == 1)
        & ((flights["D_WCHC"] > 0) | (flights["D_WCHS"] > 0))
    ).astype(int)

    # Remote is for pusher feasibility.
    flights["CanUsePusher"] = (1 - flights["Remote"]).astype(int)

    flights["scheduled_time"] = pd.to_datetime(flights["scheduled_time"])

    flights["boarding_target_time"] = pd.NaT
    dep_mask = flights["arr_dep"] == "D"
    flights.loc[dep_mask, "boarding_target_time"] = (
        flights.loc[dep_mask, "scheduled_time"]
        - pd.to_timedelta(int(boarding_offset_mins), unit="m")
    )

    flights["service_anchor_time"] = np.where(
        flights["arr_dep"] == "A",
        flights["scheduled_time"],
        flights["boarding_target_time"],
    )
    flights["service_anchor_time"] = pd.to_datetime(flights["service_anchor_time"])

    # --------------------------------------------------
    # SLA fields
    # --------------------------------------------------

    flights["arrival_sla_target_mins"] = 20

    flights["arrival_sla_required"] = (
        flights["arr_dep"] == "A"
    ).astype(int)

    flights["departure_boarding_required"] = (
        flights["arr_dep"] == "D"
    ).astype(int)

    # is_spin should only count the arrival side and should only activate if
    # there is vertical demand.
    flights["is_spin"] = (
        (flights["arr_dep"] == "A")
        & (flights["is_spin_candidate"] == 1)
        & (flights["NeedVertical"] == 1)
    ).astype(int)

    flights["demand_mode"] = demand_mode.upper()

    final_cols = [
        "flight_key",
        "Flight ID",
        "Airline Code",
        "Flight Number",
        "arr_dep",
        "Arrival",
        "Sector",
        "CountryName",
        "Stand",
        "scheduled_time",
        "boarding_target_time",
        "service_anchor_time",
        "Pax",
        "P_total",
        "D_WCHC",
        "D_WCHS",
        "D_WCHR",
        "D_OTHER",
        "D_wc",
        "D_seat",
        "D_vert_total",
        "D_vert_wc",
        "D_vert_seat",
        "Remote",
        "EffRemote",
        "CanUsePusher",
        "NeedVertical",
        "Domestic",
        "Safety",
        "turn_pair_id",
        "paired_flight_key",
        "is_spin_candidate",
        "is_spin",
        "source",
        "is_forecast",
        "penetration_base",
        "penetration_uplift",
        "penetration_effective",
        "demand_mode",
        "Chocks DT",
        "Minutes on Chocks",
        "arrival_sla_target_mins",
        "arrival_sla_required",
        "departure_boarding_required",
    ]

    final_cols = [c for c in final_cols if c in flights.columns]

    return flights[final_cols].reset_index(drop=True)