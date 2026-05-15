
"""
Build S26 forecast assumption inputs directly from S25 historical data.

This module prepares all *non-optimisation* inputs required by the S26
ingestion and optimisation pipelines. Everything is built in-memory;
no CSVs are written.

Purpose:
---------
- Derive PRM penetration and SSR mix from historical S25 PRM data
- Derive service-time, delay, and chocks-offset parameters
- Construct robust stand assignment inputs that guarantee every future flight
  can be assigned a stand (using stand plans where available, otherwise fallback)

Key design principles:
----------------------
1) Load S25 data ONCE and reuse it everywhere
2) Use PRM-only data only where genuinely required (e.g. penetration rates)
3) Use ALL historical flights where operational coverage is required (e.g. stand fallback)
4) Ensure “sector” (Domestic / CTA / International) means the same thing everywhere
5) Do not mix ingestion, assumptions, or optimisation logic
"""

from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from pathlib import Path
import time
import pandas as pd

from modules.utils.progress import step
from prm_opt.ingest_s25 import ingest_s25, load_flight_data
from prm_opt.ingest_stand_allocations import (
    load_stand_allocations,
    build_stand_distribution,
)
from prm_opt.config import WCHS_OWN_CHAIR_PROB
from prm_opt.sector import normalise_sector


# =========================================================
# 1) Penetration rates and SSR mix (PRM-only logic)
# =========================================================
def build_penetration_and_ssr_mix(
    df_prm: pd.DataFrame,
    df_flights: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build:
      - Airline x Country PRM penetration rates
      - Airline x Country x SSR mix shares

    Data sources:
    -------------
    - df_prm     : PRM-only passenger-level data (ingest_s25)
    - df_flights : All historical flights (load_flight_data)

    Why two sources?
    ----------------
    - Penetration requires PRM counts (numerator) and total pax (denominator)
    - SSR mix is defined only within observed PRM passengers
    """

    # PRM passenger counts (unique passengers)
    prm_counts = (
        df_prm.groupby(["Airline Code", "CountryName"])["Passenger ID"]
        .nunique()
        .reset_index(name="prm_count")
    )

    # Total passengers from flight data (no duplication)
    pax_totals = (
        df_flights.groupby(["Airline Code", "CountryName"])["Pax"]
        .sum()
        .reset_index(name="pax")
    )

    # Penetration = PRM passengers / total passengers
    penetration = prm_counts.merge(
        pax_totals,
        on=["Airline Code", "CountryName"],
        how="left",
    ).fillna({"pax": 0})

    penetration["penetration"] = penetration.apply(
        lambda r: r.prm_count / r.pax if r.pax > 0 else 0.0,
        axis=1,
    )

    # SSR mix within PRM passengers
    ssr = (
        df_prm.groupby(["Airline Code", "CountryName", "SSR Code"])
        .size()
        .reset_index(name="count")
    )

    ssr = ssr.merge(
        ssr.groupby(["Airline Code", "CountryName"])["count"]
        .sum()
        .reset_index(name="total"),
        on=["Airline Code", "CountryName"],
    )

    ssr["share"] = ssr["count"] / ssr["total"]

    # Own-chair rates are fixed assumptions, exposed explicitly
    def own_chair_rate(code: str) -> float:
        if code == "WCHC":
            return 1.0
        if code == "WCHS":
            return float(WCHS_OWN_CHAIR_PROB)
        return 0.0

    ssr["own_chair_rate"] = ssr["SSR Code"].apply(own_chair_rate)

    return penetration, ssr


# =========================================================
# 2) Service time parameters (PRM-only logic)
# =========================================================
def build_service_time_params(
    df_prm: pd.DataFrame,
    *,
    bucket: str = "15min",
) -> pd.DataFrame:
    """
    Build empirical service-time parameters from S25 PRM jobs.

    Output:
      SSR Code | dir | median | std | count

    These are later used to sample job durations in S26.
    """
    from prm_opt.build_jobs import build_jobs

    jobs = build_jobs(df_prm, bucket=bucket)

    svc = (
        jobs.groupby(["SSR Code", "dir"])["base_duration_mins"]
        .agg(["median", "std", "count"])
        .reset_index()
    )
    return svc


# =========================================================
# 2b) Mode-specific tau parameters (S25-derived; PRM-only)
# =========================================================
def build_tau_mode_params(df_prm_s25: pd.DataFrame) -> pd.DataFrame:
    """
    Build median mode-specific tau from S25 by SSR Code x A/D.

    Output schema:
      SSR Code | A/D | tau_amb_mins | tau_mini_mins | tau_push_mins

    Rationale:
    - In S25 these tau_* columns are empirical (built from observed segment_mins by vehicle type).
    - In S26 we don't know the chosen mode yet, so we must attach *all three* mode taus to each job.
    - The optimiser will choose the mode; tau provides the minutes consumed IF that mode is chosen.
    """
    cols = ["tau_amb_mins", "tau_mini_mins", "tau_push_mins"]

    missing = [c for c in cols if c not in df_prm_s25.columns]
    if missing:
        raise ValueError(f"S25 df_prm_s25 missing required tau columns: {missing}")

    out = (
        df_prm_s25
        .groupby(["SSR Code", "A/D"])[cols]
        .median()
        .reset_index()
    )

    # Safety: fill any missing medians (rare) to a conservative default
    for c in cols:
        out[c] = out[c].fillna(20.0)

    return out



# =========================================================
# 3) Scheduled → Chocks offsets (ALL flights)
# =========================================================
def build_chocks_offset_params(df_flights: pd.DataFrame) -> pd.DataFrame:
    """
    Build Scheduled → Chocks offset distributions from ALL historical flights.

    These offsets represent typical early/late behaviour by:
      Airline x A/D x Sector
    """
    df = df_flights.copy()

    df["offset_mins"] = (
        df["Chocks DT"] - df["Scheduled Flight DT"]
    ).dt.total_seconds() / 60.0

    out = (
        df.groupby(["Airline Code", "A/D", "Sector"])["offset_mins"]
        .agg(mean_offset_mins="mean", std_offset_mins="std", count="count")
        .reset_index()
    )
    return out


# =========================================================
# 4) Early / late timing parameters (ALL flights)
# =========================================================
def build_early_late_params(
    df_flights: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute early/late delay statistics from ALL historical flights.

    Outputs:
      - Global mean/std
      - Airline x A/D segmented mean/std
    """
    df = df_flights.copy()

    df["delay_mins"] = (
        df["Actual Flight DT"] - df["Scheduled Flight DT"]
    ).dt.total_seconds() / 60.0

    global_stats = pd.DataFrame([{
        "key": "GLOBAL",
        "mean_delay": df["delay_mins"].mean(),
        "std_delay": df["delay_mins"].std(),
    }])

    by_airline_dir = (
        df.groupby(["Airline Code", "A/D"])["delay_mins"]
        .agg(mean_delay="mean", std_delay="std", count="count")
        .reset_index()
    )

    return global_stats, by_airline_dir


# =========================================================
# 5) Stand fallback distributions (ALL flights)
# =========================================================
def build_stand_fallback_distribution_s25(
    df_flights: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a robust stand fallback distribution from ALL S25 flights.

    Purpose:
    --------
    Guarantee that *every future flight* can be assigned a stand,
    even if no explicit stand plan exists.

    Conditioning:
      Airline Code x A/D x Sector x Stand
    """
    df = df_flights.copy()

    stand_dist = (
        df.groupby(["Airline Code", "A/D", "Sector", "Stand"])
        .size()
        .reset_index(name="count")
    )

    stand_dist = stand_dist.merge(
        stand_dist.groupby(["Airline Code", "A/D", "Sector"])["count"]
        .sum()
        .reset_index(name="total"),
        on=["Airline Code", "A/D", "Sector"],
    )

    stand_dist["prob"] = stand_dist["count"] / stand_dist["total"]
    return stand_dist


def convert_s25_stand_fallback_to_s26_schema(
    stand_fallback: pd.DataFrame,
) -> pd.DataFrame:
    """
    Convert S25 stand fallback into the schema expected by ingest_s26.

    Output schema:
      Airline | dir | sector | stand | prob
    """
    df = stand_fallback.copy()

    df["Airline"] = df["Airline Code"].astype(str)
    df["dir"] = df["A/D"]
    df["Sector"] = df["Sector"].apply(normalise_sector)

    df = df.rename(columns={"Stand": "stand"})[
        ["Airline", "dir", "Sector", "stand", "prob"]
    ]
    return df


# =========================================================
# 6) Stand input builder
# =========================================================
def build_stand_inputs(
    *,
    repo_root: Path,
    df_flights_s25: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Construct stand inputs for S26.

    Priority order:
    ---------------
    1) Exact stand plans (June / July)
    2) Empirical distributions from stand plans
    3) Fallback distributions from ALL S25 flights
    """

    stands_dir = repo_root / "data" / "stands"
    june = stands_dir / "stand_allocation-june.csv"
    july = stands_dir / "stand_allocation-july.csv"

    stand_csvs = [p for p in (june, july) if p.exists()]
    meta: Dict[str, Any] = {}

    if stand_csvs:
        stand_actuals = load_stand_allocations([str(p) for p in stand_csvs])
        stand_dist_plan = build_stand_distribution(stand_actuals)

        fallback_s25 = build_stand_fallback_distribution_s25(df_flights_s25)
        stand_dist_s25 = convert_s25_stand_fallback_to_s26_schema(fallback_s25)

        stand_dist = (
            pd.concat([stand_dist_plan, stand_dist_s25], ignore_index=True)
            .drop_duplicates(
                subset=["Airline", "dir", "Sector", "stand"],
                keep="first",
            )
        )

        meta["mode"] = "plan_plus_s25_fallback"
        return stand_actuals, stand_dist, meta

    fallback = build_stand_fallback_distribution_s25(df_flights_s25)
    stand_dist = convert_s25_stand_fallback_to_s26_schema(fallback)

    stand_actuals = pd.DataFrame(
        columns=[
            "TurnID",
            "FlightNumber",
            "ScheduledDateTime_Local",
            "Airline",
            "dir",
            "Sector",
            "stand",
        ]
    )

    meta["mode"] = "pure_s25_fallback"
    return stand_actuals, stand_dist, meta


# =========================================================
# 7) Public wrapper
# =========================================================
def build_s26_assumptions(
    *,
    s25_start: str,
    s25_end: str,
    bucket: str = "15min",
) -> Dict[str, Any]:
    """
    Build all S26 assumption inputs in a single, consistent pass.
    """

    t = time.perf_counter()

    df_prm_s25 = ingest_s25(s25_start, s25_end)
    df_flights_s25 = load_flight_data(s25_start, s25_end)

    print("[1/5] Penetration + SSR mix…")
    penetration_rates, ssr_mix = build_penetration_and_ssr_mix(
        df_prm_s25, df_flights_s25
    )
    t = step(t, "penetration + SSR mix built")

    print("[2/5] Service times…")
    service_time_params = build_service_time_params(
        df_prm_s25, bucket=bucket
    )
    t = step(t, "service times built")

    
    print("[2b/5] Tau mode params (S25 medians)…")
    tau_mode_params = build_tau_mode_params(df_prm_s25)
    t = step(t, "tau mode params built")


    print("[3/5] Chocks offsets…")
    chocks_offset_params = build_chocks_offset_params(df_flights_s25)
    t = step(t, "chocks offsets built")

    print("[4/5] Early/late timing…")
    early_late_global, early_late_by_airline_dir = build_early_late_params(df_flights_s25)
    t = step(t, "early/late params built")

    print("[5/5] Stand inputs…")
    stand_actuals, stand_dist, stand_meta = build_stand_inputs(
        repo_root=Path(__file__).resolve().parents[2],
        df_flights_s25=df_flights_s25,
    )
    t = step(t, "stand inputs built")

    return {
        "inputs": {
            "penetration_rates": penetration_rates,
            "ssr_mix": ssr_mix,
            "stand_actuals": stand_actuals,
            "stand_dist": stand_dist,
            "service_time_params": service_time_params,
            "tau_mode_params": tau_mode_params,
            "chocks_offset_params": chocks_offset_params,
        },
        "extras": {
            "early_late_global": early_late_global,
            "early_late_by_airline_dir": early_late_by_airline_dir,
            "stand_meta": stand_meta,
            "s25_window": {"start": s25_start, "end": s25_end},
            "bucket_used_for_service_time_params": bucket,
        },
    }
