from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Tuple

import pandas as pd

from modules.utils.progress import step

from prm_opt.ingest_s25_v2 import ingest_s25_v2
from prm_opt.ingest_s25 import load_flight_data

from prm_opt.ingest_stand_allocations import (
    load_stand_allocations,
    build_stand_distribution,
)

from prm_opt.config import WCHS_OWN_CHAIR_PROB
from prm_opt.sector import normalise_sector


# =========================================================
# Penetration + SSR mix
# =========================================================

def build_penetration_and_ssr_mix(
    df_prm: pd.DataFrame,
    df_flights: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    prm_counts = (
        df_prm.groupby(
            ["Airline Code", "CountryName"]
        )["Passenger ID"]
        .nunique()
        .reset_index(name="prm_count")
    )

    pax_totals = (
        df_flights.groupby(
            ["Airline Code", "CountryName"]
        )["Pax"]
        .sum()
        .reset_index(name="pax")
    )

    penetration = prm_counts.merge(
        pax_totals,
        on=["Airline Code", "CountryName"],
        how="left",
    ).fillna({"pax": 0})

    penetration["penetration"] = penetration.apply(
        lambda r:
        r.prm_count / r.pax
        if r.pax > 0
        else 0.0,
        axis=1,
    )

    ssr = (
        df_prm.groupby(
            [
                "Airline Code",
                "CountryName",
                "SSR Code",
            ]
        )
        .size()
        .reset_index(name="count")
    )

    ssr = ssr.merge(
        ssr.groupby(
            ["Airline Code", "CountryName"]
        )["count"]
        .sum()
        .reset_index(name="total"),
        on=["Airline Code", "CountryName"],
    )

    ssr["share"] = (
        ssr["count"] / ssr["total"]
    )

    def own_chair_rate(code):

        if code == "WCHC":
            return 1.0

        if code == "WCHS":
            return float(
                WCHS_OWN_CHAIR_PROB
            )

        return 0.0

    ssr["own_chair_rate"] = (
        ssr["SSR Code"]
        .apply(own_chair_rate)
    )

    return penetration, ssr


# =========================================================
# Tau parameters
# =========================================================

def build_tau_mode_params(
    df_prm: pd.DataFrame,
    config=None,
) -> Dict[str, Any]:

    old_cols = [
        "tau_amb_mins",
        "tau_mini_mins",
        "tau_push_mins",
    ]

    missing = [
        c
        for c in old_cols
        if c not in df_prm.columns
    ]

    # V2 no longer requires passenger-level tau columns.
    # The optimiser uses OptimiserConfig directly:
    #   tau_amb_solo_mins
    #   tau_amb_comb_mins
    #   tau_mini_mins
    #   tau_push_mins
    #
    # Therefore if old tau columns are not present, fall back to config
    # instead of failing.
    if missing:

        if config is None:
            return {
                "by_ssr": pd.DataFrame(),
                "global": {},
            }

        return {
            "by_ssr": pd.DataFrame(),
            "global": {
                "tau_amb_solo_mins": float(config.tau_amb_solo_mins),
                "tau_amb_comb_mins": float(config.tau_amb_comb_mins),
                "tau_mini_mins": float(config.tau_mini_mins),
                "tau_push_mins": float(config.tau_push_mins),
            },
        }

    by_ssr = (
        df_prm.groupby(
            ["SSR Code", "A/D"]
        )[old_cols]
        .median()
        .reset_index()
    )

    global_tau = {
        "tau_amb_mins": float(df_prm["tau_amb_mins"].median()),
        "tau_mini_mins": float(df_prm["tau_mini_mins"].median()),
        "tau_push_mins": float(df_prm["tau_push_mins"].median()),
    }

    return {
        "by_ssr": by_ssr,
        "global": global_tau,
    }


# =========================================================
# Stand fallback
# =========================================================

def build_stand_fallback_distribution_s25(
    df_flights: pd.DataFrame,
) -> pd.DataFrame:

    stand_dist = (
        df_flights.groupby(
            [
                "Airline Code",
                "A/D",
                "Sector",
                "Stand",
            ]
        )
        .size()
        .reset_index(name="count")
    )

    stand_dist = stand_dist.merge(
        stand_dist.groupby(
            [
                "Airline Code",
                "A/D",
                "Sector",
            ]
        )["count"]
        .sum()
        .reset_index(name="total"),
        on=[
            "Airline Code",
            "A/D",
            "Sector",
        ],
    )

    stand_dist["prob"] = (
        stand_dist["count"]
        / stand_dist["total"]
    )

    return stand_dist


def convert_s25_stand_fallback_to_s26_schema(
    stand_fallback: pd.DataFrame,
) -> pd.DataFrame:

    df = stand_fallback.copy()

    df["Airline"] = (
        df["Airline Code"]
        .astype(str)
    )

    df["dir"] = df["A/D"]

    df["Sector"] = (
        df["Sector"]
        .apply(normalise_sector)
    )

    return (
        df.rename(
            columns={"Stand": "stand"}
        )[[
            "Airline",
            "dir",
            "Sector",
            "stand",
            "prob",
        ]]
    )


# =========================================================
# Stand inputs
# =========================================================

def build_stand_inputs(
    *,
    repo_root: Path,
    df_flights_s25: pd.DataFrame,
    stand_plan_files= None,
):

    stands_dir = (
        repo_root
        / "data"
        / "stands"
    )

    if stand_plan_files is not None:

        stand_csvs = [
            Path(p)
            for p in stand_plan_files
            if Path(p).exists()
        ]

    else:

        june = (
            stands_dir
            / "stand_allocation-june.csv"
        )

        july = (
            stands_dir
            / "stand_allocation-july.csv"
        )

        stand_csvs = [
            p
            for p in [june, july]
            if p.exists()
        ]

    if stand_csvs:

        stand_actuals = (
            load_stand_allocations(
                [str(p) for p in stand_csvs]
            )
        )

        stand_dist_plan = (
            build_stand_distribution(
                stand_actuals
            )
        )

        fallback = (
            build_stand_fallback_distribution_s25(
                df_flights_s25
            )
        )

        stand_dist_s25 = (
            convert_s25_stand_fallback_to_s26_schema(
                fallback
            )
        )

        stand_dist = (
            pd.concat(
                [
                    stand_dist_plan,
                    stand_dist_s25,
                ],
                ignore_index=True,
            )
            .drop_duplicates(
                subset=[
                    "Airline",
                    "dir",
                    "Sector",
                    "stand",
                ],
                keep="first",
            )
        )

        return (
            stand_actuals,
            stand_dist,
        )

    fallback = (
        build_stand_fallback_distribution_s25(
            df_flights_s25
        )
    )

    stand_dist = (
        convert_s25_stand_fallback_to_s26_schema(
            fallback
        )
    )

    stand_actuals = pd.DataFrame()

    return (
        stand_actuals,
        stand_dist,
    )


# =========================================================
# Public
# =========================================================

def build_assumptions_v2(
    *,
    s25_start: str,
    s25_end: str,
    config=None,
    stand_plan_files=None,
):

    df_prm = ingest_s25_v2(
        s25_start,
        s25_end,
    )

    df_flights = load_flight_data(
        s25_start,
        s25_end,
    )

    penetration_rates, ssr_mix = (
        build_penetration_and_ssr_mix(
            df_prm,
            df_flights,
        )
    )

    tau_mode_params = (
        build_tau_mode_params(
            df_prm,
            config=config,
        )
    )

    stand_actuals, stand_dist = (
        build_stand_inputs(
            repo_root=Path(__file__).resolve().parents[2],
            df_flights_s25=df_flights,
            stand_plan_files=stand_plan_files,
        )
    )

    return {
        "penetration_rates": penetration_rates,
        "ssr_mix": ssr_mix,
        "tau_mode_params": tau_mode_params,
        "stand_actuals": stand_actuals,
        "stand_dist": stand_dist,
    }