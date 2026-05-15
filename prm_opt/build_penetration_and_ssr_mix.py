
# scripts/prm_opt/build_penetration_and_ssr_mix.py

"""
Build:
- Airline x Country penetration rate
- Airline x Country SSR mix (WCHC/WCHS/WCHR/OTHER)
- Own-chair rates per SSR (WCHC=1, WCHS=0.109, others=0)

Outputs:
- penetration_rates (DataFrame)
- ssr_mix (DataFrame)
"""

import pandas as pd

from prm_opt.ingest_s25 import ingest_s25, load_flight_data
from prm_opt.config import WCHS_OWN_CHAIR_PROB


def build_penetration_and_ssr_mix(
    s25_start: str,
    s25_end: str,
):
    """
    Returns:
      penetration_rates : Airline Code x CountryName penetration
      ssr_mix           : Airline Code x CountryName x SSR Code mix
    """

    # =====================================================
    # Load data
    # =====================================================
    df_prm = ingest_s25(s25_start, s25_end)
    df_flights = load_flight_data(s25_start, s25_end)

    # =====================================================
    # Penetration (PRM pax / total pax)
    # =====================================================
    # PRM passengers (unique people)
    prm_counts = (
        df_prm.groupby(["Airline Code", "CountryName"])["Passenger ID"]
        .nunique()
        .reset_index(name="prm_count")
    )

    # Total passengers (FLIGHT-level, no duplication)
    pax_totals = (
        df_flights.groupby(["Airline Code", "CountryName"])["Pax"]
        .sum()
        .reset_index(name="pax")
    )

    penetration = prm_counts.merge(
        pax_totals,
        on=["Airline Code", "CountryName"],
        how="left",
    ).fillna({"pax": 0})

    penetration["penetration"] = penetration.apply(
        lambda r: r.prm_count / r.pax if r.pax > 0 else 0.0,
        axis=1,
    )

    # =====================================================
    # SSR mix (within PRM passengers)
    # =====================================================
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

    def own_chair_rate(code: str) -> float:
        if code == "WCHC":
            return 1.0
        if code == "WCHS":
            return float(WCHS_OWN_CHAIR_PROB)
        return 0.0

    ssr["own_chair_rate"] = ssr["SSR Code"].apply(own_chair_rate)

    return penetration, ssr
