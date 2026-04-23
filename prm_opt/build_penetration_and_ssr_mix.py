
# scripts/prm_opt/build_penetration_and_ssr_mix.py

"""
Build:
- Airline x Country penetration rate
- Airline x Country SSR mix (WCHC/WCHS/WCHR/OTHER)
- Own-chair rates per SSR (WCHC=1, WCHS=0.109, others=0)

Outputs:
- penetration_rates.csv
- ssr_mix_by_airline_country.csv
"""

import pandas as pd
from prm_opt.ingest_s25 import ingest_s25


df = ingest_s25("2025-06-01", "2025-09-30")

# -----------------------------
# Total penetration
# -----------------------------
# Requires ingest_s25 to include:
#   - Airline Code
#   - CountryName
#   - Passenger ID
#   - TotalPassengers (or Passengers mapped into TotalPassengers)
penetration = (
    df.groupby(["Airline Code", "CountryName"])
    .agg(
        prm_count=("Passenger ID", "count"),
        pax=("TotalPassengers", "sum"),
    )
    .assign(penetration=lambda x: x.prm_count / x.pax)
    .reset_index()
)

penetration.to_csv("penetration_rates.csv", index=False)

# -----------------------------
# SSR mix
# -----------------------------
ssr = (
    df.groupby(["Airline Code", "CountryName", "SSR Code"])
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
        return 0.109
    return 0.0


ssr["own_chair_rate"] = ssr["SSR Code"].apply(own_chair_rate)

ssr.to_csv("ssr_mix_by_airline_country.csv", index=False)
