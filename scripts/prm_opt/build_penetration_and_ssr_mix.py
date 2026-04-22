
import pandas as pd
from prm_opt.ingest_s25 import ingest_s25

df = ingest_s25("2025-06-01", "2025-09-30")

# -----------------------------
# Total penetration
# -----------------------------
penetration = (
    df.groupby(["Airline Code", "CountryName"])
      .agg(
          prm_count=("Passenger ID", "count"),
          pax=("TotalPassengers", "sum")
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
    on=["Airline Code", "CountryName"]
)

ssr["share"] = ssr["count"] / ssr["total"]

# Own‑chair logic
def own_chair_rate(ssr):
    if ssr == "WCHC":
        return 1.0
    if ssr == "WCHS":
        return 0.109
    return 0.0

ssr["own_chair_rate"] = ssr["SSR Code"].apply(own_chair_rate)
