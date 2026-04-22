
import pandas as pd
from prm_opt.ingest_s25 import ingest_s25

df = ingest_s25("2025-06-01", "2025-09-30")

stand_dist = (
    df.groupby(["Airline Code", "Sector", "Stand"])
      .size()
      .reset_index(name="count")
)

stand_dist = stand_dist.merge(
    stand_dist.groupby(["Airline Code", "Sector"])["count"]
              .sum()
              .reset_index(name="total"),
    on=["Airline Code", "Sector"]
)

stand_dist["prob"] = stand_dist["count"] / stand_dist["total"]

stand_dist.to_csv("stand_fallback_distribution.csv", index=False)
