
"""
Build Scheduled -> Chocks offset parameters from S25.

Outputs:
- chocks_offset_params.csv with:
    Airline Code, A/D, class, mean_offset_mins, std_offset_mins, count
"""

import pandas as pd
from prm_opt.ingest_s25 import load_flight_data

df = load_flight_data("2025-06-01", "2025-09-30")

df["offset_mins"] = (
    df["Chocks DT"] - df["Scheduled Flight DT"]
).dt.total_seconds() / 60.0

out = (
    df.groupby(["Airline Code", "A/D", "class"])["offset_mins"]
      .agg(mean_offset_mins="mean", std_offset_mins="std", count="count")
      .reset_index()
)

out.to_csv("chocks_offset_params.csv", index=False)
