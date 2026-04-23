# scripts/prm_opt/build_early_late_params.py

"""
Compute early/late timing stochastic parameters from S25.

We define:
  delay_mins = Actual Flight DT - Scheduled Flight DT

Outputs:
- early_late_params.csv (global mean/std + by airline + by direction)
"""

import pandas as pd
from prm_opt.ingest_s25 import load_flight_data

df = load_flight_data("2025-06-01", "2025-09-30")

df["delay_mins"] = (
    df["Actual Flight DT"] - df["Scheduled Flight DT"]
).dt.total_seconds() / 60.0

# Global stats
global_stats = pd.DataFrame([{
    "key": "GLOBAL",
    "mean_delay": df["delay_mins"].mean(),
    "std_delay": df["delay_mins"].std(),
}])

# Optional: by airline + direction
by_airline_dir = (
    df.groupby(["Airline Code", "A/D"])["delay_mins"]
    .agg(mean_delay="mean", std_delay="std", count="count")
    .reset_index()
)

global_stats.to_csv("early_late_params_global.csv", index=False)
by_airline_dir.to_csv("early_late_params_by_airline_dir.csv", index=False)
