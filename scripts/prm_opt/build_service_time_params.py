
# scripts/prm_opt/build_service_time_params.py

"""
Build empirical service time distribution parameters from S25.

We use base_duration_mins built inside build_jobs().

Outputs:
- service_time_params.csv with (SSR Code, dir, median, std, count)
"""

import pandas as pd
from prm_opt.ingest_s25 import ingest_s25
from prm_opt.build_jobs import build_jobs

df = ingest_s25("2025-06-01", "2025-09-30")
jobs = build_jobs(df)

svc = (
    jobs.groupby(["SSR Code", "dir"])["base_duration_mins"]
    .agg(["median", "std", "count"])
    .reset_index()
)

svc.to_csv("service_time_params.csv", index=False)
