
# scripts/build_service_time_params.py

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
