
# prm_opt/ingest_s26.py

import sys
import pathlib
from pathlib import Path

# Add parent directory to path so custom modules can be imported
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))


import numpy as np
import pandas as pd
from datetime import timedelta

from modules.utils.query import query
from modules.analytics.grouping import group_sum
from prm_opt.config import STAND_ZONES, WCHS_OWN_CHAIR_PROB, NO_JETBRIDGE_AIRLINES
from prm_opt.sector import normalise_sector


def load_future_flights(start: str, end: str) -> pd.DataFrame:
    """
    Load future flight schedule from EAL.FlightPerformance_FutureFlights.
    """
    df = query(
        table="EAL.FlightPerformance_FutureFlights",
        columns=[
            "FlightID",
            "ScheduledDateTime_Local",
            "ArrDeptureCode",
            "FlightNumber",
            "AirlineCode_IATA",
            "CountryName",
            "Sector",
            "Pax_MostConfident",
            "PublishedForecast_Pax",
        ],
        where=[
            "ScheduledDateTime_Local >= :start",
            "ScheduledDateTime_Local < :end",
            "IsPassengerFlight = 1",
        ],
        params={"start": start, "end": end},
        query_option="OPTION (RECOMPILE)",
    )
    return df

flights_df = load_future_flights(start="2026-06-28", end="2026-07-31")
flights_df["Date"] = flights_df["ScheduledDateTime_Local"].dt.date

grouped = group_sum(flights_df, by_cols=["Date"], value_col="PublishedForecast_Pax", out_col="Total_Pax")

print(grouped)