# scripts/prm_opt/ingest_s25.py

"""
Faithful refactor of S25 notebook ingestion.
Builds df_prm_master identical in meaning and structure.
"""

import numpy as np
import pandas as pd
from datetime import timedelta

from modules.utils.query import query
from modules.utils.dates import add_date_parts, to_datetime
from modules.domain.prm.minibus import passenger_level_flags

from prm_opt.config import NO_JETBRIDGE_AIRLINES, WCHS_OWN_CHAIR_PROB


# ---------------------------------------------------------
# SSR numeric mapping (unchanged)
# ---------------------------------------------------------

def map_ssr(ssr_code: str) -> int:
    if ssr_code == "WCHC":
        return 3
    if ssr_code == "WCHS":
        return 2
    return 1


# ---------------------------------------------------------
# Load PRM job data
# ---------------------------------------------------------

def load_prm_data(start: str, end: str) -> pd.DataFrame:
    start_op = start.replace("-", "")
    end_op = end.replace("-", "")

    df = query(
        table="PRM.CompletedServicesByJob",
        columns=[
            "RequestID AS [Job ID]",
            "PassengerID AS [Passenger ID]",
            "FlightID AS [Flight ID]",
            "AirlineCode_IATA AS [Airline Code]",
            "FlightNumber AS [Flight Number]",
            "Sector",
            "ArrDep AS [A/D]",
            "adhocOrPlanned AS [Adhoc Or Planned]",
            "requestCreated_DAteTime_Local AS [Request Created DT]",
            "currentSSRCode AS [SSR Code]",
            "startService_DateTime_Local AS [Job Start Time]",
            "finishService_DateTime_Local AS [Job End Time]",
            "scheduledPickupLocation AS [Scheduled PU Location]",
            "scheduledDestinationLocation AS [Scheduled DO Location]",
            "actualPickupLocation AS [Actual PU Location]",
            "actualDestinationLocation AS [Actual DO Location]",
            "scheduledPickup_DateTime_Local AS [Scheduled PU DT]",
            "arriveAtLocation_DateTime_Local AS [Location Arrival DT]",
            "arrival_ActualGate_DateTime_Local AS [Plane Gate Arrival DT]",
            "ScheduledDateTime_Local AS [Scheduled Flight DT]",
            "ActualDateTime_Local AS [Actual Flight DT]",
            "VehicleShortName AS [Vehicle Model]",
            "VehicleTypeName AS [Vehicle Type]",
            "StandCode AS [Stand]",
        ],
        where=[
            "BillingPRM = 1",
            "Operation_DateID_Local >= :start_op",
            "Operation_DateID_Local < :end_op",
        ],
        params={"start_op": start_op, "end_op": end_op},
        query_option="OPTION (RECOMPILE)",
    )

    df = to_datetime(
        df,
        [
            "Request Created DT",
            "Job Start Time",
            "Job End Time",
            "Scheduled PU DT",
            "Location Arrival DT",
            "Plane Gate Arrival DT",
            "Scheduled Flight DT",
            "Actual Flight DT",
        ],
    )

    df = add_date_parts(df, col="Actual Flight DT", day=True)

    df["Flight Number"] = df["Flight Number"].astype(str).str.lstrip("0")
    df["Vehicle Type"] = df["Vehicle Type"].fillna("No Vehicle")
    df["Stand"] = df["Stand"].astype(str).str.replace(r"^[A-Za-z]+", "", regex=True)

    return df


# ---------------------------------------------------------
# Load flight data
# ---------------------------------------------------------

def load_flight_data(start: str, end: str) -> pd.DataFrame:
    df = query(
        table="EAL.FlightPerformance",
        columns=[
            "FlightID AS [Flight ID]",
            "ScheduledDateTime_Local AS [Scheduled Flight DT]",
            "ArrDeptureCode AS [A/D]",
            "FlightNumber AS [Flight Number]",
            "AirlineCode_IATA AS [Airline Code]",
            "Sector",
            "StandCode AS [Stand]",
            "DepartureGate AS [Departure Gate]",
            "ActualDateTime_Local AS [Actual Flight DT]",
            "ChocksDateTime_Local AS [Chocks DT]",
            "TurnAroundFlightNumber AS [Turnaround Flight Number]",
            "TurnAround_ScheduledDateTime_Local AS [Turnaround Scheduled DT]",
            "MinutesOnStand_Chocks AS [Minutes on Chocks]",
            "RemoteStand AS [Remote Stand]",
        ],
        where=[
            "ScheduledDateTime_Local >= :start",
            "ScheduledDateTime_Local < :end",
        ],
        params={"start": start, "end": end},
        query_option="OPTION (RECOMPILE)",
    )

    df = to_datetime(
        df,
        [
            "Scheduled Flight DT",
            "Actual Flight DT",
            "Chocks DT",
            "Turnaround Scheduled DT",
        ],
    )

    df = add_date_parts(df, col="Actual Flight DT", day=True)
    df["Flight Number"] = df["Flight Number"].astype(str).str.lstrip("0")
    df["Stand"] = df["Stand"].astype(str).str.replace(r"^[A-Za-z]+", "", regex=True)

    return df


# ---------------------------------------------------------
# Build df_prm_master (core S25 artefact)
# ---------------------------------------------------------

def ingest_s25(start: str, end: str, seed: int = 42) -> pd.DataFrame:
    # Flights
    df_flights = load_flight_data(start, end)
    df_flights = df_flights.sort_values("Chocks DT").reset_index(drop=True)

    df_flights["IsEffectiveRemote"] = np.where(
        (df_flights["Remote Stand"] == 1)
        | (df_flights["Airline Code"].isin(NO_JETBRIDGE_AIRLINES)),
        1,
        0,
    )

    # Concurrent stress (+/- 30 mins)
    def rolling_stress(row):
        start_window = row["Chocks DT"] - timedelta(minutes=30)
        end_window = row["Chocks DT"] + timedelta(minutes=30)
        return (
            len(
                df_flights[
                    (df_flights["Chocks DT"] >= start_window)
                    & (df_flights["Chocks DT"] <= end_window)
                ]
            )
            - 1
        )

    df_flights["Concurrent Stress"] = df_flights.apply(
        lambda r: rolling_stress(r), axis=1
    )

    # PRM jobs
    df_prm = load_prm_data(start, end)
    df_prm["SSR numeric"] = df_prm["SSR Code"].apply(map_ssr)

    # Passenger-level flags 
    df_prm_flags = passenger_level_flags(df_prm)

    # Own-chair assignment
    np.random.seed(seed)
    rolls = np.random.rand(len(df_prm_flags))

    df_prm_flags["Has Own Chair"] = 0
    df_prm_flags.loc[df_prm_flags["SSR numeric"] == 3, "Has Own Chair"] = 1
    df_prm_flags.loc[
        (df_prm_flags["SSR numeric"] == 2)
        & (rolls < WCHS_OWN_CHAIR_PROB),
        "Has Own Chair",
    ] = 1

    # Strategic location
    df_prm_flags["Strategic Location"] = np.where(
        df_prm_flags["Sector"] == "A",
        df_prm_flags["Actual PU Location"],
        df_prm_flags["Actual DO Location"],
    )

    # Passenger aggregation
    agg_rules = {
        "Sector": "first",
        "A/D": "first",
        "Day": "first",
        "Adhoc Or Planned": "first",
        "SSR Code": "first",
        "SSR numeric": "first",
        "Has Own Chair": "max",
        "Job Start Time": "min",
        "Job End Time": "max",
        "Strategic Location": "first",
        "Location Arrival DT": "min",
        "Plane Gate Arrival DT": "first",
        "Scheduled Flight DT": "first",
        "Stand": "first",
        "PassengerType": "first",
    }

    df_prm_grouped = (
        df_prm_flags.groupby(["Passenger ID", "Airline Code", "Flight Number"])
        .agg(agg_rules)
        .reset_index()
    )

    # PRM Flight Count
    flight_prm_count = (
        df_prm_flags.groupby(["Flight Number", "Airline Code", "Day"])[
            "Passenger ID"
        ]
        .nunique()
        .reset_index(name="PRM Flight Count")
    )

    df_flights = df_flights.merge(
        flight_prm_count,
        on=["Flight Number", "Airline Code", "Day"],
        how="left",
    ).fillna({"PRM Flight Count": 0})

    # Merge passenger + flight
    df_prm_master = df_prm_grouped.merge(
        df_flights[
            [
                "Flight Number",
                "Airline Code",
                "Day",
                "IsEffectiveRemote",
                "Concurrent Stress",
                "Minutes on Chocks",
                "PRM Flight Count",
                "Scheduled Flight DT",
            ]
        ],
        on=["Flight Number", "Airline Code", "Day"],
        how="left",
    )

    # Turnaround PRM Count (faithful logic)
    arrivals = df_flights[df_flights["A/D"] == "A"][
        ["Flight Number", "Airline Code", "Turnaround Flight Number", "PRM Flight Count", "Scheduled Flight DT"]
    ]
    departures = df_flights[df_flights["A/D"] == "D"][
        ["Flight Number", "Airline Code", "Turnaround Flight Number", "PRM Flight Count", "Scheduled Flight DT"]
    ]

    turnaround_bridge = arrivals.merge(
        departures,
        left_on=["Turnaround Flight Number", "Airline Code"],
        right_on=["Flight Number", "Airline Code"],
        suffixes=("_ARR", "_DEP"),
        how="inner",
    )

    turnaround_bridge["Gap"] = (
        turnaround_bridge["Scheduled Flight DT_DEP"]
        - turnaround_bridge["Scheduled Flight DT_ARR"]
    ).dt.total_seconds() / 3600

    turnaround_bridge = turnaround_bridge[
        (turnaround_bridge["Gap"] > 0.5)
        & (turnaround_bridge["Gap"] < 4)
    ]

    lookup_A = turnaround_bridge[
        ["Flight Number_ARR", "Scheduled Flight DT_ARR", "Airline Code", "PRM Flight Count_ARR"]
    ].rename(
        columns={
            "Flight Number_ARR": "Flight Number",
            "Scheduled Flight DT_ARR": "Scheduled Flight DT",
            "PRM Flight Count_ARR": "Turnaround PRM Count",
        }
    )

    lookup_D = turnaround_bridge[
        ["Flight Number_DEP", "Scheduled Flight DT_DEP", "Airline Code", "PRM Flight Count_DEP"]
    ].rename(
        columns={
            "Flight Number_DEP": "Flight Number",
            "Scheduled Flight DT_DEP": "Scheduled Flight DT",
            "PRM Flight Count_DEP": "Turnaround PRM Count",
        }
    )

    turnaround_lookup = pd.concat([lookup_A, lookup_D])

    df_prm_master = df_prm_master.merge(
        turnaround_lookup,
        on=["Flight Number", "Airline Code", "Scheduled Flight DT"],
        how="left",
    ).fillna({"Turnaround PRM Count": 0})

    # Final flags
    df_prm_master["IsArrival"] = (df_prm_master["A/D"] == "A").astype(int)
    df_prm_master["IsAdhoc"] = (
        df_prm_master["Adhoc Or Planned"] == "Ad-Hoc"
    ).astype(int)

    return df_prm_master
