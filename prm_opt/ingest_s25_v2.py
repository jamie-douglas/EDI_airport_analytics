# scripts/prm_opt/ingest_s25_v2.py

from __future__ import annotations

import numpy as np
import pandas as pd

from modules.utils.query import query
from modules.utils.dates import add_date_parts, to_datetime
from modules.domain.prm.minibus import passenger_level_flags

from prm_opt.sector import normalise_sector
from prm_opt.config import NO_JETBRIDGE_AIRLINES, WCHS_OWN_CHAIR_PROB


"""
Historical S25 ingest for the V2 PRM fleet optimisation model.

Purpose
-------
Creates a passenger-level PRM master table with one row per unique
passenger-flight, but does NOT create optimisation jobs, time buckets,
release times, or service windows.

The optimiser V2 works at FLIGHT LEVEL. This file only prepares the
historical passenger-level input which build_flights_v2.py will aggregate.

Key V2 principles
-----------------
- No time buckets.
- No job/release/preposition timing.
- Keep scheduled flight time as the main anchor.
- Keep actual chocks only as optional/reporting context.
- Keep both:
    is_remote             -> pusher restriction
    is_effective_remote   -> ambulift/vertical requirement
- Preserve WCHC / WCHS / own-chair information for P100/P90 demand modes.
"""


# ---------------------------------------------------------------------
# SSR severity mapping
# ---------------------------------------------------------------------

def map_ssr(ssr_code: str) -> int:
    if ssr_code == "WCHC":
        return 3
    if ssr_code == "WCHS":
        return 2
    return 1


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _clean_flight_number(s: object) -> str:
    return str(s).strip().lstrip("0")


def _clean_stand(s: object) -> str:
    out = str(s).upper().strip()
    if out in {"", "NAN", "NONE"}:
        return ""
    out = out.replace("-T1", "")
    out = out.replace("STAND", "")
    out = out.strip()
    return out


def _make_flight_key(
    airline: object,
    flight_number: object,
    direction: object,
    scheduled_time: object,
) -> str:
    sched = pd.to_datetime(scheduled_time)
    return (
        str(airline).strip()
        + "_"
        + _clean_flight_number(flight_number)
        + "_"
        + str(direction).strip()
        + "_"
        + sched.strftime("%Y%m%d%H%M")
    )


# ---------------------------------------------------------------------
# Load PRM service records
# ---------------------------------------------------------------------

def load_prm_data(start: str, end: str) -> pd.DataFrame:
    """
    Load raw PRM service records.

    One passenger can appear multiple times because completed services
    can contain multiple service segments. We collapse to one passenger-
    flight row later.
    """

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

    df["Flight Number"] = df["Flight Number"].apply(_clean_flight_number)
    df["Airline Code"] = df["Airline Code"].astype(str).str.strip()
    df["A/D"] = df["A/D"].astype(str).str.strip()
    df["SSR Code"] = df["SSR Code"].astype(str).str.upper().str.strip()
    df["Vehicle Type"] = df["Vehicle Type"].fillna("No Vehicle")
    df["Stand"] = df["Stand"].apply(_clean_stand)

    return df


# ---------------------------------------------------------------------
# Load flight performance data
# ---------------------------------------------------------------------

def load_flight_data(start: str, end: str) -> pd.DataFrame:
    """
    Load historical flight performance data.

    V2 uses Scheduled Flight DT as the optimisation anchor. Chocks remains
    available as context/reporting if required.
    """

    df = query(
        table="EAL.FlightPerformance",
        columns=[
            "FlightID AS [Flight ID]",
            "ScheduledDateTime_Local AS [Scheduled Flight DT]",
            "ArrDeptureCode AS [A/D]",
            "FlightNumber AS [Flight Number]",
            "AirlineCode_IATA AS [Airline Code]",
            "CountryName AS [CountryName]",
            "Sector",
            "Passengers AS [Pax]",
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

    df = add_date_parts(df, col="Scheduled Flight DT", day=True)

    df["Flight Number"] = df["Flight Number"].apply(_clean_flight_number)
    df["Airline Code"] = df["Airline Code"].astype(str).str.strip()
    df["A/D"] = df["A/D"].astype(str).str.strip()
    df["Stand"] = df["Stand"].apply(_clean_stand)
    df["Sector_norm"] = df["Sector"].apply(normalise_sector)

    df["is_remote"] = pd.to_numeric(df["Remote Stand"], errors="coerce").fillna(0).astype(int)

    df["is_effective_remote"] = np.where(
        (df["is_remote"] == 1)
        | (df["Airline Code"].astype(str).isin(NO_JETBRIDGE_AIRLINES)),
        1,
        0,
    ).astype(int)

    df["flight_key"] = df.apply(
        lambda r: _make_flight_key(
            r["Airline Code"],
            r["Flight Number"],
            r["A/D"],
            r["Scheduled Flight DT"],
        ),
        axis=1,
    )

    return df


# ---------------------------------------------------------------------
# Turnaround / spin pairing for historical flights
# ---------------------------------------------------------------------

def build_turn_pairs_from_flight_data(
    df_flights: pd.DataFrame,
    spin_window_mins: int = 120,
) -> pd.DataFrame:
    """
    Build simple A/D turnaround pair information using historical
    TurnAroundFlightNumber and scheduled times.

    Output columns:
        flight_key
        turn_pair_id
        paired_flight_key
        is_spin_candidate

    Final is_spin should be calculated after demand aggregation because it
    depends on vertical demand.
    """

    flights = df_flights.copy()
    flights["turn_pair_id"] = -1
    flights["paired_flight_key"] = None
    flights["is_spin_candidate"] = 0

    arrivals = flights[flights["A/D"] == "A"].copy()
    departures = flights[flights["A/D"] == "D"].copy()

    dep_lookup = departures[
        [
            "flight_key",
            "Flight Number",
            "Airline Code",
            "Scheduled Flight DT",
        ]
    ].copy()

    pair_id = 0

    for a_idx, a in arrivals.iterrows():
        tafn = a.get("Turnaround Flight Number")
        if pd.isna(tafn):
            continue

        tafn_clean = _clean_flight_number(tafn)

        cand = dep_lookup[
            (dep_lookup["Airline Code"] == a["Airline Code"])
            & (dep_lookup["Flight Number"] == tafn_clean)
            & (dep_lookup["Scheduled Flight DT"] > a["Scheduled Flight DT"])
        ].copy()

        if len(cand) == 0:
            continue

        cand["gap_mins"] = (
            cand["Scheduled Flight DT"] - a["Scheduled Flight DT"]
        ).dt.total_seconds() / 60.0

        cand = cand[(cand["gap_mins"] > 0) & (cand["gap_mins"] <= 120)]
        if len(cand) == 0:
            continue

        d = cand.sort_values("gap_mins").iloc[0]
        d_key = d["flight_key"]
        a_key = a["flight_key"]
        gap = float(d["gap_mins"])

        flights.loc[flights["flight_key"].isin([a_key, d_key]), "turn_pair_id"] = pair_id
        flights.loc[flights["flight_key"] == a_key, "paired_flight_key"] = d_key
        flights.loc[flights["flight_key"] == d_key, "paired_flight_key"] = a_key

        if gap <= spin_window_mins:
            flights.loc[flights["flight_key"].isin([a_key, d_key]), "is_spin_candidate"] = 1

        pair_id += 1

    return flights[
        [
            "flight_key",
            "turn_pair_id",
            "paired_flight_key",
            "is_spin_candidate",
        ]
    ]


# ---------------------------------------------------------------------
# Main ingest
# ---------------------------------------------------------------------

def ingest_s25_v2(
    start: str,
    end: str,
    seed: int = 42,
    spin_window_mins: int = 120,
) -> pd.DataFrame:
    """
    Build historical passenger-level V2 PRM master.

    Returns one row per passenger-flight, with clean flight identity and
    operational flags. build_flights_v2.py then aggregates to flight level.
    """

    np.random.seed(seed)

    run_start = pd.to_datetime(start)
    run_end = pd.to_datetime(end)

    df_flights = load_flight_data(start, end)
    df_turn = build_turn_pairs_from_flight_data(
        df_flights,
        spin_window_mins=spin_window_mins,
    )

    df_prm_raw = load_prm_data(start, end)
    df_prm_raw["SSR numeric"] = df_prm_raw["SSR Code"].apply(map_ssr)

    # Collapse service segments to one passenger-flight row.
    df_prm_flags = passenger_level_flags(df_prm_raw)

    if "SSR numeric" not in df_prm_flags.columns:
        df_prm_flags["SSR numeric"] = df_prm_flags["SSR Code"].apply(map_ssr)

    rolls = np.random.rand(len(df_prm_flags))
    df_prm_flags["Has Own Chair"] = 0
    df_prm_flags.loc[df_prm_flags["SSR Code"] == "WCHC", "Has Own Chair"] = 1
    df_prm_flags.loc[
        (df_prm_flags["SSR Code"] == "WCHS")
        & (rolls < WCHS_OWN_CHAIR_PROB),
        "Has Own Chair",
    ] = 1

    agg_rules = {
        "Sector": "first",
        "Day": "first",
        "Adhoc Or Planned": "first",
        "SSR Code": "first",
        "SSR numeric": "first",
        "Has Own Chair": "max",
        "Actual Flight DT": "first",
        "Stand": "first",
    }

    group_cols = [
        "Passenger ID",
        "Airline Code",
        "Flight Number",
        "A/D",
        "Scheduled Flight DT",
    ]

    existing_agg_rules = {
        k: v
        for k, v in agg_rules.items()
        if (
            k in df_prm_flags.columns
            and k not in group_cols
        )
    }

    df_passengers = (
        df_prm_flags
        .dropna(subset=group_cols)
        .groupby(group_cols, dropna=False)
        .agg(existing_agg_rules)
        .reset_index(drop=False)
    )


    # If groupby created duplicate named columns, prefer the grouped keys.
    df_passengers = df_passengers.loc[:, ~df_passengers.columns.duplicated()]

    df_passengers["Flight Number"] = df_passengers["Flight Number"].apply(_clean_flight_number)
    df_passengers["Airline Code"] = df_passengers["Airline Code"].astype(str).str.strip()
    df_passengers["A/D"] = df_passengers["A/D"].astype(str).str.strip()
    df_passengers["Scheduled Flight DT"] = pd.to_datetime(df_passengers["Scheduled Flight DT"])

    df_passengers = df_passengers[
        (df_passengers["Scheduled Flight DT"] >= run_start)
        & (df_passengers["Scheduled Flight DT"] < run_end)
    ].copy()

    df_passengers["flight_key"] = df_passengers.apply(
        lambda r: _make_flight_key(
            r["Airline Code"],
            r["Flight Number"],
            r["A/D"],
            r["Scheduled Flight DT"],
        ),
        axis=1,
    )

    merge_cols = [
        "flight_key",
        "Flight ID",
        "CountryName",
        "Sector_norm",
        "Pax",
        "Stand",
        "Departure Gate",
        "Chocks DT",
        "Minutes on Chocks",
        "is_remote",
        "is_effective_remote",
    ]

    df_passengers = df_passengers.merge(
        df_flights[[c for c in merge_cols if c in df_flights.columns]].drop_duplicates("flight_key"),
        on="flight_key",
        how="left",
        suffixes=("", "_flight"),
    )

    # Prefer flight-performance stand if available.
    if "Stand_flight" in df_passengers.columns:
        df_passengers["Stand"] = df_passengers["Stand_flight"].combine_first(df_passengers["Stand"])
        df_passengers.drop(columns=["Stand_flight"], inplace=True)

    df_passengers = df_passengers.merge(
        df_turn,
        on="flight_key",
        how="left",
    )

    df_passengers["turn_pair_id"] = df_passengers["turn_pair_id"].fillna(-1).astype(int)
    df_passengers["is_spin_candidate"] = df_passengers["is_spin_candidate"].fillna(0).astype(int)

    df_passengers["source"] = "S25"
    df_passengers["is_forecast"] = 0

    df_passengers["Sector"] = np.where(
        df_passengers.get("Sector_norm").notna()
        if "Sector_norm" in df_passengers.columns else False,
        df_passengers.get("Sector_norm"),
        df_passengers["Sector"].apply(normalise_sector),
    )

    df_passengers["is_remote"] = df_passengers["is_remote"].fillna(0).astype(int)
    df_passengers["is_effective_remote"] = df_passengers["is_effective_remote"].fillna(0).astype(int)
    df_passengers["Has Own Chair"] = df_passengers["Has Own Chair"].fillna(0).astype(int)
    df_passengers["SSR Code"] = df_passengers["SSR Code"].fillna("OTHER").astype(str).str.upper()

    return df_passengers.reset_index(drop=True)