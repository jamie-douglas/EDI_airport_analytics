# scripts/prm_opt/ingest_s25.py

import numpy as np
import pandas as pd
from datetime import timedelta

from modules.utils.query import query
from modules.utils.dates import add_date_parts, to_datetime
from modules.domain.prm.minibus import passenger_level_flags
from prm_opt.sector import normalise_sector


from prm_opt.config import NO_JETBRIDGE_AIRLINES, WCHS_OWN_CHAIR_PROB


""" 
Ingest and prepare historical S25 PRM data.

This module constructs df_prm_master, the canonical passenger–flight–PRM dataset
used by downstream optimisation and reporting.

Responsibilities:
- Load PRM service records and flight performance data
- Clean and harmonise timestamps and identifiers
- Derive PRM service-time proxies (tau) from observed S25 data
- Construct flight-level aggregates (PRM count, vertical count)
- Build turnaround and spin indicators
- Return a dataset that matches the S26 ingest contract

This file contains NO optimisation logic.
"""


# ---------------------------------------------------------
# SSR numeric mapping (unchanged)
# ---------------------------------------------------------

# Map SSR codes to an ordinal severity scale.
# Used by Scenario 1 policy logic and legacy analytics.

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
    
    """
    Load raw PRM service records from CompletedServicesByJob.

    One row ~= one service segment performed for a passenger.
    This table is intentionally NOT yet suitable for optimisation:
    - Passengers may appear multiple times
    - Flight anchoring is incomplete
    - Service times are segment-level, not job-level
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

    
    
    # --------------------------------------------------
    # Segment-level active busy minutes (S25 empirical)
    # --------------------------------------------------
    # Each PRM job may involve multiple segments.
    # We compute observed segment durations and later aggregate
    # to typical service-time proxies by SSR + direction + vehicle.

    df_prm["segment_mins"] = (df_prm["Job End Time"] - df_prm["Job Start Time"]).dt.total_seconds() / 60.0
    df_prm["segment_mins"] = df_prm["segment_mins"].clip(lower=0)

    
    # Typical (median) busy minutes by SSR + direction + vehicle type.
    # These become the base τ inputs for optimisation (S25 only).

    veh_svc = (
        df_prm.groupby(["SSR Code", "A/D", "Vehicle Type"])["segment_mins"]
            .median()
            .reset_index()
    )

    veh_svc_wide = (
        veh_svc.pivot_table(index=["SSR Code", "A/D"], columns="Vehicle Type", values="segment_mins", aggfunc="first")
            .reset_index()
    )

    veh_svc_wide["tau_amb_mins"]  = veh_svc_wide.get("Ambulift", 0.0)
    veh_svc_wide["tau_mini_mins"] = veh_svc_wide.get("Mini Bus", 0.0)
    veh_svc_wide["tau_push_mins"] = veh_svc_wide.get("No Vehicle", 0.0)

    veh_svc_wide = veh_svc_wide[["SSR Code", "A/D", "tau_amb_mins", "tau_mini_mins", "tau_push_mins"]]


    # Passenger-level flags 
    df_prm_flags = passenger_level_flags(df_prm)

    
    # --------------------------------------------------
    # Own-chair assignment (S25 empirical rates)
    # --------------------------------------------------
    # WCHC always has own chair.
    # WCHS is sampled probabilistically using observed

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

    
    # --------------------------------------------------
    # Passenger-level aggregation
    # --------------------------------------------------
    # Collapse multiple service segments into one PRM job per passenger-flight.
    # This produces the passenger-level demand used by the optimiser.

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

    
    df_prm_grouped = df_prm_grouped.merge(
        veh_svc_wide,
        on=["SSR Code", "A/D"],
        how="left",
    )

    # fallbacks if any SSR+dir combo is missing
    df_prm_grouped["tau_amb_mins"]  = df_prm_grouped["tau_amb_mins"].fillna(30.0)
    df_prm_grouped["tau_mini_mins"] = df_prm_grouped["tau_mini_mins"].fillna(30.0)
    df_prm_grouped["tau_push_mins"] = df_prm_grouped["tau_push_mins"].fillna(30.0)


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

    


        
    
    # Merge passenger-level PRMs with flight performance.
    # Join is anchored on Flight Number + Airline + A/D + Scheduled Flight DT.
    # (Robust to repeated flight numbers across days.)

    df_prm_master = df_prm_grouped.merge(
        df_flights[
            [
                "Flight Number",
                "Airline Code",
                "A/D",
                "Scheduled Flight DT",
                "IsEffectiveRemote",
                "Concurrent Stress",
                "Minutes on Chocks",
                "PRM Flight Count",
                "Chocks DT",
                "CountryName",
            ]
        ].drop_duplicates(subset=["Flight Number", "Airline Code", "A/D", "Scheduled Flight DT"]),
        on=["Flight Number", "Airline Code", "A/D", "Scheduled Flight DT"],
        how="left",
        validate="m:1",  # optional but recommended
    )

    # ==========================================================
    # Unmatched Flight Diagnostics & Filtering
    # ----------------------------------------------------------
    # Purpose:
    # 1) Keep only PRM records within the requested run window (by Scheduled Flight DT).
    # 2) Diagnose why some PRM flight keys cannot be linked to FlightPerformance.
    # 3) Optionally drop PRM rows linked to "unresolvable" flight keys so the optimiser
    #    operates only on records anchored to flight timing (e.g., Chocks DT).
    #
    # Definitions:
    # - "Unmatched" = PRM row has no joined Chocks DT after merging to df_flights.
    # - We classify unmatched keys into:
    #   A) no_flight_candidate: no FlightPerformance row exists for the same airline+flight+A/D
    #   B) scheduled_dt_mismatch: a FlightPerformance candidate exists for airline+flight+A/D,
    #      but Scheduled Flight DT differs (often repeated flight numbers on different days)
    #   C) missing_scheduled_dt_in_prm: PRM row has no Scheduled Flight DT
    # ==========================================================

    # -------------------------
    # 0) Filter PRM records to the run window (Scheduled Flight DT)
    # -------------------------
    run_start = pd.to_datetime(start)
    run_end = pd.to_datetime(end)

    # Keep rows where Scheduled Flight DT is known and inside [start, end)
    # (This removes out-of-window scheduled flights such as 29-03-2025 when your run starts later.)
    df_prm_master = df_prm_master[
        df_prm_master["Scheduled Flight DT"].notna()
        & (df_prm_master["Scheduled Flight DT"] >= run_start)
        & (df_prm_master["Scheduled Flight DT"] < run_end)
    ].copy()

    # -------------------------
    # 1) Identify unmatched rows (join failed => missing Chocks DT)
    # -------------------------
    unmatched_rows = df_prm_master[df_prm_master["Chocks DT"].isna()].copy()
    print("\nUnmatched passenger rows after merge (missing Chocks DT):", len(unmatched_rows))

    key_cols = ["Airline Code", "Flight Number", "A/D", "Scheduled Flight DT"]
    unmatched_keys = unmatched_rows[key_cols].drop_duplicates().copy()
    print("Unique unmatched flight keys:", len(unmatched_keys))

    if len(unmatched_keys) > 0:
        # -------------------------
        # 2) Candidate search ignoring Scheduled Flight DT (diagnosis)
        # -------------------------
        cand = unmatched_keys.merge(
            df_flights[["Airline Code", "Flight Number", "A/D", "Scheduled Flight DT", "Chocks DT"]],
            on=["Airline Code", "Flight Number", "A/D"],
            how="left",
            suffixes=("_prm", "_flt"),
        )

        # If Scheduled Flight DT on the flight side is missing, there are no candidates at all
        cand["has_candidate"] = cand["Scheduled Flight DT_flt"].notna()

        # Scheduled-time delta (minutes) for candidate rows
        cand["delta_mins"] = (
            cand["Scheduled Flight DT_flt"] - cand["Scheduled Flight DT_prm"]
        ).dt.total_seconds() / 60.0
        cand["abs_delta_mins"] = cand["delta_mins"].abs()

        # Closest candidate per unmatched key (if any candidates exist)
        closest = (
            cand[cand["has_candidate"]]
            .sort_values("abs_delta_mins")
            .groupby(["Airline Code", "Flight Number", "A/D", "Scheduled Flight DT_prm"], as_index=False)
            .first()
        )

        # -------------------------
        # 3) Build a reason-coded report per unmatched key
        # -------------------------
        reason = unmatched_keys.rename(columns={"Scheduled Flight DT": "Scheduled Flight DT_prm"}).copy()
        reason["reason"] = "unknown"

        # A) missing scheduled dt in PRM (rare after window filter, but kept for safety)
        reason.loc[reason["Scheduled Flight DT_prm"].isna(), "reason"] = "missing_scheduled_dt_in_prm"

        # B) no candidates (same airline+flight+A/D not present in df_flights)
        no_cand = (
            cand[~cand["has_candidate"]][["Airline Code", "Flight Number", "A/D", "Scheduled Flight DT_prm"]]
            .drop_duplicates()
        )
        no_cand["reason"] = "no_flight_candidate"

        reason = reason.merge(
            no_cand,
            on=["Airline Code", "Flight Number", "A/D", "Scheduled Flight DT_prm"],
            how="left",
            suffixes=("", "_r"),
        )
        reason["reason"] = reason["reason_r"].combine_first(reason["reason"])
        reason.drop(columns=["reason_r"], inplace=True)

        # C) candidates exist but scheduled dt mismatch (store closest match info)
        reason = reason.merge(
            closest[[
                "Airline Code", "Flight Number", "A/D",
                "Scheduled Flight DT_prm", "Scheduled Flight DT_flt",
                "delta_mins"
            ]],
            on=["Airline Code", "Flight Number", "A/D", "Scheduled Flight DT_prm"],
            how="left",
        )

        mask_has = reason["Scheduled Flight DT_flt"].notna()
        mask_exact = mask_has & (reason["delta_mins"].abs() < 0.5)
        mask_mismatch = mask_has & ~mask_exact

        reason.loc[mask_exact, "reason"] = "exact_match_should_have_joined"
        reason.loc[mask_mismatch, "reason"] = "scheduled_dt_mismatch"

        print("\nUnmatched key reasons:")
        print(reason["reason"].value_counts(dropna=False))

        # -------------------------
        # 4) Drop categories you do not want downstream
        # -------------------------
        drop_reasons = {"no_flight_candidate", "scheduled_dt_mismatch"}

        df_prm_master = df_prm_master.merge(
            reason.rename(columns={"Scheduled Flight DT_prm": "Scheduled Flight DT"})[
                ["Airline Code", "Flight Number", "A/D", "Scheduled Flight DT", "reason"]
            ],
            on=["Airline Code", "Flight Number", "A/D", "Scheduled Flight DT"],
            how="left",
        )

        before = len(df_prm_master)
        df_prm_master = df_prm_master[
            df_prm_master["reason"].isna() | ~df_prm_master["reason"].isin(drop_reasons)
        ].copy()
        after = len(df_prm_master)

        print(f"\nDropped {before - after} passenger rows due to reasons {drop_reasons}")

        df_prm_master.drop(columns=["reason"], inplace=True)

    
    # --------------------------------------------------
    # Vertical PRM count per flight
    # --------------------------------------------------
    # Used as a flight-level proxy for vertical

    df_prm_master["VerticalCandidate"] = (
        df_prm_master["SSR Code"].isin(["WCHC", "WCHS"])
        & (df_prm_master["IsEffectiveRemote"] == 1)
    ).astype(int)

    flight_vert_count = (
        df_prm_master.groupby(["Flight Number", "Airline Code", "A/D", "Scheduled Flight DT"])["VerticalCandidate"]
        .sum()
        .reset_index(name="Vertical PRM Count")
    )

    # Merge vertical count onto df_flights so the turnaround bridge can see it
    df_flights = df_flights.merge(
        flight_vert_count,
        on=["Flight Number", "Airline Code", "A/D", "Scheduled Flight DT"],
        how="left",
    ).fillna({"Vertical PRM Count": 0})


    
    # --------------------------------------------------
    # Turnaround PRM and vertical counts
    # --------------------------------------------------
    # Link arrival and departure legs to identify short-turn ("spin") situations.

    arrivals = df_flights[df_flights["A/D"] == "A"][
        ["Flight Number", "Airline Code", "Turnaround Flight Number", "PRM Flight Count", "Vertical PRM Count", "Scheduled Flight DT"]
    ]
    departures = df_flights[df_flights["A/D"] == "D"][
        ["Flight Number", "Airline Code", "Turnaround Flight Number", "PRM Flight Count", "Vertical PRM Count", "Scheduled Flight DT"]
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
        ["Flight Number_ARR", "Scheduled Flight DT_ARR", "Airline Code",
        "PRM Flight Count_DEP", "Vertical PRM Count_DEP"]
    ].rename(columns={
        "Flight Number_ARR": "Flight Number",
        "Scheduled Flight DT_ARR": "Scheduled Flight DT",
        "PRM Flight Count_DEP": "Turnaround PRM Count",
        "Vertical PRM Count_DEP": "Turnaround Vertical Count",
    })

    lookup_D = turnaround_bridge[
        ["Flight Number_DEP", "Scheduled Flight DT_DEP", "Airline Code",
        "PRM Flight Count_ARR", "Vertical PRM Count_ARR"]
    ].rename(columns={
        "Flight Number_DEP": "Flight Number",
        "Scheduled Flight DT_DEP": "Scheduled Flight DT",
        "PRM Flight Count_ARR": "Turnaround PRM Count",
        "Vertical PRM Count_ARR": "Turnaround Vertical Count",
    })

    turnaround_lookup = pd.concat([lookup_A, lookup_D], ignore_index=True)

    
    # --------------------------------------------------
    # OUTPUT
    # --------------------------------------------------

    # df_prm_master is the canonical S25 input to build_jobs().
    # S26 ingestion is designed to replicate this schema


    df_prm_master = df_prm_master.merge(
        turnaround_lookup,
        on=["Flight Number", "Airline Code", "Scheduled Flight DT"],
        how="left",
    ).fillna({"Turnaround PRM Count": 0, "Turnaround Vertical Count": 0})


    # Final flags
    df_prm_master["IsArrival"] = (df_prm_master["A/D"] == "A").astype(int)
    df_prm_master["IsAdhoc"] = (
        df_prm_master["Adhoc Or Planned"] == "Ad-Hoc"
    ).astype(int)

    df_prm_master["Sector_norm"] = df_prm_master["Sector"].apply(normalise_sector)

    return df_prm_master
