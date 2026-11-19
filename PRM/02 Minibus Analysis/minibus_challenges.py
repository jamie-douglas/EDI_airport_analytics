import sys
import pathlib
from pathlib import Path

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import time
import pandas as pd
import numpy as np

from modules.utils.progress import step
from modules.domain.prm.minibus import passenger_level_flags
from modules.domain.prm.challenges import (
    prepare_prm_flight_summary,
    merge_prm_and_flights,
    challenge_summary,
)
from modules.domain.prm.efficiency import get_secondarySSR_count_penrate

from modules.utils.dates import to_datetime, add_date_parts
from modules.utils.query import query
from modules.config import JETBRIDGE_STANDS

# =====================================================================
# Loaders
# =====================================================================

def load_prm_data(start: str, end: str) -> pd.DataFrame:
    """
    Load PRM Completed Services jobs.
    """

    start_op = start.replace("-", "")
    end_op   = end.replace("-", "")

    df = query(
        table="PRM.CompletedServicesByJob",
        columns=[
            "RequestID AS [Job ID]",
            "PassengerID AS [Passenger ID]",
            "FlightID AS [Flight ID]",
            "AirlineCode_IATA AS [Airline Code]",
            "FlightNumber AS [Flight Number]",
            "ArrDep AS [A/D]",
            "currentSSRCode AS [SSR Code]",
            "startService_DateTime_Local AS [Job Start Time]",
            "finishService_DateTime_Local AS [Job End Time]",
            "disregardCode AS [Disregard Code]",
            "EmployeeName AS [Employee]",
            "VehicleShortName AS [Vehicle Model]",
            "VehicleTypeName AS [Vehicle Type]",
            "StandCode AS [Stand]",
            "ScheduledDateTime_Local AS [Scheduled DateTime]"
        ],
        where=[
            "BillingPRM = 1",
            "Operation_DateID_Local >= :start_op",
            "Operation_DateID_Local < :end_op",
        ],
        params={"start_op": start_op, "end_op": end_op},
        query_option="OPTION (RECOMPILE)",
    )

    df = to_datetime(df, ["Job Start Time", "Job End Time", "Scheduled DateTime"])
    df = add_date_parts(df, col="Scheduled DateTime", day=True)

    # Normalise flight number
    df["Flight Number"] = df["Flight Number"].astype(str).str.lstrip("0")

    return df


def load_flight_data(start: str, end: str) -> pd.DataFrame:
    """
    Load flight data from EAL.FlightPerformance for merge.
    """

    df = query(
        table="EAL.FlightPerformance",
        columns=[
            "FlightID AS [Flight ID]",
            "ScheduledDateTime_Local AS [Scheduled DateTime]",
            "FlightNumber AS [Flight Number]",
            "AirlineCode_IATA AS [Airline Code]",
            "Sector",
            "StandCode AS [Stand]",
            "DepartureGate AS [Departure Gate]",
            "RemoteStand AS [Remote Stand]",
            "IsPassengerFlight"
        ],
        where=[
            "ScheduledDateTime_Local >= :start",
            "ScheduledDateTime_Local < :end",
        ],
        params={"start": start, "end": end},
        query_option="OPTION (RECOMPILE)",
    )

    df = to_datetime(df, ["Scheduled DateTime"])
    df = add_date_parts(df, col="Scheduled DateTime", day=True)

    df["Flight Number"] = df["Flight Number"].astype(str).str.lstrip("0")

    return df

def load_future_flight_data(start: str, end: str) -> pd.DataFrame:
    """ 
    Load Future Flight Data from defined period
    
    Parameters
    ----------
    start: str
        Start of window (ISO format)
    end: str
        End of window (ISO format)
    
     Returns
    ----------
    pd.DataFrame
        Dataframe of Future Flight Data
    """

    df = query(
        table="EAL.FlightPerformance_FutureFlights",
        columns = [
            "FlightID AS [Flight ID]",
            "ScheduledDateTime_Local AS [Scheduled DateTime]",
            "FlightNumber AS [Flight Number]",
            "AirlineCode_IATA AS [Airline Code]",
            "Sector",
            "IsPassengerFlight"

        ],
       where=[
            "ScheduledDateTime_Local >= :start",
            "ScheduledDateTime_Local < :end",
        ],
        params={"start": start, "end": end},
        query_option="OPTION (RECOMPILE)",
    )

    df = to_datetime(df, ["Scheduled DateTime"])
    df = add_date_parts(df, col="Scheduled DateTime", day=True)

    df["Flight Number"] = df["Flight Number"].astype(str).str.lstrip("0")

    return df

def load_avtech_data(start: str, end: str):
    ecac_arrivals = pd.read_excel(
        io="inputs/ECAC Arrivals 2025.xlsx", 
        sheet_name="AvTech ECAC Arrivals", 
        header=0, 
        usecols=["Source.Name", "Date", "PassengerID", "Primary PRM", "PRM Secondary String", "Airline Code", "Flt Number"]
     )
    
    ecac_departures = pd.read_excel(
        io="inputs/ECAC Departures 2025.xlsx", 
        sheet_name="AvTech ECAC Departures", 
        header=0, 
        usecols=["Source.Name", "Date", "PassengerID", "Primary PRM", "PRM Secondary String", "Airline Code", "Flt Number"]
    )

    ecac_S25 = pd.concat([ecac_arrivals, ecac_departures], axis=0, ignore_index = True)

    ecac_S25 = ecac_S25[(ecac_S25["Date"]>=start) & (ecac_S25["Date"] < end)]


    return ecac_S25


# =====================================================================
# Main Script
# =====================================================================

if __name__ == "__main__":

    start = "2025-03-30"
    end   = "2025-10-26"

    t0 = time.perf_counter()

    # ------------------------------------------------------------
    # 1. Load PRM Jobs
    # ------------------------------------------------------------
    print("[1/7] Loading PRM jobs…")
    prm_df = load_prm_data(start, end)
    t1 = step(t0, f"Loaded PRM rows: {len(prm_df):,}")

    # ------------------------------------------------------------
    # 2. Apply Passenger Flags
    # ------------------------------------------------------------
    print("[2/7] Applying Passenger Flags…")
    prm_df = passenger_level_flags(prm_df)
    t2 = step(t1, f"Passenger Flags applied: {len(prm_df):,}")

    # ------------------------------------------------------------
    # 3. Prepare flight-level PRM summary
    # ------------------------------------------------------------
    print("[3/7] Summarising PRM at flight level…")
    prm_summary = prepare_prm_flight_summary(prm_df)
    t3 = step(t2, f"Flight-level PRM summary created: {len(prm_summary):,}")

    # ------------------------------------------------------------
    # 4. Load flight schedule
    # ------------------------------------------------------------
    print("[4/7] Loading flight schedule (historic)…")
    flights_df = load_flight_data(start, end)
    t4 = step(t3, f"Flight schedule loaded: {len(flights_df):,}")

    # ------------------------------------------------------------
    # 5. Merge PRM summary with flight schedule
    # ------------------------------------------------------------
    print("[5/7] Merging PRM summary with flight schedule…")
    merged_df = merge_prm_and_flights(prm_summary, flights_df)
    t5 = step(t4, f"Merged dataset ready: {len(merged_df):,}")

    # ------------------------------------------------------------
    # 6. Load & prepare forecast data
    # ------------------------------------------------------------
    print("[6/7] Loading forecast flight schedule…")
    forecast_df = load_future_flight_data(start="2026-03-24", end="2026-10-24")
    t6 = step(t5, f"Forecast flights loaded: {len(forecast_df):,}")

    # ------------------------------------------------------------
    # 6. Load avtech ECAC detail data
    # ------------------------------------------------------------
    print("[7/8] Loading forecast ECAC data…")
    ecac_df = load_avtech_data(start, end)
    t6 = step(t5, f"Forecast flights loaded: {len(ecac_df):,}")
    
    # ------------------------------------------------------------
    # CHALLENGE #1 — United flights at Stand 10
    # ------------------------------------------------------------
    print("[8/8] Loading challenge specific information…")


    def ch_united_stand10(df):
        """
        Challenge #1:
        United Airlines (UA) flights occurring at Stand 10.
        """
        return (df["Airline Code"] == "UA") & (df["Stand"] == "10")


    mask_united_stand10 = ch_united_stand10(merged_df)

    summary_united_stand10 = challenge_summary(
        df_merged=merged_df,
        forecast_df=forecast_df,
        challenge_mask=mask_united_stand10
    )

    t7 = step(t6, f"\n=== CHALLENGE #1 — UNITED AT STAND 10 ===")
    for k, v in summary_united_stand10.items():
        print(f"{k}: {v}")


    # ------------------------------------------------------------
    # CHALLENGE #2 — Non‑Ryanair flights at constrained stands
    #                     (15A / 15B / 17)
    # ------------------------------------------------------------

    def ch_non_ryanair_at_constrained_stands(df):
        """
        Challenge #2:
        Flights operating at stands 15A, 15B, or 17
        that are NOT Ryanair operated (FR / RK).
        """
        return (
            (~df["Airline Code"].isin(["FR", "RK"])) &
            (df["Stand"].isin(["15A", "15B", "17"]))
        )


    mask_non_ryanair_stands = ch_non_ryanair_at_constrained_stands(merged_df)

    summary_non_ryanair_stands = challenge_summary(
        df_merged=merged_df,
        forecast_df=forecast_df,
        challenge_mask=mask_non_ryanair_stands
    )

    t8 = step(t7, f"\n=== CHALLENGE #2 — NON‑RYANAIR FLIGHTS AT STANDS 15A / 15B / 17 ===")
    for k, v in summary_non_ryanair_stands.items():
        print(f"{k}: {v}")

    
    # ------------------------------------------------------------
    # CHALLENGE #3 — Departure Gate 7 or 8 with >1 wheelchair PRM
    #                     (WCHC / WCHS / WCHR)
    # ------------------------------------------------------------

    def ch_gate_7_8_multiple_wheelchairs(df):
        """
        Challenge #3:
        Flights operating at Departure Gate 7 or 8
        with more than one wheelchair PRM
        (WCHC + WCHS + WCHR > 1).
        """
        return (
            (df["Departure Gate"].isin(["7", "8"])) &
            (
                df["WCHC Count"]
                + df["WCHS Count"]
                + df["WCHR Count"]
                > 1
            )
        )


    mask_gate_7_8_multi_wc = ch_gate_7_8_multiple_wheelchairs(merged_df)

    summary_gate_7_8_multi_wc = challenge_summary(
        df_merged=merged_df,
        forecast_df=forecast_df,
        challenge_mask=mask_gate_7_8_multi_wc
    )

    t9 = step(
        t8,
        f"\n=== CHALLENGE #3 — GATE 7 / 8 WITH >1 WHEELCHAIR PRM ==="
    )
    for k, v in summary_gate_7_8_multi_wc.items():
        print(f"{k}: {v}")

    
    # ------------------------------------------------------------
    # CHALLENGE #4 — Domestic flights with WCHS or WCHC
    #                     (WCHR irrelevant)
    # ------------------------------------------------------------

    def ch_domestic_wchs_or_wchc(df):
        """
        Challenge #4:
        Domestic flights with at least one WCHS or WCHC passenger.
        WCHR passengers may be present but are not relevant to this challenge.
        """
        return (
            (df["Sector"] == "Domestic") &
            ((df["WCHC Count"] + df["WCHS Count"]) >= 1)
        )


    mask_domestic_wchs_wchc = ch_domestic_wchs_or_wchc(merged_df)

    summary_domestic_wchs_wchc = challenge_summary(
        df_merged=merged_df,
        forecast_df=forecast_df,
        challenge_mask=mask_domestic_wchs_wchc
    )

    t10 = step(
        t9,
        f"\n=== CHALLENGE #4 — DOMESTIC FLIGHTS WITH WCHS OR WCHC ==="
    )
    for k, v in summary_domestic_wchs_wchc.items():
        print(f"{k}: {v}")

    
    # ------------------------------------------------------------
    # CHALLENGE #5 — Capacity breach: >1 Ambulift / Minibus required
    #                  (2 WCHC or 6 seated PRMs per vehicle)
    # ------------------------------------------------------------

    def ch_vehicle_capacity_breach(df):
        """
        Challenge #5:
        Flights where PRM demand exceeds the capacity of a single
        Ambulift or Minibus, assuming:
        • 2 WCHC per vehicle
        • 6 seated PRMs per vehicle
        """
        
        
        # Treat missing counts as zero
        wchc = df["WCHC Count"].fillna(0)
        prms = df["PRM Count"].fillna(0)

        # Wheelchair requirement (2 WCHC per vehicle)
        veh_for_wchc = np.ceil(wchc / 2)

        # Seated requirement (WCHC occupy chairs, rest are seated PRMs)
        seated_prms = prms - wchc
        veh_for_seated = np.ceil(seated_prms / 6)

        # Required vehicles is the maximum of the two
        required_vehicles = np.maximum(veh_for_wchc, veh_for_seated)

        return required_vehicles > 1



    mask_capacity_breach = ch_vehicle_capacity_breach(merged_df)

    summary_capacity_breach = challenge_summary(
        df_merged=merged_df,
        forecast_df=forecast_df,
        challenge_mask=mask_capacity_breach
    )

    t11 = step(
        t10,
        f"\n=== CHALLENGE #5 — VEHICLE CAPACITY BREACH (>1 VEHICLE REQUIRED) ==="
    )
    for k, v in summary_capacity_breach.items():
        print(f"{k}: {v}")

    
    # ------------------------------------------------------------
    # CHALLENGE #6 — Flights without a jetbridge
    # ------------------------------------------------------------

    def ch_no_jetbridge(df):
        """
        Challenge #6:
        Flights operating at stands without a jetbridge.
        Jetbridge stands are defined in config.JETBRIDGE_STANDS.
        """
        return ~df["Stand"].isin(JETBRIDGE_STANDS)


    mask_no_jetbridge = ch_no_jetbridge(merged_df)

    summary_no_jetbridge = challenge_summary(
        df_merged=merged_df,
        forecast_df=forecast_df,
        challenge_mask=mask_no_jetbridge
    )

    t12 = step(
        t11,
        f"\n=== CHALLENGE #6 — FLIGHTS WITHOUT A JETBRIDGE ==="
    )
    for k, v in summary_no_jetbridge.items():
        print(f"{k}: {v}")

    
    # # ------------------------------------------------------------
    # # CHALLENGE #7 — Companion proxy:
    # #                  Vehicles deployed exceed PRM capacity need
    # # ------------------------------------------------------------

    # def ch_companion_proxy(df):
    #     """
    #     Challenge #7:
    #     Flights where more vehicles were deployed than required
    #     by PRM demand alone, indicating likely companion transport.

    #     Assumptions:
    #     • 2 WCHC per vehicle
    #     • 6 seated PRMs per vehicle
    #     """

    #     # Fill missing counts
    #     prms = df["PRM Count"].fillna(0)
    #     wchc = df["WCHC Count"].fillna(0)

    #     # Vehicles required by PRMs
    #     veh_for_wchc = np.ceil(wchc / 2)
    #     seated_prms = prms - wchc
    #     veh_for_seated = np.ceil(seated_prms / 6)

    #     required_vehicles = np.maximum(veh_for_wchc, veh_for_seated)

    #     # Actual vehicles used
    #     actual_vehicles = df["Total Vehicle Count"].fillna(0)

    #     # Companion proxy condition
    #     return actual_vehicles > required_vehicles


    # mask_companion_proxy = ch_companion_proxy(merged_df)

    # summary_companion_proxy = challenge_summary(
    #     df_merged=merged_df,
    #     forecast_df=forecast_df,
    #     challenge_mask=mask_companion_proxy
    # )

    # t13 = step(
    #     t12,
    #     f"\n=== CHALLENGE #7 — COMPANION PROXY (EXCESS VEHICLE DEPLOYMENT) ==="
    # )
    # for k, v in summary_companion_proxy.items():
    #     print(f"{k}: {v}")


    # ------------------------------------------------------------
    # CHALLENGE #7 (REFINED) — Companion proxy:
    #   Same-vehicle overdeployment vs PRM demand
    # ------------------------------------------------------------

    
    def ch_companion_proxy_refined(df):
        """
        Refined companion proxy (final):

        Flags flights where MORE vehicles of the SAME type were deployed
        than PRM demand alone requires, with the critical condition that
        a baseline need for that vehicle type existed.

        Assumptions:
        • Ambulift capacity: 2 × WCHC
        • Minibus capacity: 6 seated PRMs
        • A vehicle used when required = 0 does NOT count as excess
        """

        # Fill missing values
        prms = df["PRM Count"].fillna(0)
        wchc = df["WCHC Count"].fillna(0)

        ambulifts_used = df.get("Ambulift", 0).fillna(0)
        minibuses_used = df.get("Mini Bus", 0).fillna(0)

        # Required vehicles by class
        required_ambulifts = np.ceil(wchc / 2)

        seated_prms = prms - wchc
        required_minibuses = np.ceil(seated_prms / 6)

        # Only count overdeployment where a baseline need existed
        excess_ambulift = (
            (required_ambulifts >= 1) &
            (ambulifts_used > required_ambulifts)
        )

        excess_minibus = (
            (required_minibuses >= 1) &
            (minibuses_used > required_minibuses)
        )

        return excess_ambulift | excess_minibus



    mask_companion_proxy_refined = ch_companion_proxy_refined(merged_df)

    summary_companion_proxy_refined = challenge_summary(
        df_merged=merged_df,
        forecast_df=forecast_df,
        challenge_mask=mask_companion_proxy_refined
    )

    t13 = step(
        t12,
        f"\n=== CHALLENGE #7 (REFINED) — COMPANION PROXY (SAME-VEHICLE OVERDEPLOYMENT) ==="
    )
    for k, v in summary_companion_proxy_refined.items():
        print(f"{k}: {v}")

    # ------------------------------------------------------------
    # CHALLENGE #8 — WCHS SSR code with own chair. 
    # ------------------------------------------------------------

    def ch_wchs_own_chair(df: pd.DataFrame) -> pd.DataFrame:
        """
        Challenge:
        Passengers whose Primary PRM is WCHS and who own a wheelchair
        requiring a wheelchair space (WC* secondary SSR),
        excluding WCHR and WCHC.
        """

        # ------------------------------------------
        # Base secondary SSR breakdown (already clean)
        # ------------------------------------------
        SSR_df = get_secondarySSR_count_penrate(df)
        wchs_df = SSR_df[SSR_df["Primary PRM"] == "WCHS"].copy()

        # ------------------------------------------
        # Passenger-level calculation for ANY WC*
        # excluding WCHR / WCHC
        # ------------------------------------------
        sec_df = df.dropna(subset=["PRM Secondary String"]).copy()

        sec_df["Secondary SSR"] = (
            sec_df["PRM Secondary String"]
            .str.split(r"[,\|]", regex=True)
        )
        sec_df = sec_df.explode("Secondary SSR")

        sec_df["Secondary SSR"] = sec_df["Secondary SSR"].str.strip()

        sec_df = sec_df[
            (sec_df["Primary PRM"] == "WCHS") &
            (sec_df["Secondary SSR"].str.startswith("WC")) &
            (~sec_df["Secondary SSR"].isin(["WCHR", "WCHC"])) &
            (sec_df["Secondary SSR"] != "WCHS")
        ]

        # Unique WCHS passengers with ANY qualifying WC* secondary
        wc_any_count = sec_df["PassengerID"].nunique()

        # Primary total (constant for WCHS)
        primary_total = wchs_df["Primary Total"].iloc[0]

        wc_any_penetration = wc_any_count / primary_total

        # ------------------------------------------
        # Append summary row
        # ------------------------------------------
        summary_row = pd.DataFrame([{
            "Primary PRM": "WCHS",
            "Secondary SSR": "ANY WC* (excl WCHR/WCHC)",
            "Secondary Count": wc_any_count,
            "Primary Total": primary_total,
            "Penetration Rate": wc_any_penetration
        }])

        wchs_df = pd.concat([wchs_df, summary_row], ignore_index=True)

        return wchs_df.sort_values("Secondary Count", ascending=False)


    wchs_own_chair = ch_wchs_own_chair(ecac_df)

    t14 = step(
        t13,
        f"\n=== CHALLENGE #8 — PASSENGERS WITH WCHS SSR CODE AND OWN WHEELCHAIR ==="
    )
    print(wchs_own_chair)
    
