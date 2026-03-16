
# scripts/minibus_report.py
import sys, pathlib, argparse, time
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import pandas as pd
from modules.config import JETBRIDGE_STANDS
from modules.utils.query import query
from modules.utils.dates import to_datetime
from modules.utils.excel import write_once_then_update
from modules.utils.progress import step

from modules.domain.prm.demand import (
    calculate_prm_per_flight,
    merge_prm_to_flights,
)
from modules.domain.prm.minibus import (
    minibus_demand_summary,
    minibus_demand_histogram,
    top_stands_and_ssr,
)
from modules.domain.prm.reception import (
    passenger_level_flags,
    ssr_yearly_distribution,
    reception_use_by_ssr,
)
from modules.domain.prm.efficiency import yearly_prm_penetration


def load_prm_jobs_for_minibus(start: str, end: str) -> pd.DataFrame:
    """
    Load PRM jobs for Minibus analysis using the standard query helper.

    Parameters
    ----------
    start : str
        Start window (YYYY-MM-DD), inclusive.
    end : str
        End window (YYYY-MM-DD), exclusive.

    Returns
    -------
    pandas.DataFrame
        PRM jobs with timestamps, stand, SSR, vehicle, plus:
        - Flight Date (date), Year (int), Has Jet Bridge (0/1)
    """
    df = query(
        table="PRM.CompletedServicesByJob",
        columns=[
            "PassengerID          AS [Passenger ID]",
            "FlightID             AS [Flight ID]",
            "AirlineCode_IATA     AS [Airline IATA Code]",
            "FlightNumber         AS [Flight Number]",
            "ArrDep               AS [A/D]",
            "currentSSRCode       AS [SSR Code]",
            "VehicleTypeName      AS [Vehicle Type]",
            "ActualDateTime_Local AS [Actual Date Time]",
            "StandCode            AS [Stand Code]",
            # Optional: "Location         AS [Location]",
        ],
        where=["BillingPRM = 1"],
        date_column="ActualDateTime_Local",
        start=start, end=end,
    )
    df = to_datetime(df, "Actual Date Time")
    df["Flight Date"]    = df["Actual Date Time"].dt.date
    df["Year"]           = df["Actual Date Time"].dt.year
    df["Has Jet Bridge"] = df["Stand Code"].isin(JETBRIDGE_STANDS).astype(int)
    return df


def load_flights_for_minibus(start: str, end: str) -> pd.DataFrame:
    """
    Load flights for Minibus analysis using the standard query helper.

    Parameters
    ----------
    start : str
        Start window (YYYY-MM-DD), inclusive.
    end : str
        End window (YYYY-MM-DD), exclusive.

    Returns
    -------
    pandas.DataFrame
        Flights with passengers, stand, A/D, plus:
        - Flight Date (date), Year (int), Has Jet Bridge (0/1)
    """
    df = query(
        table="EAL.FlightPerformance",
        columns=[
            "FlightID              AS [Flight ID]",
            "ArrDeptureCode        AS [A/D]",
            "FlightNumber          AS [Flight Number]",
            "AirlineCode_IATA      AS [Airline IATA Code]",
            "StandCode             AS [Stand Code]",
            "ActualDateTime_Local  AS [Actual Date Time]",
            "Passengers            AS [Passengers]",
            "RemoteStand           AS [Remote Stand]",
        ],
        date_column="ActualDateTime_Local",
        start=start, end=end,
    )
    df = to_datetime(df, "Actual Date Time")
    df["Flight Date"]    = df["Actual Date Time"].dt.date
    df["Year"]           = df["Actual Date Time"].dt.year
    df["Has Jet Bridge"] = df["Stand Code"].isin(JETBRIDGE_STANDS).astype(int)
    return df


def build_minibus_report(start: str, end: str) -> dict[str, pd.DataFrame]:
    """
    Build the Minibus analysis tables for the supplied window.

    Parameters
    ----------
    start : str
        Start window (YYYY-MM-DD), inclusive.
    end : str
        End window (YYYY-MM-DD), exclusive.

    Returns
    -------
    dict[str, pandas.DataFrame]
        Named output tables ready to be written to Excel.
    """
    t0 = time.perf_counter()
    print("[1/5] Loading PRM jobs…")
    prm = load_prm_jobs_for_minibus(start, end)
    t1 = step(t0, f"PRM rows: {len(prm):,}")

    print("[2/5] Loading flights…")
    flights = load_flights_for_minibus(start, end)
    t2 = step(t1, f"Flight rows: {len(flights):,}")

    print("[3/5] Building per-flight master…")
    prm_by_flight = calculate_prm_per_flight(prm)
    per_flight    = merge_prm_to_flights(flights, prm_by_flight)
    t3 = step(t2, "Per-flight table built.")

    print("[4/5] Computing analyses…")
    # Demand views
    minibus_summary   = minibus_demand_summary(per_flight, prm)
    minibus_histogram = minibus_demand_histogram(per_flight, prm)
    # SSR distribution (Reception_by_SSR requires Location; enable when available)
    ssr_dist          = ssr_yearly_distribution(prm, split_keys=None)
    # reception_ssr     = reception_use_by_ssr(prm, location_col="Location", split_keys=["Year"])
    # Efficiency headline
    yearly_pen        = yearly_prm_penetration(per_flight)
    t4 = step(t3, "Analyses ready.")

    outputs = dict(
        PerFlight=per_flight,
        Minibus_Summary=minibus_summary,
        Minibus_Histogram=minibus_histogram,
        SSR_Distribution=ssr_dist,
        Yearly_Penetration=yearly_pen,
        # Reception_by_SSR=reception_ssr,
    )
    return outputs


def main(start: str, end: str, excel_out: str | None):
    """
    Orchestrate the Minibus report build and optionally write to Excel.

    Parameters
    ----------
    start : str
        Start window (YYYY-MM-DD), inclusive.
    end : str
        End window (YYYY-MM-DD), exclusive.
    excel_out : str | None
        Output workbook path; if None, dataframes are not written.

    Returns
    -------
    None
    """
    tables = build_minibus_report(start, end)

    if excel_out:
        print("[5/5] Writing Excel…")
        for name, df in tables.items():
            write_once_then_update(excel_out, name, df, anchor="A2", include_header=True)
        print(f"✔ Excel updated → {excel_out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Minibus Report")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end",   required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--out",   default=None,  help="Output Excel path")
    args = p.parse_args()
    main(args.start, args.end, args.out)
