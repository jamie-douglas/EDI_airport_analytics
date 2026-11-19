#---------------------------------------------------------------
# PRM flight-based grouping and merge
#---------------------------------------------------------------

def calculate_prm_per_flight(prm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute PRM passenger count per (Flight ID, Flight Date) using group_unique.
    
    Parameters
    ----------
    prm_df : pandas.DataFrame
        Must include: ['Flight ID', 'Flight Date', 'Passenger ID'].
    
    Returns
    -------
    pandas.DataFrame
        One row per (Flight ID, Flight Date) with:
        - Flight ID
        - Flight Date
        - PRM_Pax  (unique Passenger ID count)
    """
    base = prm_df[["Flight ID", "Flight Date", "Passenger ID"]].copy()
    out = group_unique(base, by_cols=["Flight ID", "Flight Date"], id_col="Passenger ID")
    return out.rename(columns={"Unique Count": "PRM_Pax"})


def merge_prm_to_flights(flights: pd.DataFrame, prm_by_flight: pd.DataFrame) -> pd.DataFrame:
    """
    Merge per-flight PRM counts into the flights table.
    
    Parameters
    ----------
    flights : pandas.DataFrame
        Must include: ['Flight ID','Flight Date','Passengers','A/D','Year','Stand Code'].
        May omit 'Has Jet Bridge' (will be derived if absent).
    prm_by_flight : pandas.DataFrame
        Must include: ['Flight ID','Flight Date','PRM_Pax'].
    
    Returns
    -------
    pandas.DataFrame
        Flights augmented with:
        - PRM_Pax (int, default 0 if missing)
        - Has Jet Bridge (0/1), derived if missing using JETBRIDGE_STANDS
    """
    out = flights.merge(prm_by_flight, on=["Flight ID","Flight Date"], how="left")
    out["PRM_Pax"] = out["PRM_Pax"].fillna(0).astype(int)
    if "Has Jet Bridge" not in out.columns:
        out["Has Jet Bridge"] = out["Stand Code"].isin(JETBRIDGE_STANDS).astype(int)
    return out

# modules/domain/prm/efficiency.py
from __future__ import annotations
import pandas as pd
from modules.analytics.grouping import group_sum
from modules.analytics.penetration import row_penetration
from modules.analytics.timeseries import bucket_time  # ready for future slot-by-slot analyses

def yearly_prm_penetration(flight_master: pd.DataFrame) -> pd.DataFrame:
    """
    Compute yearly PRM penetration (Total PRM / Total Pax).

    Parameters
    ----------
    flight_master : pandas.DataFrame
        Must include: ['Year','Passengers','PRM_Pax'].

    Returns
    -------
    pandas.DataFrame
        One row per Year with:
        - Total Pax
        - Total PRM
        - PRM% (ratio; multiply by 100 at presentation if required)
    """
    y_pax = group_sum(flight_master, ["Year"], "Passengers", "Total Pax")
    y_prm = group_sum(flight_master, ["Year"], "PRM_Pax",   "Total PRM")
    yearly = y_pax.merge(y_prm, on="Year", how="outer")
    return row_penetration(yearly, numerator_col="Total PRM", denominator_col="Total Pax", out_col="PRM%")

# modules/domain/prm/minibus.py
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import List

from modules.analytics.grouping import group_unique, group_sum
from modules.analytics.bins import histogram_counts



def minibus_demand_summary(
    flight_master: pd.DataFrame,
    prm_df: pd.DataFrame,
    seats_per_bus: int = 6
) -> pd.DataFrame:
    """
    Summarise minibus demand using labelled buckets (0/1/2/3/4+).
    
    Parameters
    ----------
    flight_master : pandas.DataFrame
        Output of merge_prm_to_flights(); must include:
        ['Flight ID','Flight Date','Year','A/D','Has Jet Bridge','PRM_Pax'].
    prm_df : pandas.DataFrame
        PRM jobs; must include: ['Flight ID','Flight Date','Passenger ID','SSR Code'].
    seats_per_bus : int, default 6
        Capacity for non-WCHC PRM per minibus.
    
    Returns
    -------
    pandas.DataFrame
        One row per (Year, A/D, Has Jet Bridge, Minibus_Category) with:
        - Flights (unique flight count in that category)
    """
    wchc_base   = prm_df.loc[prm_df["SSR Code"] == "WCHC", ["Flight ID","Flight Date","Passenger ID"]]
    wchc_counts = group_unique(wchc_base, ["Flight ID","Flight Date"], id_col="Passenger ID") \
                    .rename(columns={"Unique Count": "WCHC"})

    fm = flight_master.merge(wchc_counts, on=["Flight ID","Flight Date"], how="left")
    fm["WCHC"]      = fm["WCHC"].fillna(0).astype(int)
    fm["Other_PRM"] = (fm["PRM_Pax"] - fm["WCHC"]).clip(lower=0).astype(int)

    buses_for_other = np.ceil(fm["Other_PRM"] / seats_per_bus)
    buses_for_wchc = (fm["WCHC"] /2 ).clip(lower=0)
    extra_wchc      = (fm["WCHC"] - 1).clip(lower=0)
    fm["Minibuses_needed"] = np.maximum(buses_for_other, buses_for_wchc).astype(int)
    fm.loc[fm["Other_PRM"] == 0, "Minibuses_needed"] = 0

    bins, labels = [0,1,2,3,4,np.inf], ["0","1","2","3","4+"]
    fm["Minibus_Category"] = pd.cut(fm["Minibuses_needed"], bins=bins, labels=labels, right=False)

    dedup = fm.drop_duplicates(["Flight ID","Flight Date","Year","A/D","Has Jet Bridge","Minibus_Category"]) \
              .assign(One=1)
    out = group_sum(dedup,
                    by_cols=["Year","A/D","Has Jet Bridge","Minibus_Category"],
                    value_col="One",
                    out_col="Flights")
    return out.sort_values(["Year","A/D","Has Jet Bridge","Minibus_Category"])


def minibus_demand_histogram(
    flight_master: pd.DataFrame,
    prm_df: pd.DataFrame,
    seats_per_bus: int = 7,
    bins: List[float] = [0,1,2,3,4,np.inf]
) -> pd.DataFrame:
    """
    Summarise minibus demand as numeric histogram bins per (Year, A/D, Has Jet Bridge).
    
    Parameters
    ----------
    flight_master : pandas.DataFrame
        Output of merge_prm_to_flights(); must include:
        ['Flight ID','Flight Date','Year','A/D','Has Jet Bridge','PRM_Pax'].
    prm_df : pandas.DataFrame
        PRM jobs; must include: ['Flight ID','Flight Date','Passenger ID','SSR Code'].
    seats_per_bus : int, default 7
        Capacity for non-WCHC PRM per minibus.
    bins : list[float], default [0,1,2,3,4,inf]
        Bin edges for histogram_counts.
    
    Returns
    -------
    pandas.DataFrame
        For each (Year, A/D, Has Jet Bridge) and each bin:
        - Year, A/D, Has Jet Bridge
        - Bin Start, Bin End, Bin Midpoint, Count
    """
    wchc_base   = prm_df.loc[prm_df["SSR Code"] == "WCHC", ["Flight ID","Flight Date","Passenger ID"]]
    wchc_counts = group_unique(wchc_base, ["Flight ID","Flight Date"], id_col="Passenger ID") \
                    .rename(columns={"Unique Count": "WCHC"})

    fm = flight_master.merge(wchc_counts, on=["Flight ID","Flight Date"], how="left")
    fm["WCHC"]      = fm["WCHC"].fillna(0).astype(int)
    fm["Other_PRM"] = (fm["PRM_Pax"] - fm["WCHC"]).clip(lower=0).astype(int)

    buses_for_other = np.ceil(fm["Other_PRM"] / seats_per_bus)
    extra_wchc      = (fm["WCHC"] - 1).clip(lower=0)
    fm["Minibuses_needed"] = np.maximum(buses_for_other, extra_wchc).astype(int)
    fm.loc[fm["Other_PRM"] == 0, "Minibuses_needed"] = 0

    rows = []
    for (yr, ad, jb), g in fm.groupby(["Year","A/D","Has Jet Bridge"], dropna=False):
        h = histogram_counts(g["Minibuses_needed"], bins=bins)
        h.insert(0, "Has Jet Bridge", jb)
        h.insert(0, "A/D", ad)
        h.insert(0, "Year", yr)
        rows.append(h)
    if not rows:
        return pd.DataFrame(columns=["Year","A/D","Has Jet Bridge","Bin Start","Bin End","Bin Midpoint","Count"])
    return pd.concat(rows, ignore_index=True)


def top_stands_and_ssr(
    prm_df: pd.DataFrame,
    top_n_stands: int = 3,
    top_n_ssr: int = 3
) -> pd.DataFrame:
    """
    List top N stands and, within each, top N SSR codes by unique passengers,
    split by Vehicle Type and Jetbridge flag.
    
    Parameters
    ----------
    prm_df : pandas.DataFrame
        Must include: ['Vehicle Type','Has Jet Bridge','Stand Code','SSR Code','Passenger ID'].
    top_n_stands : int, default 3
        Number of stands to return per (Vehicle Type, Has Jet Bridge).
    top_n_ssr : int, default 3
        Number of SSR codes to return per selected stand.
    
    Returns
    -------
    pandas.DataFrame
        Columns:
        - Vehicle Type, Has Jet Bridge, Stand Code, SSR Code
        - Passengers (unique Passenger ID)
    """
    rows = []
    for vehicle in ["Ambulift","Mini Bus"]:
        for jb in [1, 0]:
            cat = prm_df[(prm_df["Vehicle Type"] == vehicle) & (prm_df["Has Jet Bridge"] == jb)]
            if cat.empty:
                continue
            stands_base = cat[["Stand Code","Passenger ID"]]
            top_stands = group_unique(stands_base, by_cols=["Stand Code"], id_col="Passenger ID") \
                           .rename(columns={"Unique Count": "Total_Passengers"}) \
                           .sort_values("Total_Passengers", ascending=False) \
                           .head(top_n_stands)

            for stand in top_stands["Stand Code"]:
                sub = cat.loc[cat["Stand Code"] == stand, ["SSR Code","Passenger ID"]]
                top_ssr = group_unique(sub, by_cols=["SSR Code"], id_col="Passenger ID") \
                            .rename(columns={"Unique Count": "Passengers"}) \
                            .sort_values("Passengers", ascending=False) \
                            .head(top_n_ssr)
                top_ssr["Stand Code"]     = stand
                top_ssr["Vehicle Type"]   = vehicle
                top_ssr["Has Jet Bridge"] = jb
                rows.append(top_ssr)

    if not rows:
        return pd.DataFrame(columns=["Vehicle Type","Has Jet Bridge","Stand Code","SSR Code","Passengers"])
    out = pd.concat(rows, ignore_index=True)
    return out[["Vehicle Type","Has Jet Bridge","Stand Code","SSR Code","Passengers"]]

# modules/domain/prm/reception.py
from __future__ import annotations
import pandas as pd
from typing import List

from modules.config import PRM_LANDSIDE_VALUES, PRM_AIRSIDE_VALUES
from modules.analytics.grouping import group_unique, group_sum

def passenger_level_flags(prm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build passenger-level flags by Year and SSR Code.

    Parameters
    ----------
    prm_df : pandas.DataFrame
        Must include: ['Year','Passenger ID','SSR Code','Vehicle Type','Has Jet Bridge'].

    Returns
    -------
    pandas.DataFrame
        One row per (Year, Passenger ID, SSR Code) with:
        - Used_Ambulift (0/1)
        - Used_Minibus  (0/1)
        - Has_JetBridge (max over jobs)
        - Passengertype ('Ambulift'|'Mini Bus'|'Both'|'No Vehicle')
    """
    x = prm_df.copy()
    grp = (x.groupby(["Year","Passenger ID","SSR Code"], dropna=False)
             .agg(
                 Used_Ambulift=("Vehicle Type", lambda s: int("Ambulift" in set(s))),
                 Used_Minibus=("Vehicle Type", lambda s: int("Mini Bus"  in set(s))),
                 Has_JetBridge=("Has Jet Bridge","max"),
             )
             .reset_index())
    grp["Passengertype"] = "No Vehicle"
    grp.loc[ grp["Used_Ambulift"].eq(1) & grp["Used_Minibus"].eq(0), "Passengertype"] = "Ambulift"
    grp.loc[ grp["Used_Ambulift"].eq(0) & grp["Used_Minibus"].eq(1), "Passengertype"] = "Mini Bus"
    grp.loc[ grp["Used_Ambulift"].eq(1) & grp["Used_Minibus"].eq(1), "Passengertype"] = "Both"
    return grp


def ssr_yearly_distribution(prm_df: pd.DataFrame, split_keys: List[str] | None = None) -> pd.DataFrame:
    """
    Compute SSR distribution by Year using group_unique; adds totals and % share.

    Parameters
    ----------
    prm_df : pandas.DataFrame
        Must include: ['Year','SSR Code','Passenger ID'] plus any split_keys.
    split_keys : list[str], optional
        Additional split keys, e.g., ['A/D'], ['Has Jet Bridge'].

    Returns
    -------
    pandas.DataFrame
        Columns:
        - <split_keys...> (if any), Year, SSR Code, Pax
        - Total (sum over SSR within group scope)
        - % of PRM (Pax / Total * 100)
    """
    keys = (split_keys or []) + ["Year","SSR Code"]
    base = prm_df[keys + ["Passenger ID"]]
    ssr  = group_unique(base, by_cols=keys, id_col="Passenger ID") \
             .rename(columns={"Unique Count": "Pax"})
    total_keys = (split_keys or []) + ["Year"]
    totals = group_sum(ssr, by_cols=total_keys, value_col="Pax", out_col="Total")
    out = ssr.merge(totals, on=total_keys, how="left")
    out["% of PRM"] = out["Pax"] / out["Total"] * 100.0
    return out


def reception_use_by_ssr(
    prm_df: pd.DataFrame,
    location_col: str = "Location",
    split_keys: List[str] | None = None
) -> pd.DataFrame:
    """
    Count unique PRM passengers by SSR Code and Reception Type (Landside/Airside/Other).

    Parameters
    ----------
    prm_df : pandas.DataFrame
        Must include: ['Passenger ID','SSR Code', location_col] and optional split_keys.
    location_col : str, default 'Location'
        Column with reception labels mapped via PRM_LANDSIDE_VALUES / PRM_AIRSIDE_VALUES.
    split_keys : list[str], optional
        Additional split keys (e.g., ['Year'], ['Year','A/D']).

    Returns
    -------
    pandas.DataFrame
        Columns:
        - <split_keys...> (if any), Reception Type, SSR Code
        - Pax, Total, % of PRM
    """
    x = prm_df.copy()
    vals = x[location_col].astype(str).fillna("")
    x["Reception Type"] = "Other"
    x.loc[vals.isin(PRM_LANDSIDE_VALUES), "Reception Type"] = "Landside"
    x.loc[vals.isin(PRM_AIRSIDE_VALUES),  "Reception Type"] = "Airside"

    keys = (split_keys or []) + ["Reception Type","SSR Code"]
    base = x[keys + ["Passenger ID"]]
    g = group_unique(base, by_cols=keys, id_col="Passenger ID") \
          .rename(columns={"Unique Count": "Pax"})

    total_keys = (split_keys or []) + ["Reception Type"]
    totals = group_sum(g, by_cols=total_keys, value_col="Pax", out_col="Total")
    out = g.merge(totals, on=total_keys, how="left")
    out["% of PRM"] = out["Pax"] / out["Total"] * 100.0
    return out


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
