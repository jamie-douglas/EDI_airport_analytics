
"""
Ingest and process stand allocation plans for S26.

These CSV files represent *realised or planned* stand allocations
for June and July.

They serve two purposes:
1) Deterministic stand assignment for flights covered by the plans
2) Empirical stand distributions for extrapolation beyond July

NOTE:
-----
This module contains NO optimisation logic.
It is purely concerned with demand construction.
"""

from __future__ import annotations
import pandas as pd


# --------------------------------------------------
# CTA classification support
# --------------------------------------------------

CTA_AIRPORTS = {
    "DUB", "SNN", "ORK", "KIR", "NOC", "WAT",
    "IOM", "GCI", "JER",
}


def classify_stand_class(
    *,
    di_code: str | None,
    origin: str | None,
    dest: str | None,
) -> str:
    """
    Assign a canonical operational class for stand usage.

    Priority order:
    ---------------
    1) CTA override based on origin/destination airports
    2) DI code fallback:
         - DOM, NIRISH → Domestic
         - IRISH       → CTA
         - INT         → International

    This logic is specific to stand operations and must remain
    independent from flight-sector classification.
    """
    di = str(di_code).upper().strip() if di_code else ""
    origin = str(origin).upper().strip() if origin else ""
    dest = str(dest).upper().strip() if dest else ""

    if origin in CTA_AIRPORTS or dest in CTA_AIRPORTS:
        return "CTA"

    if di in ("DOM", "NIRISH"):
        return "Domestic"
    if di == "IRISH":
        return "CTA"
    if di == "INT":
        return "International"

    return "International"


# --------------------------------------------------
# Stand plan ingestion
# --------------------------------------------------

def load_stand_allocations(csv_paths: list[str]) -> pd.DataFrame:
    """
    Load and harmonise stand allocation CSVs.

    Each CSV row represents a *turn* containing both
    arrival and departure information.

    Output:
      One row per flight leg (A and D), with:
        - FlightNumber
        - ScheduledDateTime_Local
        - Airline
        - dir (A/D)
        - class (Domestic / CTA / International)
        - stand
    """
    frames = [pd.read_csv(p) for p in csv_paths]
    raw = pd.concat(frames, ignore_index=True)

    # -------------------------
    # Arrivals
    # -------------------------
    arrivals = raw[
        [
            "TurnID",
            "Arr_Flight_No",
            "Arr_Scheduled_Date",
            "Arr_Operator",
            "Arr_Origin",
            "Arr_Dest",
            "DI_arr",
            "stand",
        ]
    ].copy()

    arrivals.rename(
        columns={
            "Arr_Flight_No": "FlightNumber",
            "Arr_Scheduled_Date": "ScheduledDateTime_Local",
            "Arr_Operator": "Airline",
            "Arr_Origin": "Origin",
            "Arr_Dest": "Dest",
            "DI_arr": "DI",
        },
        inplace=True,
    )
    arrivals["dir"] = "A"

    # -------------------------
    # Departures
    # -------------------------
    departures = raw[
        [
            "TurnID",
            "Dep_Flight_No",
            "Dep_Scheduled_Date",
            "Dep_Operator",
            "Dep_Origin",
            "Dep_Dest",
            "DI_dep",
            "stand",
        ]
    ].copy()

    departures.rename(
        columns={
            "Dep_Flight_No": "FlightNumber",
            "Dep_Scheduled_Date": "ScheduledDateTime_Local",
            "Dep_Operator": "Airline",
            "Dep_Origin": "Origin",
            "Dep_Dest": "Dest",
            "DI_dep": "DI",
        },
        inplace=True,
    )
    departures["dir"] = "D"

    # -------------------------
    # Combine + clean
    # -------------------------
    out = pd.concat([arrivals, departures], ignore_index=True)

    out = out.dropna(
        subset=["FlightNumber", "ScheduledDateTime_Local", "stand"]
    )

    out["FlightNumber"] = out["FlightNumber"].astype(str)
    out["ScheduledDateTime_Local"] = pd.to_datetime(out["ScheduledDateTime_Local"])

    # Normalise stand IDs
    out["stand"] = (
        out["stand"]
        .astype(str)
        .str.replace("-T1", "", regex=False)
        .str.strip()
    )

    # Assign operational stand class
    out["Sector"] = out.apply(
        lambda r: classify_stand_class(
            di_code=r["DI"],
            origin=r["Origin"],
            dest=r["Dest"],
        ),
        axis=1,
    )

    return out.drop(columns=["DI", "Origin", "Dest"])


# --------------------------------------------------
# Build empirical stand distributions
# --------------------------------------------------

def build_stand_distribution(stand_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build empirical stand distributions from stand plans.

    Conditioning dimensions:
      Airline x dir x sector

    Output:
      Airline | dir | sector | stand | prob
    """
    counts = (
        stand_df
        .groupby(["Airline", "dir", "Sector", "stand"])
        .size()
        .reset_index(name="count")
    )

    totals = (
        counts
        .groupby(["Airline", "dir", "Sector"])["count"]
        .sum()
        .reset_index(name="total")
    )

    dist = counts.merge(
        totals,
        on=["Airline", "dir", "Sector"],
        how="left",
    )

    dist["prob"] = dist["count"] / dist["total"]
    return dist
