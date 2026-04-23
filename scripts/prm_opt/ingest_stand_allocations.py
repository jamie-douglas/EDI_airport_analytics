
"""
prm_opt.ingest_stand_allocations
--------------------------------
Ingest and process stand allocation outputs for S26.

These files represent realised / planned stand allocations
for June and July only.

They are used in two ways:
  1) Deterministic stand assignment where dates overlap June–July
  2) Empirical stand distribution construction for extrapolation
     beyond July (rest of S26 season)

No optimisation logic lives here – this is purely demand construction.
"""

from __future__ import annotations
import pandas as pd


def load_stand_allocations(csv_paths: list[str]) -> pd.DataFrame:
    """
    Load and harmonise stand allocation CSVs.

    Each CSV represents a full daily turn-level stand plan.
    Each row contains both arrival and departure details.

    Output schema:
      FlightNumber
      ScheduledDateTime_Local
      Airline
      dir              ('A' or 'D')
      class            ('Dom' or 'Int')
      stand
    """

    frames = []
    for p in csv_paths:
        frames.append(pd.read_csv(p))

    raw = pd.concat(frames, ignore_index=True)

    # -------------------------
    # ARRIVALS
    # -------------------------
    arrivals = raw[
        [
            "Arr_Flight_No",
            "Arr_Scheduled_Date",
            "Arr_Operator",
            "DI_arr",
            "stand",
        ]
    ].copy()

    arrivals.rename(
        columns={
            "Arr_Flight_No": "FlightNumber",
            "Arr_Scheduled_Date": "ScheduledDateTime_Local",
            "Arr_Operator": "Airline",
            "DI_arr": "class",
        },
        inplace=True,
    )
    arrivals["dir"] = "A"

    # -------------------------
    # DEPARTURES
    # -------------------------
    departures = raw[
        [
            "Dep_Flight_No",
            "Dep_Scheduled_Date",
            "Dep_Operator",
            "DI_dep",
            "stand",
        ]
    ].copy()

    departures.rename(
        columns={
            "Dep_Flight_No": "FlightNumber",
            "Dep_Scheduled_Date": "ScheduledDateTime_Local",
            "Dep_Operator": "Airline",
            "DI_dep": "class",
        },
        inplace=True,
    )
    departures["dir"] = "D"

    # -------------------------
    # COMBINE
    # -------------------------
    out = pd.concat([arrivals, departures], ignore_index=True)

    # -------------------------
    # CLEAN & NORMALISE
    # -------------------------
    out = out.dropna(
        subset=[
            "FlightNumber",
            "ScheduledDateTime_Local",
            "stand",
        ]
    )

    out["FlightNumber"] = out["FlightNumber"].astype(str)
    out["ScheduledDateTime_Local"] = pd.to_datetime(out["ScheduledDateTime_Local"])

    # Normalise stand naming (strip terminal suffixes etc.)
    out["stand"] = (
        out["stand"]
        .astype(str)
        .str.replace("-T1", "", regex=False)
        .str.strip()
    )

    # Normalise Domestic / International labels
    out["class"] = (
        out["class"]
        .astype(str)
        .str.strip()
        .replace(
            {
                "DOM": "Dom",
                "INT": "Int",
                "IRISH": "Int",
                "NIRISH": "Int",
            }
        )
    )

    return out


def build_stand_distribution(stand_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build empirical stand distributions for extrapolation
    beyond July.

    Conditioning dimensions:
      Airline
      dir        (A/D)
      class      (Dom/Int)

    Output:
      Airline | dir | class | stand | prob
    """

    counts = (
        stand_df
        .groupby(["Airline", "dir", "class", "stand"])
        .size()
        .reset_index(name="count")
    )

    totals = (
        counts
        .groupby(["Airline", "dir", "class"])["count"]
        .sum()
        .reset_index(name="total")
    )

    dist = counts.merge(
        totals,
        on=["Airline", "dir", "class"],
        how="left",
    )

    dist["prob"] = dist["count"] / dist["total"]

    return dist
