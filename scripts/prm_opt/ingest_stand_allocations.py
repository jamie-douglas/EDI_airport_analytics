
"""
Ingest and process stand allocation outputs for S26.

These files represent realised / planned stand allocations
for June and July only.

They are used in two ways:
1. Deterministic stand assignment where dates overlap June–July
2. Empirical stand distribution construction for extrapolation
   beyond July (rest of S26 season)

"""

import pandas as pd


def load_stand_allocations(csv_paths):
    """
    Load and harmonise stand allocation CSVs.

    Each CSV represents a set of full daily turn-level stand plans.
    A row contains:
      - arrival information
      - departure information
      - a single stand for the whole turn

    We explode these into:
      - one arrival record
      - one departure record

    Output schema (canonical):
      FlightNumber
      ScheduledDateTime_Local
      Airline
      dir            (A | D)
      class          (Dom | Int)
      stand
    """

    frames = []

    for path in csv_paths:
        df = pd.read_csv(path)
        frames.append(df)

    raw = pd.concat(frames, ignore_index=True)

    # --------------------
    # ARRIVALS
    # --------------------
    arrivals = raw[
        [
            "Arr_Flight_No",
            "Arr_Scheduled_Date",
            "Arr_Operator",
            "DI_arr",
            "stand",
        ]
    ].copy()

    arrivals = arrivals.rename(
        columns={
            "Arr_Flight_No": "FlightNumber",
            "Arr_Scheduled_Date": "ScheduledDateTime_Local",
            "Arr_Operator": "Airline",
            "DI_arr": "class",
        }
    )

    arrivals["dir"] = "A"

    # --------------------
    # DEPARTURES
    # --------------------
    departures = raw[
        [
            "Dep_Flight_No",
            "Dep_Scheduled_Date",
            "Dep_Operator",
            "DI_dep",
            "stand",
        ]
    ].copy()

    departures = departures.rename(
        columns={
            "Dep_Flight_No": "FlightNumber",
            "Dep_Scheduled_Date": "ScheduledDateTime_Local",
            "Dep_Operator": "Airline",
            "DI_dep": "class",
        }
    )

    departures["dir"] = "D"

    # --------------------
    # COMBINE & CLEAN
    # --------------------
    out = pd.concat(
        [arrivals, departures],
        ignore_index=True,
    )

    # Drop incomplete rows
    out = out.dropna(
        subset=[
            "FlightNumber",
            "ScheduledDateTime_Local",
            "stand",
        ]
    )

    # Datetime parse
    out["ScheduledDateTime_Local"] = pd.to_datetime(
        out["ScheduledDateTime_Local"]
    )

    # Normalise stand naming
    out["stand"] = (
        out["stand"]
        .astype(str)
        .str.replace("-T1", "", regex=False)
    )

    # Normalise domestic / international labels
    out["class"] = out["class"].replace(
        {
            "DOM": "Dom",
            "INT": "Int",
            "IRISH": "Int",
            "NIRISH": "Int",
        }
    )

    return out


def build_stand_distribution(stand_df):
    """
    Build empirical stand distributions for extrapolation.

    Conditioning dimensions:
      Airline
      Arrival / Departure
      Domestic / International

    Output:
      For each (Airline, dir, class):
        probability mass function over stands

    This is used ONLY when we do not have a deterministic
    stand assignment (i.e. beyond July).
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
