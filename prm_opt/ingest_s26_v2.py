# scripts/prm_opt/ingest_s26_v2.py

from __future__ import annotations

import numpy as np
import pandas as pd

from modules.utils.query import query

from prm_opt.config import (
    STAND_ZONES,
    WCHS_OWN_CHAIR_PROB,
    NO_JETBRIDGE_AIRLINES,
)

from prm_opt.sector import normalise_sector


"""
Future S26 ingest for the V2 PRM fleet optimisation model.

Purpose
-------
Creates passenger-level forecast PRM rows from future flight schedules.

Important V2 differences from old ingest_s26:
- No time buckets.
- No stochastic chocks required for optimisation.
- No job start/end times.
- No concurrent stress required by optimiser.
- Keeps stand assignment.
- Keeps is_remote and is_effective_remote separately.
- Adds penetration_uplift parameter.
- Produces the same passenger-level contract as ingest_s25_v2.py.

The optimiser itself works at flight level after build_flights_v2.py.
"""


ZONE1_REMOTE_STANDS = {str(s) for s in range(99, 107)}
REMOTE_ZONES = {"Z4", "Z5", "Z6", "Z7"}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _normalise_stand_code(stand: object) -> str:
    s = str(stand).upper().strip()
    if s in {"", "NAN", "NONE"}:
        return ""
    s = s.replace("-T1", "")
    if "-" in s:
        s = s.split("-", 1)[0]
    return s.strip()


def _is_remote_stand_from_zone(stand: object) -> int:
    stand_code = _normalise_stand_code(stand)
    if stand_code == "":
        return 0

    zone = STAND_ZONES.get(stand_code)
    if zone in REMOTE_ZONES:
        return 1
    if zone == "Z1":
        return int(stand_code in ZONE1_REMOTE_STANDS)
    return 0


def _clean_flight_number(s: object) -> str:
    return str(s).strip().lstrip("0")


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
# Load future flights
# ---------------------------------------------------------------------

def load_future_flights(start: str, end: str) -> pd.DataFrame:
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


# ---------------------------------------------------------------------
# Stand assignment
# ---------------------------------------------------------------------

def assign_stand(
    flight_number: str,
    sched: pd.Timestamp,
    direction: str,
    airline: str,
    sector: str,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    rng: np.random.Generator,
) -> str:
    """
    Assign future stand using:
    1) exact stand plan / actuals if available
    2) sampled distribution fallback
    """

    if stand_actuals is not None and len(stand_actuals) > 0:
        actuals = stand_actuals.copy()
        if "ScheduledDateTime_Local" in actuals.columns:
            actuals["ScheduledDateTime_Local"] = pd.to_datetime(actuals["ScheduledDateTime_Local"])

        exact = actuals[
            (actuals["FlightNumber"].astype(str).str.lstrip("0") == _clean_flight_number(flight_number))
            & (actuals["ScheduledDateTime_Local"] == sched)
            & (actuals["dir"].astype(str) == direction)
        ]

        if len(exact) > 0:
            return str(exact.iloc[0]["stand"])

    def _clean_candidates(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or len(df) == 0:
            return pd.DataFrame(columns=["stand", "prob"])
        out = df.dropna(subset=["stand", "prob"]).copy()
        out["stand"] = out["stand"].astype(str)
        out = out[out["stand"].str.strip() != ""]
        out["prob"] = pd.to_numeric(out["prob"], errors="coerce")
        out = out.dropna(subset=["prob"])
        out = out[out["prob"] > 0]
        return out

    candidate_levels = [
        stand_dist[
            (stand_dist["Airline"] == airline)
            & (stand_dist["dir"] == direction)
            & (stand_dist["Sector"] == sector)
        ],
        stand_dist[
            (stand_dist["Airline"] == airline)
            & (stand_dist["dir"] == direction)
        ],
        stand_dist[
            (stand_dist["dir"] == direction)
            & (stand_dist["Sector"] == sector)
        ],
        stand_dist[stand_dist["Airline"] == airline],
        stand_dist[stand_dist["Sector"] == sector],
        stand_dist,
    ]

    fb = pd.DataFrame()
    for level_df in candidate_levels:
        cleaned = _clean_candidates(level_df)
        if len(cleaned) > 0:
            fb = cleaned
            break

    if len(fb) == 0:
        raise ValueError(f"No valid stands for {airline}|{direction}|{sector}")

    p = fb["prob"].values.astype(float)
    p = p / p.sum()

    return str(rng.choice(fb["stand"].values, p=p))


# ---------------------------------------------------------------------
# Turnaround pairing
# ---------------------------------------------------------------------

def pair_turnarounds(
    df_flights: pd.DataFrame,
    stand_actuals: pd.DataFrame | None = None,
    max_gap_mins: int = 240,
    spin_window_mins: int = 120,
) -> pd.DataFrame:
    """
    Create turn_pair_id and paired_flight_key.

    Prefer TurnID from stand_actuals if available, then fallback to heuristic:
    earliest departure after arrival with same airline and sector.
    """

    df = df_flights.copy()

    df["turn_pair_id"] = -1
    df["paired_flight_key"] = None
    df["is_spin_candidate"] = 0

    # Deterministic TurnID if available.
    if stand_actuals is not None and "TurnID" in stand_actuals.columns:
        sa = stand_actuals.copy()
        if "ScheduledDateTime_Local" in sa.columns:
            sa["ScheduledDateTime_Local"] = pd.to_datetime(sa["ScheduledDateTime_Local"])

        turn_map = sa[
            ["FlightNumber", "ScheduledDateTime_Local", "dir", "TurnID"]
        ].drop_duplicates()

        turn_map["FlightNumber"] = turn_map["FlightNumber"].astype(str).str.lstrip("0")
        turn_map["dir"] = turn_map["dir"].astype(str)

        df = df.merge(
            turn_map,
            left_on=["FlightNumber", "ScheduledDateTime_Local", "dir"],
            right_on=["FlightNumber", "ScheduledDateTime_Local", "dir"],
            how="left",
        )

        has_turn = df["TurnID"].notna()
        if has_turn.any():
            codes, uniques = pd.factorize(df.loc[has_turn, "TurnID"])
            df.loc[has_turn, "turn_pair_id"] = codes

    # Heuristic fallback for unpaired rows.
    remaining = df[df["turn_pair_id"] == -1].copy()
    if len(remaining) > 0:
        remaining = remaining.sort_values("ScheduledDateTime_Local").copy()
        used_dep = set()
        next_pair_id = int(df["turn_pair_id"].max()) + 1

        arrivals = remaining[remaining["dir"] == "A"]

        for ai, a in arrivals.iterrows():
            cand = remaining[
                (remaining["dir"] == "D")
                & (remaining["Airline"] == a["Airline"])
                & (remaining["Sector"] == a["Sector"])
                & (remaining["ScheduledDateTime_Local"] > a["ScheduledDateTime_Local"])
                & (
                    remaining["ScheduledDateTime_Local"]
                    <= a["ScheduledDateTime_Local"] + pd.to_timedelta(max_gap_mins, unit="m")
                )
            ].copy()

            if len(cand) == 0:
                continue

            cand = cand.sort_values("ScheduledDateTime_Local")
            chosen_idx = None

            for ci in cand.index.tolist():
                if ci not in used_dep:
                    chosen_idx = ci
                    break

            if chosen_idx is None:
                continue

            used_dep.add(chosen_idx)

            df.loc[df.index == ai, "turn_pair_id"] = next_pair_id
            df.loc[df.index == chosen_idx, "turn_pair_id"] = next_pair_id

            a_key = df.loc[ai, "flight_key"]
            d_key = df.loc[chosen_idx, "flight_key"]

            df.loc[df.index == ai, "paired_flight_key"] = d_key
            df.loc[df.index == chosen_idx, "paired_flight_key"] = a_key

            gap = (
                df.loc[chosen_idx, "ScheduledDateTime_Local"]
                - df.loc[ai, "ScheduledDateTime_Local"]
            ).total_seconds() / 60.0

            if gap <= spin_window_mins:
                df.loc[df.index.isin([ai, chosen_idx]), "is_spin_candidate"] = 1

            next_pair_id += 1

    # Populate paired keys for deterministic TurnID pairs if missing.
    for pid in df["turn_pair_id"].dropna().unique():
        if int(pid) < 0:
            continue
        pair = df[df["turn_pair_id"] == pid]
        if len(pair) != 2:
            continue

        a = pair[pair["dir"] == "A"]
        d = pair[pair["dir"] == "D"]
        if len(a) != 1 or len(d) != 1:
            continue

        a_idx = a.index[0]
        d_idx = d.index[0]

        df.loc[a_idx, "paired_flight_key"] = df.loc[d_idx, "flight_key"]
        df.loc[d_idx, "paired_flight_key"] = df.loc[a_idx, "flight_key"]

        gap = (
            df.loc[d_idx, "ScheduledDateTime_Local"]
            - df.loc[a_idx, "ScheduledDateTime_Local"]
        ).total_seconds() / 60.0

        if gap <= spin_window_mins:
            df.loc[[a_idx, d_idx], "is_spin_candidate"] = 1

    return df[
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

def ingest_s26_v2(
    start: str,
    end: str,
    penetration_rates: pd.DataFrame,
    ssr_mix: pd.DataFrame,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    penetration_uplift: float = 1.0,
    seed: int = 42,
    turnaround_max_gap_mins: int = 240,
    spin_window_mins: int = 120,
) -> pd.DataFrame:
    """
    Build future forecast passenger-level PRM rows.

    Output is intentionally passenger-level so P90 stratified demand can
    preserve WCHC / WCHS / own-chair composition before aggregation.
    """

    rng = np.random.default_rng(seed)

    df_flights = load_future_flights(start, end).copy()

    df_flights["ScheduledDateTime_Local"] = pd.to_datetime(df_flights["ScheduledDateTime_Local"])
    df_flights["dir"] = np.where(df_flights["ArrDeptureCode"] == "A", "A", "D")
    df_flights["Airline"] = df_flights["AirlineCode_IATA"].astype(str).str.strip()
    df_flights["FlightNumber"] = df_flights["FlightNumber"].apply(_clean_flight_number)
    df_flights["Sector"] = df_flights["Sector"].apply(normalise_sector)

    df_flights["Pax"] = np.where(
        df_flights["Pax_MostConfident"].isna(),
        df_flights["PublishedForecast_Pax"],
        df_flights["Pax_MostConfident"],
    )

    df_flights["Pax"] = pd.to_numeric(df_flights["Pax"], errors="coerce").fillna(0.0)

    # Stand assignment.
    stands = []
    for _, r in df_flights.iterrows():
        stand = assign_stand(
            flight_number=r["FlightNumber"],
            sched=r["ScheduledDateTime_Local"],
            direction=r["dir"],
            airline=r["Airline"],
            sector=r["Sector"],
            stand_actuals=stand_actuals,
            stand_dist=stand_dist,
            rng=rng,
        )
        stands.append(stand)

    df_flights["Stand"] = stands

    df_flights["is_remote"] = df_flights["Stand"].apply(_is_remote_stand_from_zone).astype(int)

    df_flights["is_effective_remote"] = np.where(
        (df_flights["is_remote"] == 1)
        | (
            (df_flights["is_remote"] == 0)
            & (df_flights["Airline"].astype(str).isin(NO_JETBRIDGE_AIRLINES))
        ),
        1,
        0,
    ).astype(int)

    df_flights["flight_key"] = df_flights.apply(
        lambda r: _make_flight_key(
            r["Airline"],
            r["FlightNumber"],
            r["dir"],
            r["ScheduledDateTime_Local"],
        ),
        axis=1,
    )

    df_turn = pair_turnarounds(
        df_flights,
        stand_actuals=stand_actuals,
        max_gap_mins=turnaround_max_gap_mins,
        spin_window_mins=spin_window_mins,
    )

    df_flights = df_flights.merge(df_turn, on="flight_key", how="left")
    df_flights["turn_pair_id"] = df_flights["turn_pair_id"].fillna(-1).astype(int)
    df_flights["is_spin_candidate"] = df_flights["is_spin_candidate"].fillna(0).astype(int)

    # Lookups.
    pen_lookup = (
        penetration_rates
        .set_index(["Airline Code", "CountryName"])["penetration"]
        .to_dict()
    )

    ssr_lookup = (
        ssr_mix
        .set_index(["Airline Code", "CountryName", "SSR Code"])["share"]
        .to_dict()
    )

    rows = []

    for idx, f in df_flights.iterrows():
        airline = str(f["Airline"])
        country = str(f["CountryName"])
        direction = str(f["dir"])

        base_pen = float(pen_lookup.get((airline, country), 0.01))
        effective_pen = max(0.0, base_pen * float(penetration_uplift))

        pax = float(f["Pax"]) if pd.notna(f["Pax"]) else 0.0
        n_prm = int(round(pax * effective_pen))

        if n_prm <= 0:
            continue

        ssr_probs = {
            s: float(ssr_lookup.get((airline, country, s), 0.0))
            for s in ["WCHC", "WCHS", "WCHR", "OTHER"]
        }

        tot = sum(ssr_probs.values())
        if tot <= 0:
            ssr_probs = {"WCHC": 0.05, "WCHS": 0.30, "WCHR": 0.50, "OTHER": 0.15}
            tot = 1.0

        ssr_probs = {k: v / tot for k, v in ssr_probs.items()}

        ssrs = rng.choice(
            list(ssr_probs.keys()),
            size=n_prm,
            p=list(ssr_probs.values()),
        )

        for i, ssr in enumerate(ssrs):
            if ssr == "WCHC":
                has_own = 1
            elif ssr == "WCHS":
                has_own = int(rng.random() < WCHS_OWN_CHAIR_PROB)
            else:
                has_own = 0

            rows.append(
                {
                    "Passenger ID": f"S26_{f['flight_key']}_{i}",
                    "Flight ID": f.get("FlightID"),
                    "flight_key": f["flight_key"],
                    "Airline Code": airline,
                    "Flight Number": f["FlightNumber"],
                    "A/D": direction,
                    "Sector": f["Sector"],
                    "CountryName": country,
                    "Stand": _normalise_stand_code(f["Stand"]),
                    "Scheduled Flight DT": f["ScheduledDateTime_Local"],
                    "SSR Code": ssr,
                    "SSR numeric": 3 if ssr == "WCHC" else 2 if ssr == "WCHS" else 1,
                    "Has Own Chair": has_own,
                    "Pax": pax,
                    "is_remote": int(f["is_remote"]),
                    "is_effective_remote": int(f["is_effective_remote"]),
                    "turn_pair_id": int(f["turn_pair_id"]),
                    "paired_flight_key": f.get("paired_flight_key"),
                    "is_spin_candidate": int(f.get("is_spin_candidate", 0)),
                    "source": "S26",
                    "is_forecast": 1,
                    "penetration_base": base_pen,
                    "penetration_uplift": float(penetration_uplift),
                    "penetration_effective": effective_pen,
                }
            )

    return pd.DataFrame(rows)