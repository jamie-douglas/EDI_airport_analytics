
# prm_opt/ingest_s26.py

from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import timedelta

from modules.utils.query import query
from prm_opt.config import STAND_ZONES, WCHS_OWN_CHAIR_PROB


def load_future_flights(start: str, end: str) -> pd.DataFrame:
    """
    Load future flight schedule from EAL.FlightPerformance_FutureFlights.
    """
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


def assign_stand(
    flight_number: str,
    sched: pd.Timestamp,
    direction: str,
    airline: str,
    flight_class: str,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    rng: np.random.Generator,
) -> str:
    """
    Stand assignment:
      1) deterministic from June/July actuals
      2) extrapolated from distributions conditioned on Airline/dir/class
    """

    exact = stand_actuals[
        (stand_actuals["FlightNumber"] == str(flight_number))
        & (stand_actuals["ScheduledDateTime_Local"] == sched)
        & (stand_actuals["dir"] == direction)
    ]

    if len(exact) > 0:
        return str(exact.iloc[0]["stand"])

    fb = stand_dist[
        (stand_dist["Airline"] == airline)
        & (stand_dist["dir"] == direction)
        & (stand_dist["class"] == flight_class)
    ]

    if len(fb) == 0:
        raise ValueError(f"No stand distribution for {airline} | {direction} | {flight_class}")

    return rng.choice(fb["stand"].values, p=fb["prob"].values)


def sector_to_class(sector: str) -> str:
    s = str(sector).upper()
    if "DOM" in s:
        return "Dom"
    if s in ["IRISH", "NIRISH"]:
        return "CTA"
    return "Int"


def build_offset_lookup(chocks_offset_params: pd.DataFrame):
    """
    Convert chocks_offset_params.csv to dict:
      (Airline, dir, class) -> (mean, std)
    """
    d = {}
    for _, r in chocks_offset_params.iterrows():
        d[(r["Airline Code"], r["A/D"], r["class"])] = (float(r["mean_offset_mins"]), float(r["std_offset_mins"]))
    return d


def sample_sched_to_chocks(airline: str, direction: str, flight_class: str, offset_lookup: dict, rng: np.random.Generator) -> float:
    """
    Sample the Scheduled -> Chocks offset in minutes using Normal(mean, std),
    segmented by Airline x A/D x Class.
    """
    mean, std = offset_lookup.get((airline, direction, flight_class), (0.0, 0.0))
    if std == 0.0:
        return mean
    return float(rng.normal(mean, std))


def compute_concurrent_stress(df_flights: pd.DataFrame, chocks_col: str = "Chocks_Est") -> pd.Series:
    """
    Concurrent Stress = number of flights in ±30 mins window around Chocks_Est, excluding itself.
    """
    stresses = []
    for _, r in df_flights.iterrows():
        w0 = r[chocks_col] - timedelta(minutes=30)
        w1 = r[chocks_col] + timedelta(minutes=30)
        c = df_flights[(df_flights[chocks_col] >= w0) & (df_flights[chocks_col] <= w1)].shape[0]
        stresses.append(c - 1)
    return pd.Series(stresses, index=df_flights.index)


def pair_turnarounds(df_flights: pd.DataFrame, max_gap_mins: int = 240) -> pd.DataFrame:
    """
    Create A/D turnaround pairs:
      - For each arrival, find earliest subsequent departure with same Airline and Class within max_gap.
      - Each departure can only be paired once.
    """
    df = df_flights.sort_values("Chocks_Est").reset_index(drop=True)

    used_dep = set()
    pair_id = [-1] * len(df)

    arrivals = df[df["dir"] == "A"].index.tolist()

    pid = 0
    for ai in arrivals:
        a = df.loc[ai]
        candidates = df[
            (df["dir"] == "D")
            & (df["Airline"] == a["Airline"])
            & (df["class"] == a["class"])
            & (df["Chocks_Est"] >= a["Chocks_Est"])
            & (df["Chocks_Est"] <= a["Chocks_Est"] + timedelta(minutes=max_gap_mins))
        ]

        # pick earliest available dep
        chosen = None
        for di in candidates.index.tolist():
            if di not in used_dep:
                chosen = di
                break

        if chosen is not None:
            used_dep.add(chosen)
            pair_id[ai] = pid
            pair_id[chosen] = pid
            pid += 1

    df["turn_pair_id"] = pair_id
    return df


def ingest_s26(
    start: str,
    end: str,
    penetration_rates: pd.DataFrame,
    ssr_mix: pd.DataFrame,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    service_time_params: pd.DataFrame,
    chocks_offset_params: pd.DataFrame,
    early_late_std_mins: float = 15.0,
    seed: int = 42,
    turnaround_max_gap_mins: int = 240,
    spin_window_mins: int = 120,
) -> pd.DataFrame:
    """
    Build S26 PRM jobs from FutureFlights table, including:
      - stochastic early/late timing
      - stochastic scheduled->chocks offset (by airline, A/D, class)
      - concurrent stress computed from stochastic chocks
      - turnaround pairing (A/D pair)
      - turnaround PRM metrics (total + WCHC/WCHS + WCHS own chair)
      - spin flag (turnaround pair + gap <= spin window + any vertical PRM on arrival)
    """

    rng = np.random.default_rng(seed)

    df_flights = load_future_flights(start, end).copy()

    # Standardise fields and create class/dir
    df_flights["ScheduledDateTime_Local"] = pd.to_datetime(df_flights["ScheduledDateTime_Local"])
    df_flights["dir"] = np.where(df_flights["ArrDeptureCode"] == "A", "A", "D")
    df_flights["Airline"] = df_flights["AirlineCode_IATA"].astype(str)
    df_flights["class"] = df_flights["Sector"].apply(sector_to_class)

    # Pax Most Confident logic
    df_flights["Pax"] = np.where(
        df_flights["Pax_MostConfident"].isna(),
        df_flights["PublishedForecast_Pax"],
        df_flights["Pax_MostConfident"],
    ).astype(float)

    # Assign stand (deterministic where possible, else sampled)
    stands = []
    for _, r in df_flights.iterrows():
        stand = assign_stand(
            flight_number=str(r["FlightNumber"]),
            sched=r["ScheduledDateTime_Local"],
            direction=r["dir"],
            airline=r["Airline"],
            flight_class=r["class"],
            stand_actuals=stand_actuals,
            stand_dist=stand_dist,
            rng=rng,
        )
        stands.append(stand)

    df_flights["Stand"] = stands

    # Build chocks offset lookup
    offset_lookup = build_offset_lookup(chocks_offset_params)

    # Stochastic early/late + stochastic sched->chocks
    earlylate = rng.normal(0, early_late_std_mins, size=len(df_flights))
    chocks_offsets = [
        sample_sched_to_chocks(r["Airline"], r["dir"], r["class"], offset_lookup, rng)
        for _, r in df_flights.iterrows()
    ]

    df_flights["EarlyLate_mins"] = earlylate
    df_flights["SchedToChocks_mins"] = chocks_offsets

    df_flights["Chocks_Est"] = df_flights["ScheduledDateTime_Local"] + pd.to_timedelta(
        df_flights["EarlyLate_mins"] + df_flights["SchedToChocks_mins"], unit="m"
    )

    # Concurrent stress (computed AFTER stochastic chocks)
    df_flights["Concurrent Stress"] = compute_concurrent_stress(df_flights, chocks_col="Chocks_Est")

    # Turnaround pairing (A/D pair)
    df_flights = pair_turnarounds(df_flights, max_gap_mins=turnaround_max_gap_mins)

    # Lookups for penetration and SSR mix
    pen_lookup = penetration_rates.set_index(["Airline Code", "CountryName"])["penetration"].to_dict()
    ssr_lookup = ssr_mix.set_index(["Airline Code", "CountryName", "SSR Code"])["share"].to_dict()
    svc_lookup = service_time_params.set_index(["SSR Code", "dir"]).to_dict("index")

    # First pass: create per-flight PRM totals and SSR composition
    flight_prm_stats = {}
    for idx, r in df_flights.iterrows():
        airline = r["Airline"]
        country = r["CountryName"]
        direction = r["dir"]

        penetration = pen_lookup.get((airline, country), 0.01)
        n_prm = int(round(r["Pax"] * penetration))

        # SSR distribution for that airline/country
        ssr_probs = {s: ssr_lookup.get((airline, country, s), 0.0) for s in ["WCHC", "WCHS", "WCHR", "OTHER"]}
        tot = sum(ssr_probs.values())
        if tot == 0:
            ssr_probs = {"WCHC": 0.05, "WCHS": 0.30, "WCHR": 0.50, "OTHER": 0.15}
            tot = 1.0
        ssr_probs = {k: v / tot for k, v in ssr_probs.items()}

        # sample SSRs at flight level for composition
        ssrs = rng.choice(list(ssr_probs.keys()), size=max(n_prm, 1), p=list(ssr_probs.values()))
        wchc = int((ssrs == "WCHC").sum()) if n_prm > 0 else 0
        wchs = int((ssrs == "WCHS").sum()) if n_prm > 0 else 0

        # own-chair: WCHC always own; WCHS has own-chair probability
        wchs_own = int(rng.binomial(wchs, WCHS_OWN_CHAIR_PROB)) if wchs > 0 else 0

        flight_prm_stats[idx] = {
            "PRM Flight Count": n_prm,
            "WCHC_count": wchc,
            "WCHS_count": wchs,
            "WCHS_own_count": wchs_own,
        }

    # Second pass: compute turnaround PRM stats for arrivals by using paired departure stats
    df_flights["Turnaround PRM Count"] = 0
    df_flights["Turnaround WCHC"] = 0
    df_flights["Turnaround WCHS"] = 0
    df_flights["Turnaround WCHS Own"] = 0

    for pid in df_flights["turn_pair_id"].unique():
        if pid == -1:
            continue
        pair = df_flights[df_flights["turn_pair_id"] == pid]
        if len(pair) != 2:
            continue

        a_idx = pair[pair["dir"] == "A"].index
        d_idx = pair[pair["dir"] == "D"].index
        if len(a_idx) != 1 or len(d_idx) != 1:
            continue

        a_i = int(a_idx[0])
        d_i = int(d_idx[0])

        dep_stats = flight_prm_stats[d_i]
        df_flights.loc[a_i, "Turnaround PRM Count"] = dep_stats["PRM Flight Count"]
        df_flights.loc[a_i, "Turnaround WCHC"] = dep_stats["WCHC_count"]
        df_flights.loc[a_i, "Turnaround WCHS"] = dep_stats["WCHS_count"]
        df_flights.loc[a_i, "Turnaround WCHS Own"] = dep_stats["WCHS_own_count"]

    # Spin flag: only arrivals can be spin triggers, and only if pair exists and gap <= spin window
    df_flights["is_spin"] = 0
    for pid in df_flights["turn_pair_id"].unique():
        if pid == -1:
            continue
        pair = df_flights[df_flights["turn_pair_id"] == pid]
        if len(pair) != 2:
            continue

        arr = pair[pair["dir"] == "A"].iloc[0]
        dep = pair[pair["dir"] == "D"].iloc[0]
        gap = (dep["Chocks_Est"] - arr["Chocks_Est"]).total_seconds() / 60.0

        # approximate "vertical exists on arrival" using WCHC/WCHS counts (any WCHC/WCHS implies vertical candidate)
        arr_idx = int(pair[pair["dir"] == "A"].index[0])
        vertical_candidate = (flight_prm_stats[arr_idx]["WCHC_count"] + flight_prm_stats[arr_idx]["WCHS_count"]) > 0

        if gap <= spin_window_mins and vertical_candidate:
            df_flights.loc[arr_idx, "is_spin"] = 1

    # Expand to passenger-level PRM jobs (structure identical to S25 ingest)
    rows = []
    for idx, f in df_flights.iterrows():
        sched = f["ScheduledDateTime_Local"]
        direction = f["dir"]
        airline = f["Airline"]
        country = f["CountryName"]
        sector = f["Sector"]
        stand = f["Stand"]

        n_prm = flight_prm_stats[idx]["PRM Flight Count"]
        prm_flight_count = n_prm
        concurrent_stress = int(f["Concurrent Stress"])
        turnaround_prm_count = int(f["Turnaround PRM Count"])
        is_arrival = 1 if direction == "A" else 0

        # SSR probs for passenger-level assignment
        ssr_probs = {s: ssr_lookup.get((airline, country, s), 0.0) for s in ["WCHC", "WCHS", "WCHR", "OTHER"]}
        tot = sum(ssr_probs.values())
        if tot == 0:
            ssr_probs = {"WCHC": 0.05, "WCHS": 0.30, "WCHR": 0.50, "OTHER": 0.15}
            tot = 1.0
        ssr_probs = {k: v / tot for k, v in ssr_probs.items()}

        # job start based on estimated chocks (arrival +5, departure -30)
        if direction == "A":
            job_start_base = f["Chocks_Est"] + timedelta(minutes=5)
        else:
            job_start_base = f["Chocks_Est"] - timedelta(minutes=30)

        for i in range(n_prm):
            ssr = rng.choice(list(ssr_probs.keys()), p=list(ssr_probs.values()))

            if ssr == "WCHC":
                has_own = 1
            elif ssr == "WCHS":
                has_own = int(rng.random() < WCHS_OWN_CHAIR_PROB)
            else:
                has_own = 0

            svc = svc_lookup.get((ssr, direction), {"median": 15.0, "std": 5.0})
            base = max(
                1.0,
                rng.lognormal(
                    mean=np.log(float(svc["median"])),
                    sigma=float(svc["std"]) / max(float(svc["median"]), 1.0),
                )
            )

            job_start = job_start_base
            job_end = job_start + timedelta(minutes=base)

            rows.append({
                "Passenger ID": f"S26_{f['FlightNumber']}_{i}",
                "Airline Code": airline,
                "Flight Number": f["FlightNumber"],
                "A/D": direction,
                "Sector": sector,
                "CountryName": country,
                "Stand": stand,
                "Departure Gate": None,
                "ScheduledDateTime_Local": sched,
                "Chocks_Est": f["Chocks_Est"],
                "SSR Code": ssr,
                "Has Own Chair": has_own,
                "IsEffectiveRemote": int(str(stand) not in STAND_ZONES),
                "PRM Flight Count": prm_flight_count,
                "Concurrent Stress": concurrent_stress,
                "Turnaround PRM Count": turnaround_prm_count,
                "IsArrival": is_arrival,
                "Job Start Time": job_start,
                "Job End Time": job_end,
            })

    return pd.DataFrame(rows)
