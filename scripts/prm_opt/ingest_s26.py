
# prm_opt/ingest_s26.py

from __future__ import annotations

import numpy as np
import pandas as pd
from datetime import timedelta

from modules.utils.query import query
from prm_opt.config import STAND_ZONES, WCHS_OWN_CHAIR_PROB


# ---------------------------------------------------------
# Load future flight data (S26)
# ---------------------------------------------------------

def load_future_flights(start: str, end: str) -> pd.DataFrame:
    """
    Load future flight schedule from EAL.FlightPerformance_FutureFlights

    This is the authoritative source for S26 demand generation.
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


# ---------------------------------------------------------
# Stand assignment helper
# ---------------------------------------------------------

def assign_stand(
    flight_number: str,
    sched: pd.Timestamp,
    direction: str,
    airline: str,
    dom_int: str,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    rng: np.random.Generator,
) -> str:
    """
    Assign a stand for a flight.

    Priority:
      1. Deterministic match from June/July stand allocation files
      2. Empirical sampling from stand distribution
         conditioned on (Airline, A/D, Dom/Int)

    This mirrors how stand plans are extrapolated operationally
    beyond periods with an explicit tow / stand plan.
    """

    # ---------
    # Deterministic (June / July)
    # ---------
    exact = stand_actuals[
        (stand_actuals["FlightNumber"] == flight_number)
        & (stand_actuals["ScheduledDateTime_Local"] == sched)
        & (stand_actuals["dir"] == direction)
    ]

    if len(exact) > 0:
        return str(exact.iloc[0]["stand"])

    # ---------
    # Extrapolated (post-July)
    # ---------
    fb = stand_dist[
        (stand_dist["Airline"] == airline)
        & (stand_dist["dir"] == direction)
        & (stand_dist["class"] == dom_int)
    ]

    if len(fb) == 0:
        raise ValueError(
            f"No stand distribution available for "
            f"{airline} | {direction} | {dom_int}"
        )

    return rng.choice(
        fb["stand"].values,
        p=fb["prob"].values,
    )


# ---------------------------------------------------------
# Main S26 ingest
# ---------------------------------------------------------

def ingest_s26(
    start: str,
    end: str,
    penetration_rates: pd.DataFrame,
    ssr_mix: pd.DataFrame,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    service_time_params: pd.DataFrame,
    early_late_std_mins: float = 15.0,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Build S26 PRM jobs from FutureFlights table.

    Output is structurally identical to S25 df_prm_master
    and is passed directly into build_jobs().

    Key behaviour:
    - Flights always come from FutureFlights
    - Stand is deterministic where available (Jun/Jul)
    - Otherwise extrapolated probabilistically
    """

    rng = np.random.default_rng(seed)
    rows = []

    # ----------------------
    # Load flights
    # ----------------------
    df_flights = load_future_flights(start, end)

    # ----------------------
    # Build lookup tables
    # ----------------------
    pen_lookup = penetration_rates.set_index(
        ["Airline Code", "CountryName"]
    )["penetration"].to_dict()

    ssr_lookup = (
        ssr_mix
        .set_index(["Airline Code", "CountryName", "SSR Code"])["share"]
        .to_dict()
    )

    svc_lookup = (
        service_time_params
        .set_index(["SSR Code", "dir"])
        .to_dict("index")
    )

    # ----------------------
    # Expand flights → PRM jobs
    # ----------------------
    for _, f in df_flights.iterrows():

        sched = pd.to_datetime(f["ScheduledDateTime_Local"])
        direction = "A" if f["ArrDeptureCode"] == "A" else "D"

        airline = f["AirlineCode_IATA"]
        country = f["CountryName"]
        sector = f["Sector"]

        dom_int = "Dom" if sector == "DOM" else "Int"

        pax = (
            f["Pax_MostConfident"]
            if not pd.isna(f["Pax_MostConfident"])
            else f["PublishedForecast_Pax"]
        )

        penetration = pen_lookup.get((airline, country), 0.01)
        n_prm = int(round(pax * penetration))

        # ----------------------
        # Stand assignment
        # ----------------------
        stand = assign_stand(
            flight_number=f["FlightNumber"],
            sched=sched,
            direction=direction,
            airline=airline,
            dom_int=dom_int,
            stand_actuals=stand_actuals,
            stand_dist=stand_dist,
            rng=rng,
        )

        # ----------------------
        # Flight timing uncertainty
        # ----------------------
        delay = rng.normal(0, early_late_std_mins)
        eff_sched = sched + timedelta(minutes=delay)

        # ----------------------
        # SSR probabilities
        # ----------------------
        ssr_probs = {
            ssr: ssr_lookup.get((airline, country, ssr), 0.0)
            for ssr in ["WCHC", "WCHS", "WCHR", "OTHER"]
        }

        total = sum(ssr_probs.values())
        if total == 0:
            continue

        ssr_probs = {k: v / total for k, v in ssr_probs.items()}

        # ----------------------
        # Create PRM jobs
        # ----------------------
        for i in range(n_prm):

            ssr = rng.choice(
                list(ssr_probs.keys()),
                p=list(ssr_probs.values()),
            )

            # Own chair logic
            if ssr == "WCHC":
                has_own = 1
            elif ssr == "WCHS":
                has_own = int(rng.random() < WCHS_OWN_CHAIR_PROB)
            else:
                has_own = 0

            # Service time draw
            svc = svc_lookup[(ssr, direction)]
            base = max(
                1.0,
                rng.lognormal(
                    mean=np.log(svc["median"]),
                    sigma=svc["std"] / svc["median"],
                )
            )

            # Job timing
            if direction == "A":
                job_start = eff_sched + timedelta(minutes=5)
            else:
                job_start = eff_sched - timedelta(minutes=30)

            job_end = job_start + timedelta(minutes=base)

            rows.append({
                "Passenger ID": f"S26_{f['FlightNumber']}_{i}",
                "Airline Code": airline,
                "Flight Number": f["FlightNumber"],
                "A/D": direction,
                "Stand": stand,
                "Departure Gate": None,
                "ScheduledDateTime_Local": sched,
                "Job Start Time": job_start,
                "Job End Time": job_end,
                "SSR Code": ssr,
                "Has Own Chair": has_own,
                "IsEffectiveRemote": int(str(stand) not in STAND_ZONES),
                "Turnaround PRM Count": n_prm if direction == "A" else 0,
                "Sector": sector,
                "CountryName": country,
            })

    return pd.DataFrame(rows)
