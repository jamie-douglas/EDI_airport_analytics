
# prm_opt/ingest_s26.py

from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import timedelta

from modules.utils.query import query
from prm_opt.config import STAND_ZONES, WCHS_OWN_CHAIR_PROB, NO_JETBRIDGE_AIRLINES
from prm_opt.sector import normalise_sector


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
    sector: str,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    rng: np.random.Generator,
) -> str:
    """
    Stand assignment:
      1) deterministic from June/July actuals
      2) extrapolated from distributions conditioned on Airline/dir/sector
    """

    exact = stand_actuals[
        (stand_actuals["FlightNumber"] == str(flight_number))
        & (stand_actuals["ScheduledDateTime_Local"] == sched)
        & (stand_actuals["dir"] == direction)
    ]

    if len(exact) > 0:
        return str(exact.iloc[0]["stand"])

    
    
    # Level 1: Airline + dir + Sector
    fb = stand_dist[
        (stand_dist["Airline"] == airline)
        & (stand_dist["dir"] == direction)
        & (stand_dist["Sector"] == sector)
    ]

    # Level 2: Airline + dir
    if len(fb) == 0:
        fb = stand_dist[
            (stand_dist["Airline"] == airline)
            & (stand_dist["dir"] == direction)
        ]

    # ✅ NEW — Level 3: dir + Sector
    if len(fb) == 0:
        fb = stand_dist[
            (stand_dist["dir"] == direction)
            & (stand_dist["Sector"] == sector)
        ]

    # Level 4: Airline only
    if len(fb) == 0:
        fb = stand_dist[
            (stand_dist["Airline"] == airline)
        ]

    # Level 5: Sector only
    if len(fb) == 0:
        fb = stand_dist[
            (stand_dist["Sector"] == sector)
        ]

    # Level 6: Global
    if len(fb) == 0:
        fb = stand_dist


    # Final safety check
    if len(fb) == 0:
        raise ValueError(f"Completely empty stand_dist — cannot assign stands")

    
    fb = fb.copy()

    # drop bad stands
    fb = fb.dropna(subset=["stand", "prob"])
    fb["stand"] = fb["stand"].astype(str)
    fb = fb[fb["stand"].str.strip() != ""]

    if len(fb) == 0:
        raise ValueError(f"No valid stands after cleaning for {airline}|{direction}|{sector}")

    # Ensure probabilities sum to 1
    p = fb["prob"].values.astype(float)
    p = p / p.sum()

    return rng.choice(fb["stand"].values, p=p)



def build_offset_lookup(chocks_offset_params: pd.DataFrame):
    """
    Convert chocks_offset_params to dict:
      (Airline Code, A/D, Sector) -> (mean, std)
    """
    d = {}
    for _, r in chocks_offset_params.iterrows():
        d[(str(r["Airline Code"]), str(r["A/D"]), str(r["Sector"]).upper())] = (
            float(r["mean_offset_mins"]),
            float(r["std_offset_mins"]),
        )
    return d




def sample_sched_to_chocks(
    airline: str,
    direction: str,
    sector: str,
    offset_lookup: dict,
    rng: np.random.Generator,
) -> float:
    """
    Sample Scheduled -> Chocks offset in minutes using Normal(mean, std),
    segmented by Airline x A/D x Sector.
    """
    key = (str(airline), str(direction), str(sector).upper())
    mean, std = offset_lookup.get(key, (0.0, 0.0))
    if std == 0.0:
        return float(mean)
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
      - For each arrival, find earliest subsequent departure with same Airline and Sector within max_gap.
      - Each departure can only be paired once.
    """
    
    df = df_flights.sort_values("Chocks_Est").copy()
    df["orig_idx"] = df.index
    df = df.reset_index(drop=True)


    used_dep = set()
    pair_id = [-1] * len(df)

    arrivals = df[df["dir"] == "A"].index.tolist()

    pid = 0
    for ai in arrivals:
        a = df.loc[ai]
        candidates = df[
            (df["dir"] == "D")
            & (df["Airline"] == a["Airline"])
            & (df["Sector"] == a["Sector"])
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
    
    out = df.set_index("orig_idx")[["turn_pair_id"]]
    return out




def ingest_s26(
    start: str,
    end: str,
    penetration_rates: pd.DataFrame,
    ssr_mix: pd.DataFrame,
    stand_actuals: pd.DataFrame,
    stand_dist: pd.DataFrame,
    service_time_params: pd.DataFrame,
    tau_mode_params: pd.DataFrame,  
    chocks_offset_params: pd.DataFrame,
    early_late_std_mins: float = 15.0,
    seed: int = 42,
    turnaround_max_gap_mins: int = 240,
    spin_window_mins: int = 120,
) -> pd.DataFrame:
    """
    Build S26 PRM jobs from FutureFlights table, including:
      - stochastic early/late timing
      - stochastic scheduled->chocks offset (by airline, A/D, sector)
      - concurrent stress computed from stochastic chocks
      - turnaround pairing (A/D pair)
      - turnaround PRM metrics (total + WCHC/WCHS + WCHS own chair)
      - spin flag (turnaround pair + gap <= spin window + any vertical PRM on arrival)
    """

    
    DEBUG = True          # turn off when done
    DEBUG_MAX_SHOW = 15   # limit prints


    rng = np.random.default_rng(seed)

    df_flights = load_future_flights(start, end).copy()

    # Standardise fields and create sector/dir
    df_flights["ScheduledDateTime_Local"] = pd.to_datetime(df_flights["ScheduledDateTime_Local"])
    df_flights["dir"] = np.where(df_flights["ArrDeptureCode"] == "A", "A", "D")
    df_flights["Airline"] = df_flights["AirlineCode_IATA"].astype(str)
    df_flights["Sector"] = df_flights["Sector"].apply(normalise_sector)

    # Pax Most Confident logic
    df_flights["Pax"] = np.where(
        df_flights["Pax_MostConfident"].isna(),
        df_flights["PublishedForecast_Pax"],
        df_flights["Pax_MostConfident"],
    ).astype(float)

    
    # if both forecast fields are missing, Pax becomes NaN → force 0
    df_flights["Pax"] = pd.to_numeric(df_flights["Pax"], errors="coerce").fillna(0.0)


    
    if DEBUG:
        print("\n" + "="*80)
        print("[DEBUG A] Future flights loaded & Pax computed")
        print("="*80)
        print("df_flights rows:", len(df_flights))
        print("Airlines (sample):", df_flights["Airline"].dropna().astype(str).unique()[:10])
        print("Countries (sample):", df_flights["CountryName"].dropna().astype(str).unique()[:10])
        print("\nPax describe:")
        print(df_flights["Pax"].describe())
        print("\nTop 10 rows (Airline, CountryName, Sector, dir, Pax):")
        print(df_flights[["Airline","CountryName","Sector","dir","Pax"]].head(DEBUG_MAX_SHOW))


    # Assign stand (deterministic where possible, else sampled)
    stands = []
    for _, r in df_flights.iterrows():
        stand = assign_stand(
            flight_number=str(r["FlightNumber"]),
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

    # Build chocks offset lookup
    offset_lookup = build_offset_lookup(chocks_offset_params)

    # Stochastic early/late + stochastic sched->chocks
    earlylate = rng.normal(0, early_late_std_mins, size=len(df_flights))
    chocks_offsets = [
        sample_sched_to_chocks(r["Airline"], r["dir"], r["Sector"], offset_lookup, rng)
        for _, r in df_flights.iterrows()
    ]

    df_flights["EarlyLate_mins"] = earlylate
    df_flights["SchedToChocks_mins"] = chocks_offsets

    df_flights["Chocks_Est"] = df_flights["ScheduledDateTime_Local"] + pd.to_timedelta(
        df_flights["EarlyLate_mins"] + df_flights["SchedToChocks_mins"], unit="m"
    )

    # Concurrent stress (computed AFTER stochastic chocks)
    df_flights["Concurrent Stress"] = compute_concurrent_stress(df_flights, chocks_col="Chocks_Est")

    
    # ---------------------------------------------------------
    # Turnaround pairing: use TurnID from stand plan when available,
    # otherwise fall back to heuristic pairing.
    # ---------------------------------------------------------
    
    if stand_actuals is not None and "ScheduledDateTime_Local" in stand_actuals.columns:
        stand_actuals = stand_actuals.copy()
        stand_actuals["ScheduledDateTime_Local"] = pd.to_datetime(
            stand_actuals["ScheduledDateTime_Local"]
        )


    # 1) Bring TurnID onto df_flights (matches how assign_stand does its exact lookup)
    if stand_actuals is not None and "TurnID" in stand_actuals.columns:
        turn_map = stand_actuals[["FlightNumber", "ScheduledDateTime_Local", "dir", "TurnID"]].drop_duplicates()
        df_flights = df_flights.merge(
            turn_map,
            left_on=["FlightNumber", "ScheduledDateTime_Local", "dir"],
            right_on=["FlightNumber", "ScheduledDateTime_Local", "dir"],
            how="left",
        )
    else:
        df_flights["TurnID"] = None

    # 2) Initialise turn_pair_id
    df_flights["turn_pair_id"] = -1

    # 3) Deterministic pairing using TurnID (for months you have the plan)
    has_turn = df_flights["TurnID"].notna()
    if has_turn.any():
        codes, _ = pd.factorize(df_flights.loc[has_turn, "TurnID"])
        df_flights.loc[has_turn, "turn_pair_id"] = codes

    # 4) Heuristic fallback for flights with no TurnID
    remaining = df_flights[df_flights["turn_pair_id"] == -1].copy()
    if len(remaining) > 0:
        remaining = pair_turnarounds(remaining, max_gap_mins=turnaround_max_gap_mins)
        offset = int(df_flights["turn_pair_id"].max()) + 1
        remaining.loc[remaining["turn_pair_id"] >= 0, "turn_pair_id"] += offset
        df_flights.loc[remaining.index, "turn_pair_id"] = remaining["turn_pair_id"]


    # Lookups for penetration and SSR mix
    pen_lookup = penetration_rates.set_index(["Airline Code", "CountryName"])["penetration"].to_dict()
    ssr_lookup = ssr_mix.set_index(["Airline Code", "CountryName", "SSR Code"])["share"].to_dict()
    svc_lookup = service_time_params.set_index(["SSR Code", "dir"]).to_dict("index")

    
    # ---------------------------------------------------------
    # Mode-specific tau lookup from S25 medians (SSR Code x A/D)
    # ---------------------------------------------------------
    tau_mode_lookup = (
        tau_mode_params
        .set_index(["SSR Code", "A/D"])[["tau_amb_mins", "tau_mini_mins", "tau_push_mins"]]
        .to_dict("index")
    )

    
    if DEBUG:
        print("\n" + "="*80)
        print("[DEBUG B] Penetration lookup match rate")
        print("="*80)

        # Create lookup keys from future flights
        keys = list(zip(df_flights["Airline"].astype(str), df_flights["CountryName"].astype(str)))
        df_flights["_pen_key"] = keys

        df_flights["_pen_found"] = df_flights["_pen_key"].apply(lambda k: k in pen_lookup)
        match_rate = df_flights["_pen_found"].mean() if len(df_flights) else 0.0

        print(f"Penetration keys available: {len(pen_lookup):,}")
        print(f"Flights with penetration match: {df_flights['_pen_found'].sum():,} / {len(df_flights):,} ({match_rate*100:0.1f}%)")

        # Show top missing key combos (this is usually the smoking gun)
        missing = df_flights[~df_flights["_pen_found"]].copy()
        if len(missing) > 0:
            top_missing = (
                missing.groupby(["Airline","CountryName"])
                    .size()
                    .reset_index(name="flight_rows")
                    .sort_values("flight_rows", ascending=False)
                    .head(DEBUG_MAX_SHOW)
            )
            print("\nTop missing (Airline, CountryName) combos in future flights:")
            print(top_missing)

            print("\nExample missing keys:")
            print(missing[["Airline","CountryName","Pax"]].head(DEBUG_MAX_SHOW))
        else:
            print("\n✅ All future flights matched a penetration key.")

    
    if DEBUG:
        print("\n" + "="*80)
        print("[DEBUG C] Expected PRMs vs rounding")
        print("="*80)

        # Use fallback when missing (same as your code)
        df_flights["_pen_used"] = df_flights["_pen_key"].map(pen_lookup).fillna(0.01).astype(float)

        df_flights["_prm_raw"] = df_flights["Pax"].fillna(0).astype(float) * df_flights["_pen_used"].fillna(0)
        df_flights["_prm_round"] = df_flights["_prm_raw"].fillna(0).round().astype(int)

        print("Total expected PRMs (raw sum, BEFORE rounding):", float(df_flights["_prm_raw"].sum()))
        print("Total PRMs after rounding:", int(df_flights["_prm_round"].sum()))

        print("\nRaw PRM demand distribution (prm_raw):")
        print(df_flights["_prm_raw"].describe())

        print("\nTop flights by raw PRM demand:")
        top = df_flights.sort_values("_prm_raw", ascending=False).head(DEBUG_MAX_SHOW)
        print(top[["Airline","CountryName","Pax","_pen_used","_prm_raw","_prm_round"]])

    # First pass: create per-flight PRM totals and SSR composition
    flight_prm_stats = {}
    for idx, r in df_flights.iterrows():
        airline = r["Airline"]
        country = r["CountryName"]
        direction = r["dir"]

        penetration = pen_lookup.get((airline, country), 0.01)
        
        pax = 0 if pd.isna(r["Pax"]) else r["Pax"]
        pen = penetration if pd.notna(penetration) else 0.0

        n_prm = int(round(pax * pen))


        
        
        if DEBUG and idx < DEBUG_MAX_SHOW:
            print(
                f"[DEBUG flight] Airline={airline} | "
                f"Country={country} | Pax={r['Pax']:.1f} | "
                f"Pen={penetration:.5f} | Pax*Pen={r['Pax']*penetration:.3f} | "
                f"PRMs={n_prm}"
            )



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

    
    # ---------------------------------------------------------
    # Derived fields to match S25 schema expectations downstream
    # ---------------------------------------------------------



    # Minutes on Chocks (proxy):
    # If the flight has a paired turnaround, approximate by (dep_chocks - arr_chocks) in minutes on the ARRIVAL row.
    df_flights["Minutes on Chocks"] = np.nan
    for pid in df_flights["turn_pair_id"].unique():
        if pid == -1:
            continue
        pair = df_flights[df_flights["turn_pair_id"] == pid]
        if len(pair) != 2:
            continue
        arr = pair[pair["dir"] == "A"]
        dep = pair[pair["dir"] == "D"]
        if len(arr) != 1 or len(dep) != 1:
            continue
        a_idx = arr.index[0]
        d_idx = dep.index[0]
        gap_mins = (df_flights.loc[d_idx, "Chocks_Est"] - df_flights.loc[a_idx, "Chocks_Est"]).total_seconds() / 60.0
        df_flights.loc[a_idx, "Minutes on Chocks"] = max(0.0, float(gap_mins))


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
        df_flights.loc[a_i, "Turnaround Vertical Count"] = dep_stats["WCHC_count"] + dep_stats["WCHS_count"]

        
    for col in ["Turnaround PRM Count", "Turnaround Vertical Count"]:
        if col in df_flights.columns:
            df_flights[col] = (
                df_flights[col]
                .fillna(0)
                .astype(int)
            )


    # Spin flag
    # Note:
    # Spin is defined as a turnaround arrival that:
    # - has a paired departure within spin_window_mins
    # - and has any vertical PRM (WCHC or WCHS) on arrival

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

            
            # ---------------------------------------------------------
            # Mode-specific tau for this (SSR, direction)
            # These are minutes consumed IF the optimiser chooses that mode.
            # ---------------------------------------------------------
            tau_row = tau_mode_lookup.get((ssr, direction))

            if tau_row is None:
                # fallback: use service-time median if unseen SSR/dir combo
                svc = svc_lookup.get((ssr, direction), {"median": 15.0, "std": 5.0})
                tau_amb = tau_mini = tau_push = float(svc["median"])
            else:
                tau_amb  = float(tau_row["tau_amb_mins"])
                tau_mini = float(tau_row["tau_mini_mins"])
                tau_push = float(tau_row["tau_push_mins"])


            
            rows.append({
                "Passenger ID": f"S26_{f['FlightNumber']}_{i}",
                "Airline Code": airline,
                "Flight Number": f["FlightNumber"],
                "A/D": direction,
                "Sector": sector,
                "CountryName": country,
                "Stand": stand,
                "Scheduled Flight DT": sched,                 # alias of ScheduledDateTime_Local
                "Chocks DT": f["Chocks_Est"],                 # alias of Chocks_Est
                "Minutes on Chocks": f.get("Minutes on Chocks", np.nan),
                "ScheduledDateTime_Local": sched,
                "Chocks_Est": f["Chocks_Est"],
                "Departure Gate": None,
                "SSR Code": ssr,
                "Has Own Chair": has_own,
                "IsEffectiveRemote": int(airline in NO_JETBRIDGE_AIRLINES),
                "PRM Flight Count": prm_flight_count,
                "Concurrent Stress": concurrent_stress,
                "Turnaround PRM Count": turnaround_prm_count,
                "Turnaround Vertical Count": int(f.get("Turnaround Vertical Count", 0) or 0),
                "IsArrival": is_arrival,
                "Adhoc Or Planned": "Planned",
                "IsAdhoc": 0,
                "Job Start Time": job_start,
                "Job End Time": job_end,
                "tau_amb_mins": tau_amb,
                "tau_mini_mins": tau_mini,
                "tau_push_mins": tau_push,
            })

    
    # ---------------- DEBUG: ingest_s26 output ----------------
    print("\n[DEBUG ingest_s26]")
    print(f"Total PRM jobs (rows): {len(rows)}")

    if len(rows) == 0:
        print("⚠️ No PRM jobs created (rows is empty)")
    else:
        df_dbg = pd.DataFrame(rows)
        print("Columns:", list(df_dbg.columns))
        print("SSR Code present:", "SSR Code" in df_dbg.columns)
        if "SSR Code" in df_dbg.columns:
            print("SSR Code distribution:")
            print(df_dbg["SSR Code"].value_counts())
    # ---------------------------------------------------------


    return pd.DataFrame(rows)
