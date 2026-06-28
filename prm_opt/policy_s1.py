
# scripts/prm_opt/policy_s1.py

import pandas as pd

def apply_policy_s1(jobs):
    """
    Apply Scenario 1 (S25 baseline) decision policy.

    This function encodes the learned S25 decision tree rules.

    Output:
      dict[job_index -> decision]

    Decisions (must match PassengerType labels exactly):
      - "No Vehicle"
      - "Ambulift Only"
      - "Mini Bus Only"
      - "Both"

    IMPORTANT:
    This logic is intentionally verbose to preserve the exact structure
    of the learned decision tree. Do not refactor unless retraining.

    INCLUDES:
    - Time-aware job handling using sla_start_time
    - Fixed 35-minute job duration
    - Concurrent job calculation (capacity awareness)
    """

    decisions = {}

    # =========================================================
    # NEW: Time setup (NON-INVASIVE ADDITION)
    # =========================================================

    jobs = jobs.copy()

    DURATION_MINS = 35

    jobs["job_start_time"] = pd.to_datetime(jobs["sla_start_time"])
    jobs["job_end_time"] = jobs["job_start_time"] + pd.Timedelta(minutes=DURATION_MINS)

    # =========================================================
    # EXISTING POLICY LOGIC (UNCHANGED)
    # =========================================================

    for j, r in jobs.iterrows():

        # Helper reads (keeps code shorter / consistent)
        SSR = float(r["SSR numeric"])
        remote = float(r["IsEffectiveRemote"])
        stress = float(r["Concurrent Stress"])
        is_arr = float(r["IsArrival"])
        adhoc = float(r["IsAdhoc"])
        own_chair = float(r["Has Own Chair"])
        prm_flight = float(r["PRM Flight Count"])
        turn_prm = float(r["Turnaround PRM Count"])

        # --------------------------------------------------
        # ROOT: SSR numeric <= 1.50
        # --------------------------------------------------
        if SSR <= 1.50:

            if remote <= 0.50:

                if stress <= 8.50:

                    if is_arr <= 0.50:

                        if turn_prm <= 0.50:

                            if adhoc <= 0.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                        else:
                            decisions[j] = "Both"

                    else:
                        if prm_flight <= 2.50:
                            if prm_flight <= 1.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                        else:
                            if prm_flight <= 12.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                else:

                    if turn_prm <= 1.50:

                        if is_arr <= 0.50:
                            if own_chair <= 0.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                        else:
                            if prm_flight <= 2.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                    else:

                        if is_arr <= 0.50:
                            if own_chair <= 0.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "Ambulift Only"

                        else:
                            if turn_prm <= 10.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

            else:

                if prm_flight <= 2.50:

                    if is_arr <= 0.50:

                        if own_chair <= 0.50:
                            if prm_flight <= 1.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                        else:
                            if prm_flight <= 1.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                    else:
                        if prm_flight <= 1.50:
                            if stress <= 5.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"
                        else:
                            if stress <= 8.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                else:

                    if is_arr <= 0.50:

                        if own_chair <= 0.50:
                            if adhoc <= 0.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                        else:
                            if turn_prm <= 3.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                    else:

                        if stress <= 16.50:
                            if prm_flight <= 21.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Mini Bus Only"

                        else:
                            if turn_prm <= 8.50:
                                decisions[j] = "Mini Bus Only"
                            else:
                                decisions[j] = "Ambulift Only"

        else:

            if remote <= 0.50:

                if turn_prm <= 0.50:

                    if stress <= 9.50:

                        if is_arr <= 0.50:
                            if stress <= 8.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "Ambulift Only"

                        else:
                            if stress <= 6.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                    else:
                        if own_chair <= 0.50:
                            if prm_flight <= 5.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                        else:
                            if prm_flight <= 22.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Both"

                else:

                    if own_chair <= 0.50:

                        if stress <= 23.50:
                            if turn_prm <= 11.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "No Vehicle"

                        else:
                            if adhoc <= 0.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                    else:

                        if is_arr <= 0.50:
                            if turn_prm <= 4.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                        else:
                            if SSR <= 2.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

            else:

                if stress <= 12.50:

                    if turn_prm <= 0.50:

                        if is_arr <= 0.50:
                            if stress <= 11.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                        else:
                            if prm_flight <= 0.50:
                                decisions[j] = "Mini Bus Only"
                            else:
                                decisions[j] = "Ambulift Only"

                    else:

                        if stress <= 11.50:
                            if stress <= 6.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                        else:
                            if prm_flight <= 3.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Mini Bus Only"

                else:

                    if is_arr <= 0.50:
                        if prm_flight <= 3.50:
                            if stress <= 15.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"
                        else:
                            if stress <= 16.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                    else:
                        if turn_prm <= 0.50:
                            if prm_flight <= 5.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"
                        else:
                            if stress <= 29.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

    
# =========================================================
    # NEW: Compute concurrency AFTER decisions
    # =========================================================

    # Build time window (uses sla_start_time)
    jobs_local = jobs.copy()

    jobs_local["job_start_time"] = pd.to_datetime(jobs_local["sla_start_time"])
    jobs_local["job_end_time"] = jobs_local["job_start_time"] + pd.Timedelta(minutes=35)

    def build_time_curve(jobs_df, freq="5min"):
        idx = pd.date_range(
            jobs_df["job_start_time"].min().floor(freq),
            jobs_df["job_end_time"].max().ceil(freq),
            freq=freq,
        )

        curve = pd.Series(0, index=idx)

        for _, r in jobs_df.iterrows():
            mask = (
                (curve.index >= r["job_start_time"]) &
                (curve.index < r["job_end_time"])
            )
            curve.loc[mask] += 1

        return curve

    curve = build_time_curve(jobs_local)

    # Store peak (for debugging only, does NOT affect pipeline)
    jobs.attrs["s1_peak_concurrent_jobs"] = int(curve.max())

    return decisions
