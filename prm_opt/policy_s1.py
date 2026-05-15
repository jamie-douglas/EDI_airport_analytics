
# scripts/prm_opt/policy_s1.py

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

    """

    decisions = {}

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

            # ----------------------------------------------
            # IsEffectiveRemote <= 0.50
            # ----------------------------------------------
            if remote <= 0.50:

                # Concurrent Stress <= 8.50
                if stress <= 8.50:

                    # IsArrival <= 0.50
                    if is_arr <= 0.50:

                        # Turnaround PRM Count <= 0.50
                        if turn_prm <= 0.50:

                            # IsAdhoc <= 0.50  -> No Vehicle
                            if adhoc <= 0.50:
                                decisions[j] = "No Vehicle"
                            # IsAdhoc > 0.50   -> No Vehicle
                            else:
                                decisions[j] = "No Vehicle"

                        # Turnaround PRM Count > 0.50 -> Both
                        else:
                            decisions[j] = "Both"

                    # IsArrival > 0.50
                    else:
                        # PRM Flight Count <= 2.50 -> No Vehicle (both sub-branches are No Vehicle)
                        if prm_flight <= 2.50:
                            # PRM Flight Count <= 1.50 -> No Vehicle
                            if prm_flight <= 1.50:
                                decisions[j] = "No Vehicle"
                            # PRM Flight Count > 1.50 -> No Vehicle
                            else:
                                decisions[j] = "No Vehicle"

                        # PRM Flight Count > 2.50 -> Ambulift Only (both sub-branches are Ambulift Only)
                        else:
                            # PRM Flight Count <= 12.50 -> Ambulift Only
                            if prm_flight <= 12.50:
                                decisions[j] = "Ambulift Only"
                            # PRM Flight Count > 12.50 -> Ambulift Only
                            else:
                                decisions[j] = "Ambulift Only"

                # Concurrent Stress > 8.50
                else:

                    # Turnaround PRM Count <= 1.50
                    if turn_prm <= 1.50:

                        # IsArrival <= 0.50 -> No Vehicle (both own-chair branches are No Vehicle)
                        if is_arr <= 0.50:
                            if own_chair <= 0.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                        # IsArrival > 0.50 -> No Vehicle (both PRM count branches are No Vehicle)
                        else:
                            if prm_flight <= 2.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                    # Turnaround PRM Count > 1.50
                    else:

                        # IsArrival <= 0.50
                        if is_arr <= 0.50:
                            # Has Own Chair <= 0.50 -> No Vehicle
                            if own_chair <= 0.50:
                                decisions[j] = "No Vehicle"
                            # Has Own Chair > 0.50 -> Ambulift Only
                            else:
                                decisions[j] = "Ambulift Only"

                        # IsArrival > 0.50 -> No Vehicle (both turnaround branches are No Vehicle)
                        else:
                            if turn_prm <= 10.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

            # ----------------------------------------------
            # IsEffectiveRemote > 0.50
            # ----------------------------------------------
            else:

                # PRM Flight Count <= 2.50
                if prm_flight <= 2.50:

                    # IsArrival <= 0.50
                    if is_arr <= 0.50:

                        # Has Own Chair <= 0.50 -> No Vehicle (both PRM count branches are No Vehicle)
                        if own_chair <= 0.50:
                            if prm_flight <= 1.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                        # Has Own Chair > 0.50 -> Ambulift Only (both PRM count branches are Ambulift Only)
                        else:
                            if prm_flight <= 1.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                    # IsArrival > 0.50 -> No Vehicle (all sub-branches are No Vehicle)
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

                # PRM Flight Count > 2.50
                else:

                    # IsArrival <= 0.50
                    if is_arr <= 0.50:

                        # Has Own Chair <= 0.50 -> No Vehicle (both adhoc branches No Vehicle)
                        if own_chair <= 0.50:
                            if adhoc <= 0.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                        # Has Own Chair > 0.50 -> Ambulift Only (both turnaround branches Ambulift Only)
                        else:
                            if turn_prm <= 3.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                    # IsArrival > 0.50
                    else:

                        # Concurrent Stress <= 16.50
                        if stress <= 16.50:
                            # PRM Flight Count <= 21.50 -> Ambulift Only
                            if prm_flight <= 21.50:
                                decisions[j] = "Ambulift Only"
                            # PRM Flight Count > 21.50 -> Mini Bus Only
                            else:
                                decisions[j] = "Mini Bus Only"

                        # Concurrent Stress > 16.50
                        else:
                            # Turnaround PRM Count <= 8.50 -> Mini Bus Only
                            if turn_prm <= 8.50:
                                decisions[j] = "Mini Bus Only"
                            # Turnaround PRM Count > 8.50 -> Ambulift Only
                            else:
                                decisions[j] = "Ambulift Only"

        # --------------------------------------------------
        # ROOT: SSR numeric > 1.50
        # --------------------------------------------------
        else:

            # ----------------------------------------------
            # IsEffectiveRemote <= 0.50
            # ----------------------------------------------
            if remote <= 0.50:

                # Turnaround PRM Count <= 0.50
                if turn_prm <= 0.50:

                    # Concurrent Stress <= 9.50
                    if stress <= 9.50:

                        # IsArrival <= 0.50
                        if is_arr <= 0.50:
                            # Concurrent Stress <= 8.50 -> No Vehicle
                            if stress <= 8.50:
                                decisions[j] = "No Vehicle"
                            # Concurrent Stress > 8.50 -> Ambulift Only
                            else:
                                decisions[j] = "Ambulift Only"

                        # IsArrival > 0.50 -> Ambulift Only (both branches)
                        else:
                            if stress <= 6.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                    # Concurrent Stress > 9.50
                    else:
                        # Has Own Chair <= 0.50 -> No Vehicle (both PRM flight branches)
                        if own_chair <= 0.50:
                            if prm_flight <= 5.50:
                                decisions[j] = "No Vehicle"
                            else:
                                decisions[j] = "No Vehicle"

                        # Has Own Chair > 0.50
                        else:
                            # PRM Flight Count <= 22.50 -> Ambulift Only
                            if prm_flight <= 22.50:
                                decisions[j] = "Ambulift Only"
                            # PRM Flight Count > 22.50 -> Both
                            else:
                                decisions[j] = "Both"

                # Turnaround PRM Count > 0.50
                else:

                    # Has Own Chair <= 0.50
                    if own_chair <= 0.50:

                        # Concurrent Stress <= 23.50
                        if stress <= 23.50:
                            # Turnaround PRM Count <= 11.50 -> Ambulift Only
                            if turn_prm <= 11.50:
                                decisions[j] = "Ambulift Only"
                            # Turnaround PRM Count > 11.50 -> No Vehicle
                            else:
                                decisions[j] = "No Vehicle"

                        # Concurrent Stress > 23.50 -> Ambulift Only (both adhoc branches)
                        else:
                            if adhoc <= 0.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                    # Has Own Chair > 0.50
                    else:

                        # IsArrival <= 0.50 -> Ambulift Only (both turnaround branches)
                        if is_arr <= 0.50:
                            if turn_prm <= 4.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                        # IsArrival > 0.50 -> Ambulift Only (SSR split does not change class)
                        else:
                            if SSR <= 2.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

            # ----------------------------------------------
            # IsEffectiveRemote > 0.50
            # ----------------------------------------------
            else:

                # Concurrent Stress <= 12.50
                if stress <= 12.50:

                    # Turnaround PRM Count <= 0.50
                    if turn_prm <= 0.50:

                        # IsArrival <= 0.50 -> Ambulift Only (both stress branches)
                        if is_arr <= 0.50:
                            if stress <= 11.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                        # IsArrival > 0.50
                        else:
                            # PRM Flight Count <= 0.50 -> Mini Bus Only
                            if prm_flight <= 0.50:
                                decisions[j] = "Mini Bus Only"
                            # PRM Flight Count > 0.50 -> Ambulift Only
                            else:
                                decisions[j] = "Ambulift Only"

                    # Turnaround PRM Count > 0.50
                    else:

                        # Concurrent Stress <= 11.50 -> Ambulift Only (both sub-branches)
                        if stress <= 11.50:
                            if stress <= 6.50:
                                decisions[j] = "Ambulift Only"
                            else:
                                decisions[j] = "Ambulift Only"

                        # Concurrent Stress > 11.50
                        else:
                            # PRM Flight Count <= 3.50 -> Ambulift Only
                            if prm_flight <= 3.50:
                                decisions[j] = "Ambulift Only"
                            # PRM Flight Count > 3.50 -> Mini Bus Only
                            else:
                                decisions[j] = "Mini Bus Only"

                # Concurrent Stress > 12.50
                else:

                    # IsArrival <= 0.50 -> Ambulift Only (all sub-branches)
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

                    # IsArrival > 0.50 -> Ambulift Only (all sub-branches)
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

    return decisions
