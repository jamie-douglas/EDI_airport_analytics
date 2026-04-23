# scripts/prm_opt/policy_s1.py


def apply_policy_s1(jobs):
    
    """
    Apply Scenario 1 (S25 baseline) decision policy.

    This function encodes a decision tree learned from S25 data
    and represents how vehicles were actually used operationally.

    Output:
      dict[job_index -> decision]

    Decisions:
      - "No Vehicle"
      - "Ambulift Only"
    """

    decisions = {}

    for j, r in jobs.iterrows():

        # --------------------------------------------------
        # SSR numeric <= 1.5
        # --------------------------------------------------
        if r["SSR numeric"] <= 1.5:

            # ---- Not effective remote ----
            if r["IsEffectiveRemote"] <= 0.5:

                if r["Concurrent Stress"] <= 8.5:
                    decisions[j] = "No Vehicle"
                else:
                    if r["Turnaround PRM Count"] <= 3.5:
                        decisions[j] = "No Vehicle"
                    else:
                        decisions[j] = "No Vehicle"

            # ---- Effective remote ----
            else:
                if r["PRM Flight Count"] <= 2.5:

                    if r["Has Own Chair"] <= 0.5:
                        decisions[j] = "No Vehicle"
                    else:
                        decisions[j] = "Ambulift Only"

                else:
                    if r["IsArrival"] <= 0.5:
                        decisions[j] = "No Vehicle"
                    else:
                        decisions[j] = "Ambulift Only"

        # --------------------------------------------------
        # SSR numeric > 1.5
        # --------------------------------------------------
        else:

            # ---- Not effective remote ----
            if r["IsEffectiveRemote"] <= 0.5:

                if r["Concurrent Stress"] <= 6.5:
                    if r["IsArrival"] <= 0.5:
                        decisions[j] = "No Vehicle"
                    else:
                        decisions[j] = "Ambulift Only"
                else:
                    if r["Turnaround PRM Count"] <= 0.5:
                        decisions[j] = "No Vehicle"
                    else:
                        decisions[j] = "Ambulift Only"

            # ---- Effective remote ----
            else:
                if r["IsArrival"] <= 0.5:
                    decisions[j] = "Ambulift Only"
                else:
                    if r["Turnaround PRM Count"] <= 13.5:
                        decisions[j] = "Ambulift Only"
                    else:
                        decisions[j] = "No Vehicle"

    return decisions
