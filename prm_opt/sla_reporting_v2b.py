# scripts/prm_opt/sla_reporting_v2b.py

from __future__ import annotations

from typing import Dict, Any

import numpy as np
import pandas as pd


"""
Passenger-based SLA reporting for PRM fleet optimiser V2B.

This file is designed to sit on top of optimise_prm_fleet_v2.py.

The current optimiser V2A is a fixed-window named-vehicle assignment model.
It produces:
    - flight_assignments
    - vehicle_schedule
    - shortfalls
    - fleet_summary
    - staff_summary
    - solver_results

This V2B layer converts those outputs into passenger-based SLA metrics.

Important:
---------
EDI SLA should be measured by PRMs, not by flights.

If an arrival flight breaches, then all PRMs on that flight are counted as
breached PRMs:

    arrival_prm_breaches = P_total * arrival_breach

This gives:

    arrival_sla_pct =
        100 * (1 - arrival_prm_breaches / total_arrival_prms)

Current breach logic:
---------------------
An arrival flight is marked as breached if either:

1) It has any optimiser shortfall, meaning the model could not cover the
   required vehicle/capacity requirement for the selected/possible service.

2) Its first assigned vehicle service_start is later than:
       scheduled_time + arrival_sla_target_mins

For the current fixed-window model, condition 2 will normally be false because
arrival intervals start at scheduled_time. So in V2B, SLA breaches are mainly
driven by shortfalls.

This is a practical V2B bridge before implementing a full movable-start-time
dispatch model.
"""


def _get_config_value(config: Any, names: list[str], default):
    """
    Helper to support either:
        arrival_sla_target_pct
    or:
        arrival_prm_sla_target_pct
    depending on which naming is currently in the optimiser config.
    """
    for name in names:
        if hasattr(config, name):
            return getattr(config, name)
    return default


def add_passenger_sla_reporting(
    outputs: Dict[str, pd.DataFrame],
    config: Any,
) -> Dict[str, pd.DataFrame]:
    """
    Add passenger-based SLA reporting to optimiser outputs.

    Parameters
    ----------
    outputs:
        Output dictionary returned by optimise_prm_fleet_v2().

    config:
        OptimiserConfig instance.

    Returns
    -------
    outputs:
        Same dictionary, with:
            - flight_assignments updated with SLA columns
            - sla_summary replaced with passenger-based SLA summary
            - arrival_breach_details added
    """

    if "flight_assignments" not in outputs:
        raise ValueError("outputs must include 'flight_assignments'.")

    flight_out = outputs["flight_assignments"].copy()

    if "estimated_late_arrival_prms" not in flight_out.columns:
        flight_out["estimated_late_arrival_prms"] = 0

    flight_out["estimated_late_arrival_prms"] = pd.to_numeric(
        flight_out["estimated_late_arrival_prms"],
        errors="coerce",
    ).fillna(0).astype(int)

    if "shortfalls" in outputs and outputs["shortfalls"] is not None:
        shortfalls = outputs["shortfalls"].copy()
    else:
        shortfalls = pd.DataFrame()

    if "vehicle_schedule" in outputs and outputs["vehicle_schedule"] is not None:
        vehicle_schedule = outputs["vehicle_schedule"].copy()
    else:
        vehicle_schedule = pd.DataFrame()

    arrival_sla_target_mins = float(
        _get_config_value(
            config,
            ["arrival_sla_target_mins"],
            20,
        )
    )

    arrival_target_pct = float(
        _get_config_value(
            config,
            ["arrival_prm_sla_target_pct", "arrival_sla_target_pct"],
            0.98,
        )
    )

    # Defensive types.
    if "scheduled_time" in flight_out.columns:
        flight_out["scheduled_time"] = pd.to_datetime(flight_out["scheduled_time"])

    if "P_total" not in flight_out.columns:
        raise ValueError("flight_assignments must include P_total.")

    flight_out["P_total"] = pd.to_numeric(
        flight_out["P_total"],
        errors="coerce",
    ).fillna(0).astype(int)

    # --------------------------------------------------
    # Shortfall by flight
    # --------------------------------------------------
    if len(shortfalls) > 0:
        shortfall_by_flight = (
            shortfalls
            .groupby("flight_key")["shortfall_value"]
            .sum()
            .rename("total_shortfall_value")
            .reset_index()
        )
    else:
        shortfall_by_flight = pd.DataFrame(
            columns=["flight_key", "total_shortfall_value"]
        )

    flight_out = flight_out.merge(
        shortfall_by_flight,
        on="flight_key",
        how="left",
    )

    flight_out["total_shortfall_value"] = pd.to_numeric(
        flight_out["total_shortfall_value"],
        errors="coerce",
    ).fillna(0.0)

    flight_out["has_shortfall"] = (
        flight_out["total_shortfall_value"] > 1e-6
    ).astype(int)

    # --------------------------------------------------
    # Earliest assigned service start by flight
    # --------------------------------------------------
    if len(vehicle_schedule) > 0 and "service_start" in vehicle_schedule.columns:
        vehicle_schedule["service_start"] = pd.to_datetime(
            vehicle_schedule["service_start"]
        )

        first_service = (
            vehicle_schedule
            .groupby("flight_key")["service_start"]
            .min()
            .rename("first_assigned_service_start")
            .reset_index()
        )

        flight_out = flight_out.merge(
            first_service,
            on="flight_key",
            how="left",
        )
    else:
        flight_out["first_assigned_service_start"] = pd.NaT

    # For flights with no vehicle assignment, use service anchor if available.
    # This avoids false missing values for pusher-only flights.
    if "service_anchor_time" in flight_out.columns:
        flight_out["service_anchor_time"] = pd.to_datetime(
            flight_out["service_anchor_time"]
        )
        flight_out["first_assigned_service_start"] = flight_out[
            "first_assigned_service_start"
        ].fillna(flight_out["service_anchor_time"])

    # --------------------------------------------------
    # Arrival delay and breach
    # --------------------------------------------------
    arrival_mask = flight_out["arr_dep"].astype(str).eq("A")

    flight_out["arrival_delay_mins"] = np.nan

    valid_arrival_start = (
        arrival_mask
        & flight_out["scheduled_time"].notna()
        & flight_out["first_assigned_service_start"].notna()
    )

    flight_out.loc[valid_arrival_start, "arrival_delay_mins"] = (
        flight_out.loc[valid_arrival_start, "first_assigned_service_start"]
        - flight_out.loc[valid_arrival_start, "scheduled_time"]
    ).dt.total_seconds() / 60.0

    flight_out["arrival_prm_breaches"] = np.where(
        arrival_mask,
        flight_out["estimated_late_arrival_prms"],
        0,
    ).astype(int)
    
    flight_out["arrival_breach"] = np.where(
        arrival_mask & (flight_out["arrival_prm_breaches"] > 0),
        1,
        0,
    ).astype(int)

    flight_out.loc[
        arrival_mask
        & (
            (flight_out["has_shortfall"] == 1)
            | (
                flight_out["arrival_delay_mins"].fillna(0.0)
                > arrival_sla_target_mins
            )
        ),
        "arrival_breach",
    ] = 1

    # Departures are not treated as arrival SLA breaches.
    # We keep a separate departure breach flag for reporting/debugging.
    flight_out["departure_breach"] = 0

    # --------------------------------------------------
    # Passenger-based breach counts
    # --------------------------------------------------

    flight_out["departure_prm_breaches"] = 0

    arrival_prms = int(
        flight_out.loc[arrival_mask, "P_total"].sum()
    )

    arrival_breached_prms = int(
        flight_out["arrival_prm_breaches"].sum()
    )

    departure_mask = flight_out["arr_dep"].astype(str).eq("D")

    departure_prms = int(
        flight_out.loc[departure_mask, "P_total"].sum()
    )

    departure_breached_prms = int(
        flight_out["departure_prm_breaches"].sum()
    )

    arrival_sla_pct = (
        100.0
        if arrival_prms == 0
        else 100.0 * (1.0 - arrival_breached_prms / arrival_prms)
    )

    departure_sla_pct = (
        100.0
        if departure_prms == 0
        else 100.0 * (1.0 - departure_breached_prms / departure_prms)
    )

    arrival_breached_flights = int(
        flight_out.loc[arrival_mask, "arrival_breach"].sum()
    )

    arrival_flights = int(arrival_mask.sum())

    departure_flights = int(departure_mask.sum())

    # --------------------------------------------------
    # SLA summary
    # --------------------------------------------------
    sla_summary = pd.DataFrame(
        [
            {
                "arrival_flights": arrival_flights,
                "arrival_breached_flights": arrival_breached_flights,
                "arrival_prms": arrival_prms,
                "arrival_prm_breaches": arrival_breached_prms,
                "arrival_sla_pct": arrival_sla_pct,
                "arrival_target_pct": arrival_target_pct * 100.0,
                "arrival_meets_target": int(
                    arrival_sla_pct >= arrival_target_pct * 100.0
                ),
                "arrival_sla_target_mins": arrival_sla_target_mins,
                "departure_flights": departure_flights,
                "departure_prms": departure_prms,
                "departure_prm_breaches": departure_breached_prms,
                "departure_sla_pct": departure_sla_pct,
            }
        ]
    )

    arrival_breach_details = flight_out[
        arrival_mask & (flight_out["arrival_breach"] == 1)
    ].copy()

    outputs["flight_assignments"] = flight_out
    outputs["sla_summary"] = sla_summary
    outputs["arrival_breach_details"] = arrival_breach_details

    return outputs