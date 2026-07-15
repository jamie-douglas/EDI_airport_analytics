# scripts/prm_opt/policy_s2_flight_rules.py

from __future__ import annotations

import math
from typing import Dict, Any

import pandas as pd


def _ceil_div(a, b) -> int:
    a = float(a)
    b = float(b)

    if b <= 0:
        return 0

    if a <= 0:
        return 0

    return int(math.ceil(a / b))


def _amb_trip_duration_mins(row, config, service_role: str) -> float:
    """
    Amb service duration per trip.

    service_role:
        - "solo": ambulift does vertical + horizontal
        - "vertical_only": ambulift does vertical component only
    """

    arr_dep = str(row.get("arr_dep", "")).upper().strip()
    is_arrival = arr_dep == "A"

    is_spin = int(row.get("is_spin", 0) or 0)
    is_spin_candidate = int(row.get("is_spin_candidate", 0) or 0)

    if is_arrival and is_spin == 1:
        return float(getattr(config, "tau_amb_spin_arr_mins", getattr(config, "tau_amb_comb_mins", 14.0)))

    if (not is_arrival) and is_spin_candidate == 1:
        return float(getattr(config, "tau_amb_spin_dep_mins", getattr(config, "tau_amb_comb_mins", 26.0)))

    if service_role == "solo":
        return float(getattr(config, "tau_amb_solo_mins", 31.0))

    return float(getattr(config, "tau_amb_comb_mins", 26.0))


def _make_interval(anchor_time, arr_dep: str, duration_mins: float):
    anchor_time = pd.to_datetime(anchor_time)
    duration = pd.to_timedelta(float(duration_mins), unit="m")

    if str(arr_dep).upper().strip() == "A":
        start = anchor_time
        end = anchor_time + duration
    else:
        end = anchor_time
        start = anchor_time - duration

    return start, end


def build_s2_flight_requirements(
    flights: pd.DataFrame,
    config,
    *,
    amb_seatcap: int = 7,
    amb_wccap: int = 1,
    mini_seatcap: int = 6,
    mini_wccap: int = 2,
    minibus_threshold_prms: int = 3,
) -> pd.DataFrame:
    """
    Build S2 flight-level rule-based vehicle requirements.

    S2 policy intent:
        - vertical demand always requires an ambulift
        - if a minibus can/should be used, use it
        - remote stands cannot use pusher/no-vehicle horizontal handling
        - safety stands cannot use pusher/no-vehicle handling
        - spin flights with vertical demand send minibuses anyway
        - if no vertical demand, use minibus for flights with >= minibus_threshold_prms PRMs
        - otherwise no vehicle

    Inputs expected from build_flights_v2:
        - flight_key
        - arr_dep
        - service_anchor_time
        - P_total
        - D_wc
        - D_seat
        - D_vert_total
        - D_vert_wc
        - D_vert_seat
        - Remote
        - EffRemote
        - NeedVertical
        - Domestic
        - Safety
        - is_spin
        - is_spin_candidate
    """

    if flights is None or len(flights) == 0:
        return pd.DataFrame()

    f = flights.copy()

    required_cols = [
        "flight_key",
        "arr_dep",
        "service_anchor_time",
        "P_total",
        "D_wc",
        "D_seat",
        "D_vert_total",
        "D_vert_wc",
        "D_vert_seat",
        "Remote",
        "EffRemote",
        "NeedVertical",
        "Domestic",
        "Safety",
    ]

    missing = [c for c in required_cols if c not in f.columns]
    if missing:
        raise ValueError(f"S2 flight rules missing required columns: {missing}")

    f["service_anchor_time"] = pd.to_datetime(f["service_anchor_time"])

    numeric_cols = [
        "P_total",
        "D_wc",
        "D_seat",
        "D_vert_total",
        "D_vert_wc",
        "D_vert_seat",
        "Remote",
        "EffRemote",
        "NeedVertical",
        "Domestic",
        "Safety",
        "is_spin",
        "is_spin_candidate",
    ]

    for c in numeric_cols:
        if c not in f.columns:
            f[c] = 0

        f[c] = pd.to_numeric(f[c], errors="coerce").fillna(0).astype(int)

    rows = []

    for _, row in f.iterrows():

        flight_key = row["flight_key"]
        arr_dep = str(row["arr_dep"]).upper().strip()
        anchor = pd.to_datetime(row["service_anchor_time"])

        total_prms = int(row["P_total"])
        total_wc_spaces = int(row["D_wc"])
        total_seated = int(row["D_seat"])

        vertical_prms = int(row["D_vert_total"])
        vertical_wc_spaces = int(row["D_vert_wc"])
        vertical_seated = int(row["D_vert_seat"])

        actual_remote = int(row["Remote"])
        effective_remote = int(row["EffRemote"])
        need_vertical = int(row["NeedVertical"])

        is_spin = int(row.get("is_spin", 0) or 0)
        is_spin_candidate = int(row.get("is_spin_candidate", 0) or 0)
        spin_related = int((is_spin == 1) or (is_spin_candidate == 1))

        domestic = int(row["Domestic"])
        safety = int(row["Safety"])

        high_volume = total_prms >= int(minibus_threshold_prms)

        has_vertical = need_vertical == 1 and vertical_prms > 0

        amb_required = 0
        mini_required = 0

        amb_trips = 0
        mini_trips = 0

        amb_service_role = None
        mini_service_role = None
        rule_reason = None

        # --------------------------------------------------
        # CASE 1: vertical required
        # --------------------------------------------------
        if has_vertical:

            amb_required = 1

            # Single-cycle test for ambulift-only operation.
            #
            # For ambulift-only, the ambulift is assumed to carry the full
            # flight PRM load, so use total demand.
            amb_can_take_all_in_one_cycle = (
                total_wc_spaces <= int(amb_wccap)
                and total_prms <= int(amb_seatcap)
            )

            # For combined operation, ambulift only handles the vertical
            # component. Minibus handles horizontal passenger movement.
            vertical_trips_needed = max(
                _ceil_div(vertical_seated, amb_seatcap),
                _ceil_div(vertical_wc_spaces, amb_wccap),
            )

            solo_trips_needed = max(
                _ceil_div(total_seated, amb_seatcap),
                _ceil_div(total_wc_spaces, amb_wccap),
            )

            # Spin: send minibus anyway.
            if spin_related == 1:
                mini_required = max(
                    _ceil_div(total_seated, mini_seatcap),
                    _ceil_div(total_wc_spaces, mini_wccap),
                )
                amb_trips = max(1, vertical_trips_needed)
                mini_trips = max(1, mini_required)
                amb_service_role = "vertical_only"
                mini_service_role = "horizontal_after_vertical"
                rule_reason = "vertical_spin_send_minibus"

            # Safety stand: no pusher/no-vehicle horizontal handling.
            elif safety == 1:
                if amb_can_take_all_in_one_cycle and not high_volume:
                    mini_required = 0
                    amb_trips = max(1, solo_trips_needed)
                    amb_service_role = "solo_vertical_and_horizontal"
                    rule_reason = "vertical_safety_single_cycle_amb_only"
                else:
                    mini_required = max(
                        _ceil_div(total_seated, mini_seatcap),
                        _ceil_div(total_wc_spaces, mini_wccap),
                    )
                    amb_trips = max(1, vertical_trips_needed)
                    mini_trips = max(1, mini_required)
                    amb_service_role = "vertical_only"
                    mini_service_role = "horizontal_after_vertical"
                    rule_reason = "vertical_safety_combined_minibus"

            # True remote stand: cannot use push/no-vehicle horizontal section.
            elif actual_remote == 1:
                if amb_can_take_all_in_one_cycle:
                    mini_required = 0
                    amb_trips = max(1, solo_trips_needed)
                    amb_service_role = "solo_vertical_and_horizontal"
                    rule_reason = "vertical_remote_single_cycle_amb_only"
                else:
                    mini_required = max(
                        _ceil_div(total_seated, mini_seatcap),
                        _ceil_div(total_wc_spaces, mini_wccap),
                    )
                    amb_trips = max(1, vertical_trips_needed)
                    mini_trips = max(1, mini_required)
                    amb_service_role = "vertical_only"
                    mini_service_role = "horizontal_after_vertical"
                    rule_reason = "vertical_remote_multi_cycle_combined"

            # Effective remote but not true remote.
            # This covers contact/no-jetbridge style cases.
            elif effective_remote == 1:
                if high_volume:
                    mini_required = max(
                        _ceil_div(total_seated, mini_seatcap),
                        _ceil_div(total_wc_spaces, mini_wccap),
                    )
                    amb_trips = max(1, vertical_trips_needed)
                    mini_trips = max(1, mini_required)
                    amb_service_role = "vertical_only"
                    mini_service_role = "horizontal_after_vertical"
                    rule_reason = "vertical_effective_remote_high_volume_combined"
                else:
                    mini_required = 0
                    amb_trips = max(1, vertical_trips_needed)
                    amb_service_role = "vertical_only"
                    rule_reason = "vertical_effective_remote_low_volume_amb_only"

            # Domestic arrival modifier.
            elif domestic == 1 and arr_dep == "A":
                if high_volume:
                    mini_required = max(
                        _ceil_div(total_seated, mini_seatcap),
                        _ceil_div(total_wc_spaces, mini_wccap),
                    )
                    amb_trips = max(1, vertical_trips_needed)
                    mini_trips = max(1, mini_required)
                    amb_service_role = "vertical_only"
                    mini_service_role = "horizontal_after_vertical"
                    rule_reason = "vertical_domestic_arrival_high_volume_combined"
                else:
                    mini_required = 0
                    amb_trips = max(1, vertical_trips_needed)
                    amb_service_role = "vertical_only"
                    rule_reason = "vertical_domestic_arrival_low_volume_amb_only"

            # Fallback vertical case.
            else:
                if high_volume:
                    mini_required = max(
                        _ceil_div(total_seated, mini_seatcap),
                        _ceil_div(total_wc_spaces, mini_wccap),
                    )
                    amb_trips = max(1, vertical_trips_needed)
                    mini_trips = max(1, mini_required)
                    amb_service_role = "vertical_only"
                    mini_service_role = "horizontal_after_vertical"
                    rule_reason = "vertical_fallback_high_volume_combined"
                else:
                    mini_required = 0
                    amb_trips = max(1, vertical_trips_needed)
                    amb_service_role = "vertical_only"
                    rule_reason = "vertical_fallback_low_volume_amb_only"

        # --------------------------------------------------
        # CASE 2: no vertical required
        # --------------------------------------------------
        else:

            amb_required = 0
            amb_trips = 0
            amb_service_role = None

            # If there is no vertical demand, a safety stand does not
            # automatically require a vehicle.
            #
            # Safety stands only matter when there is a vertical/ambulift
            # component, because push/no-vehicle cannot be used as the
            # horizontal part of a combined ambulift movement.
            #
            # Therefore:
            #   - low-volume non-vertical safety flight can still be no vehicle
            #   - high-volume non-vertical safety flight gets a minibus
            if safety == 1:

                if high_volume:
                    mini_required = max(
                        _ceil_div(total_seated, mini_seatcap),
                        _ceil_div(total_wc_spaces, mini_wccap),
                    )
                    mini_required = max(1, mini_required)
                    mini_trips = mini_required
                    mini_service_role = "solo_horizontal"
                    rule_reason = "non_vertical_safety_high_volume_minibus"

                else:
                    mini_required = 0
                    mini_trips = 0
                    mini_service_role = None
                    rule_reason = "non_vertical_safety_low_volume_no_vehicle"

            elif actual_remote == 1:

                # True remote stand: cannot use pusher/no-vehicle.
                # If no vertical is needed, minibus is the vehicle option.
                mini_required = max(
                    _ceil_div(total_seated, mini_seatcap),
                    _ceil_div(total_wc_spaces, mini_wccap),
                )
                mini_required = max(1, mini_required)
                mini_trips = mini_required
                mini_service_role = "solo_horizontal"
                rule_reason = "non_vertical_remote_minibus"

            elif high_volume:

                mini_required = max(
                    _ceil_div(total_seated, mini_seatcap),
                    _ceil_div(total_wc_spaces, mini_wccap),
                )
                mini_required = max(1, mini_required)
                mini_trips = mini_required
                mini_service_role = "solo_horizontal"
                rule_reason = "non_vertical_high_volume_minibus"

            else:

                mini_required = 0
                mini_trips = 0
                mini_service_role = None
                rule_reason = "non_vertical_low_volume_no_vehicle"

        # --------------------------------------------------
        # Durations and intervals
        # --------------------------------------------------
        amb_trip_mins = 0.0
        amb_duration_mins = 0.0
        amb_start = pd.NaT
        amb_end = pd.NaT

        if amb_required > 0:
            service_role_for_duration = (
                "solo"
                if amb_service_role == "solo_vertical_and_horizontal"
                else "vertical_only"
            )

            amb_trip_mins = _amb_trip_duration_mins(
                row,
                config,
                service_role_for_duration,
            )

            amb_duration_mins = float(amb_trips) * float(amb_trip_mins)

            amb_start, amb_end = _make_interval(
                anchor,
                arr_dep,
                amb_duration_mins,
            )

        mini_trip_mins = float(getattr(config, "tau_mini_mins", 23.0))
        handover_mins = float(getattr(config, "handover_buffer_mins", 5.0))

        mini_duration_mins = 0.0
        mini_start = pd.NaT
        mini_end = pd.NaT

        if mini_required > 0:

            if mini_service_role == "horizontal_after_vertical":

                mini_duration_mins = handover_mins + mini_trip_mins

                if arr_dep == "A":
                    # Minibus starts near the end of the vertical process.
                    if pd.isna(amb_end):
                        mini_start = anchor
                    else:
                        mini_start = amb_end - pd.to_timedelta(handover_mins, unit="m")

                    mini_end = mini_start + pd.to_timedelta(mini_duration_mins, unit="m")

                else:
                    # For departures, treat minibus/handover as final block before boarding anchor.
                    mini_end = anchor
                    mini_start = mini_end - pd.to_timedelta(mini_duration_mins, unit="m")

            else:
                mini_duration_mins = mini_trip_mins

                mini_start, mini_end = _make_interval(
                    anchor,
                    arr_dep,
                    mini_duration_mins,
                )

        rows.append(
            {
                "flight_key": flight_key,
                "arr_dep": arr_dep,
                "scheduled_time": row.get("scheduled_time", pd.NaT),
                "service_anchor_time": anchor,

                "P_total": total_prms,
                "D_seat": total_seated,
                "D_wc": total_wc_spaces,
                "D_vert_total": vertical_prms,
                "D_vert_seat": vertical_seated,
                "D_vert_wc": vertical_wc_spaces,

                "Remote": actual_remote,
                "EffRemote": effective_remote,
                "NeedVertical": need_vertical,
                "Domestic": domestic,
                "Safety": safety,
                "is_spin": is_spin,
                "is_spin_candidate": is_spin_candidate,

                "amb_required": int(amb_required),
                "mini_required": int(mini_required),

                "amb_trips": int(amb_trips),
                "mini_trips": int(mini_trips),

                "amb_service_role": amb_service_role,
                "mini_service_role": mini_service_role,

                "amb_trip_duration_mins": float(amb_trip_mins),
                "amb_duration_mins": float(amb_duration_mins),
                "mini_duration_mins": float(mini_duration_mins),

                "amb_start": amb_start,
                "amb_end": amb_end,
                "mini_start": mini_start,
                "mini_end": mini_end,

                "rule_reason": rule_reason,
            }
        )

    return pd.DataFrame(rows)


def build_s2_vehicle_curves(
    s2_requirements: pd.DataFrame,
    *,
    time_freq: str = "5min",
) -> Dict[str, Any]:
    """
    Convert S2 flight-level requirements into active vehicle curves.

    Returns:
        - ambulift_curve
        - minibus_curve
        - driver_curve
        - veh_agent_curve
        - event_detail
        - summary
    """

    if s2_requirements is None or len(s2_requirements) == 0:
        empty = pd.Series(dtype=int)

        return {
            "ambulift_curve": empty,
            "minibus_curve": empty,
            "driver_curve": empty,
            "veh_agent_curve": empty,
            "event_detail": pd.DataFrame(),
            "summary": {
                "PeakAmb": 0,
                "PeakMini": 0,
                "PeakDrivers": 0,
                "PeakVehAgents": 0,
            },
        }

    req = s2_requirements.copy()

    time_cols = ["amb_start", "amb_end", "mini_start", "mini_end"]

    for c in time_cols:
        if c in req.columns:
            req[c] = pd.to_datetime(req[c], errors="coerce")

    starts = []
    ends = []

    if "amb_start" in req.columns:
        starts.extend(req.loc[req["amb_required"] > 0, "amb_start"].dropna().tolist())
        ends.extend(req.loc[req["amb_required"] > 0, "amb_end"].dropna().tolist())

    if "mini_start" in req.columns:
        starts.extend(req.loc[req["mini_required"] > 0, "mini_start"].dropna().tolist())
        ends.extend(req.loc[req["mini_required"] > 0, "mini_end"].dropna().tolist())

    if len(starts) == 0 or len(ends) == 0:
        empty = pd.Series(dtype=int)

        return {
            "ambulift_curve": empty,
            "minibus_curve": empty,
            "driver_curve": empty,
            "veh_agent_curve": empty,
            "event_detail": pd.DataFrame(),
            "summary": {
                "PeakAmb": 0,
                "PeakMini": 0,
                "PeakDrivers": 0,
                "PeakVehAgents": 0,
            },
        }

    start = pd.Series(starts).min().floor(time_freq)
    end = pd.Series(ends).max().ceil(time_freq)

    time_index = pd.date_range(
        start=start,
        end=end,
        freq=time_freq,
    )

    curve_rows = []
    detail_rows = []

    for ts in time_index:

        amb_active = req[
            (req["amb_required"] > 0)
            & (req["amb_start"] <= ts)
            & (ts < req["amb_end"])
        ].copy()

        mini_active = req[
            (req["mini_required"] > 0)
            & (req["mini_start"] <= ts)
            & (ts < req["mini_end"])
        ].copy()

        amb_count = int(amb_active["amb_required"].sum()) if len(amb_active) else 0
        mini_count = int(mini_active["mini_required"].sum()) if len(mini_active) else 0

        curve_rows.append(
            {
                "timestamp": ts,
                "Amb_req": amb_count,
                "Mini_req": mini_count,
                "Drivers_req": amb_count + mini_count,
                "VehAgents_req": amb_count + mini_count,
            }
        )

        for _, row in amb_active.iterrows():
            detail_rows.append(
                {
                    "timestamp": ts,
                    "flight_key": row["flight_key"],
                    "vehicle_type": "Amb",
                    "vehicles_required": int(row["amb_required"]),
                    "rule_reason": row["rule_reason"],
                }
            )

        for _, row in mini_active.iterrows():
            detail_rows.append(
                {
                    "timestamp": ts,
                    "flight_key": row["flight_key"],
                    "vehicle_type": "Mini",
                    "vehicles_required": int(row["mini_required"]),
                    "rule_reason": row["rule_reason"],
                }
            )

    curve_df = pd.DataFrame(curve_rows).set_index("timestamp").sort_index()

    amb_curve = curve_df["Amb_req"].astype(int)
    mini_curve = curve_df["Mini_req"].astype(int)
    driver_curve = curve_df["Drivers_req"].astype(int)
    veh_agent_curve = curve_df["VehAgents_req"].astype(int)

    event_detail = pd.DataFrame(detail_rows)

    summary = {
        "PeakAmb": int(amb_curve.max()) if len(amb_curve) else 0,
        "PeakMini": int(mini_curve.max()) if len(mini_curve) else 0,
        "PeakDrivers": int(driver_curve.max()) if len(driver_curve) else 0,
        "PeakVehAgents": int(veh_agent_curve.max()) if len(veh_agent_curve) else 0,
    }

    return {
        "ambulift_curve": amb_curve,
        "minibus_curve": mini_curve,
        "driver_curve": driver_curve,
        "veh_agent_curve": veh_agent_curve,
        "event_detail": event_detail,
        "summary": summary,
        "curve_df": curve_df.reset_index(),
    }


def run_s2_flight_rules(
    flights: pd.DataFrame,
    config,
    *,
    amb_seatcap: int = 7,
    amb_wccap: int = 1,
    mini_seatcap: int = 6,
    mini_wccap: int = 2,
    minibus_threshold_prms: int = 3,
    time_freq: str = "5min",
) -> Dict[str, Any]:
    """
    End-to-end S2 flight-level rule runner.
    """

    requirements = build_s2_flight_requirements(
        flights=flights,
        config=config,
        amb_seatcap=amb_seatcap,
        amb_wccap=amb_wccap,
        mini_seatcap=mini_seatcap,
        mini_wccap=mini_wccap,
        minibus_threshold_prms=minibus_threshold_prms,
    )

    curves = build_s2_vehicle_curves(
        requirements,
        time_freq=time_freq,
    )

    return {
        "flight_requirements": requirements,
        "summary": curves["summary"],
        "ambulift_curve": curves["ambulift_curve"],
        "minibus_curve": curves["minibus_curve"],
        "driver_curve": curves["driver_curve"],
        "veh_agent_curve": curves["veh_agent_curve"],
        "event_detail": curves["event_detail"],
        "curve_df": curves["curve_df"],
    }