# scripts/prm_opt/fleet_requirements_report_v2c.py

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from typing import Dict, Any, Iterable

import pandas as pd

from prm_opt.optimise_prm_fleet_v2 import (
    OptimiserConfig,
    optimise_prm_fleet_v2,
)

from prm_opt.sla_reporting_v2b import add_passenger_sla_reporting


"""
Fleet requirements report for PRM fleet optimisation V2C.

Purpose
-------
Runs multiple fleet scenarios and identifies which scenario achieves the
passenger-based arrival SLA target.

This sits on top of:
    - build_flights_v2.py
    - optimise_prm_fleet_v2.py
    - sla_reporting_v2b.py

Important:
---------
This uses the current V2A optimiser plus V2B passenger-SLA reporting.

Therefore, in this version, SLA risk is mainly driven by optimiser shortfalls.
A proper future V2D could add movable service start decisions directly into
the Pyomo model.

Outputs
-------
For each scenario:
    - arrival PRMs
    - arrival PRM breaches
    - arrival SLA %
    - target %
    - meets target?
    - future minibuses bought
    - current/future fleet used
    - shortfall totals
    - solver status
    - objective value
"""


def _base_current_fleet_only(
    vehicle_models: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """
    Return only current vehicles; remove all future purchase options.
    """
    return {
        k: deepcopy(v)
        for k, v in vehicle_models.items()
        if not bool(v.get("is_future", False))
    }


def _fleet_with_future_options(
    vehicle_models: Dict[str, Dict[str, Any]],
    allowed_future_models: Iterable[str],
) -> Dict[str, Dict[str, Any]]:
    """
    Return current fleet plus selected future model options.

    Example:
        allowed_future_models=["MB_EV_10"]
    """
    allowed_future_models = set(allowed_future_models)

    out = {}

    for k, v in vehicle_models.items():
        is_future = bool(v.get("is_future", False))

        if not is_future:
            out[k] = deepcopy(v)
        elif k in allowed_future_models:
            out[k] = deepcopy(v)

    return out


def _summarise_one_scenario(
    scenario_name: str,
    outputs: Dict[str, pd.DataFrame],
) -> dict:
    """
    Convert optimiser outputs into one summary row.
    """

    sla = outputs.get("sla_summary", pd.DataFrame())
    fleet = outputs.get("fleet_summary", pd.DataFrame())
    short = outputs.get("shortfalls", pd.DataFrame())
    solver = outputs.get("solver_results", pd.DataFrame())

    if len(sla) == 0:
        sla_row = {}
    else:
        sla_row = sla.iloc[0].to_dict()

    if len(fleet) == 0:
        future_bought = 0
        current_used = 0
        future_used = 0
    else:
        future_bought = int(
            fleet.loc[fleet["is_future"].astype(int) == 1, "bought"].sum()
            if "bought" in fleet.columns else 0
        )

        current_used = int(
            fleet.loc[fleet["is_future"].astype(int) == 0, "used"].sum()
            if "used" in fleet.columns else 0
        )

        future_used = int(
            fleet.loc[fleet["is_future"].astype(int) == 1, "used"].sum()
            if "used" in fleet.columns else 0
        )

    if len(short) == 0:
        shortfall_rows = 0
        shortfall_value = 0.0
    else:
        shortfall_rows = int(len(short))
        shortfall_value = float(
            pd.to_numeric(
                short["shortfall_value"],
                errors="coerce",
            ).fillna(0.0).sum()
        )

    if len(solver) == 0:
        solver_status = None
        termination_condition = None
        objective_value = None
    else:
        solver_status = solver.iloc[0].get("solver_status")
        termination_condition = solver.iloc[0].get("termination_condition")
        objective_value = solver.iloc[0].get("objective_value")

    return {
        "scenario": scenario_name,

        "arrival_prms": sla_row.get("arrival_prms"),
        "arrival_prm_breaches": sla_row.get("arrival_prm_breaches"),
        "arrival_sla_pct": sla_row.get("arrival_sla_pct"),
        "arrival_target_pct": sla_row.get("arrival_target_pct"),
        "arrival_meets_target": sla_row.get("arrival_meets_target"),
        "arrival_breached_flights": sla_row.get("arrival_breached_flights"),

        "future_minibuses_bought": future_bought,
        "future_vehicles_used": future_used,
        "current_vehicles_used": current_used,

        "shortfall_rows": shortfall_rows,
        "shortfall_value": shortfall_value,

        "solver_status": solver_status,
        "termination_condition": termination_condition,
        "objective_value": objective_value,
    }


def run_fleet_requirements_report_v2c(
    flights: pd.DataFrame,
    vehicle_models: Dict[str, Dict[str, Any]],
    base_config: OptimiserConfig | None = None,
    max_ev10: int = 5,
    max_ev18: int = 5,
    include_current_only: bool = True,
) -> tuple[pd.DataFrame, dict[str, Dict[str, pd.DataFrame]]]:
    """
    Run fleet scenarios and return:
        1) summary report dataframe
        2) detailed outputs by scenario

    Scenarios:
        - current fleet only
        - EV10 available up to 1..max_ev10
        - EV18 available up to 1..max_ev18

    The optimiser still decides whether to actually buy/use those future options.
    """

    if base_config is None:
        base_config = OptimiserConfig()

    scenario_specs = []

    if include_current_only:
        scenario_specs.append(
            {
                "scenario": "current_fleet_only",
                "allowed_future_models": [],
                "future_copies": 0,
            }
        )

    for n in range(1, int(max_ev10) + 1):
        scenario_specs.append(
            {
                "scenario": f"ev10_available_{n}",
                "allowed_future_models": ["MB_EV_10"],
                "future_copies": n,
            }
        )

    for n in range(1, int(max_ev18) + 1):
        scenario_specs.append(
            {
                "scenario": f"ev18_available_{n}",
                "allowed_future_models": ["MB_EV_18"],
                "future_copies": n,
            }
        )

    report_rows = []
    detailed_outputs = {}

    for spec in scenario_specs:
        scenario_name = spec["scenario"]

        if len(spec["allowed_future_models"]) == 0:
            scenario_vehicle_models = _base_current_fleet_only(vehicle_models)
            config = replace(
                base_config,
                max_future_copies_per_model=0,
            )
        else:
            scenario_vehicle_models = _fleet_with_future_options(
                vehicle_models,
                allowed_future_models=spec["allowed_future_models"],
            )
            config = replace(
                base_config,
                max_future_copies_per_model=int(spec["future_copies"]),
            )

        outputs = optimise_prm_fleet_v2(
            flights=flights,
            vehicle_models=scenario_vehicle_models,
            config=config,
        )

        outputs = add_passenger_sla_reporting(
            outputs=outputs,
            config=config,
        )

        detailed_outputs[scenario_name] = outputs

        report_rows.append(
            _summarise_one_scenario(
                scenario_name=scenario_name,
                outputs=outputs,
            )
        )

    report = pd.DataFrame(report_rows)

    if len(report) > 0:
        report["arrival_meets_target"] = (
            pd.to_numeric(
                report["arrival_meets_target"],
                errors="coerce",
            ).fillna(0).astype(int)
        )

        report["future_minibuses_bought"] = pd.to_numeric(
            report["future_minibuses_bought"],
            errors="coerce",
        ).fillna(0).astype(int)

        report["shortfall_value"] = pd.to_numeric(
            report["shortfall_value"],
            errors="coerce",
        ).fillna(0.0)

        report = report.sort_values(
            [
                "arrival_meets_target",
                "future_minibuses_bought",
                "shortfall_value",
                "objective_value",
            ],
            ascending=[False, True, True, True],
        ).reset_index(drop=True)

    return report, detailed_outputs


def get_recommended_fleet_scenario(
    fleet_report: pd.DataFrame,
) -> pd.DataFrame:
    """
    Return the recommended minimum scenario that meets target.

    If none meet the target, returns the best available row by highest SLA.
    """

    if fleet_report is None or len(fleet_report) == 0:
        return pd.DataFrame()

    meets = fleet_report[
        pd.to_numeric(
            fleet_report["arrival_meets_target"],
            errors="coerce",
        ).fillna(0).astype(int) == 1
    ].copy()

    if len(meets) > 0:
        return meets.head(1).reset_index(drop=True)

    out = fleet_report.copy()
    out["arrival_sla_pct"] = pd.to_numeric(
        out["arrival_sla_pct"],
        errors="coerce",
    ).fillna(-1)

    return (
        out.sort_values("arrival_sla_pct", ascending=False)
        .head(1)
        .reset_index(drop=True)
    )