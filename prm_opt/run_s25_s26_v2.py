# scripts/prm_opt/run_s25_s26_v2.py

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from dataclasses import replace

import pandas as pd
import time
from datetime import date, timedelta

from modules.utils.progress import step
from prm_opt.build_assumptions_v2 import build_assumptions_v2
from prm_opt.ingest_s25_v2 import ingest_s25_v2
from prm_opt.ingest_s26_v2 import ingest_s26_v2
from prm_opt.build_flights_v2 import build_flights_v2
from prm_opt.optimise_prm_fleet_v2 import (
    OptimiserConfig,
    optimise_prm_fleet_v2,
)

from prm_opt.sla_reporting_v2b import add_passenger_sla_reporting
from prm_opt.fleet_requirements_report_v2c import (
    run_fleet_requirements_report_v2c,
    get_recommended_fleet_scenario,
)


"""
Run S25/S26 PRM fleet optimiser V2.

This file pulls together:

1. Historical S25 ingest
2. Future S26 ingest
3. Flight-level aggregation
4. P100/P90 demand modes
5. V2A optimiser
6. V2B passenger-SLA reporting
7. V2C fleet requirements report
8. Output export

Designed to be called from a notebook or script.
"""


def _write_outputs_to_excel(
    output_path: Path,
    outputs_by_sheet: Dict[str, pd.DataFrame],
) -> None:
    """
    Write multiple dataframes to one XLSX workbook.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in outputs_by_sheet.items():
            if df is None:
                continue

            safe_sheet = str(sheet_name)[:31]

            if isinstance(df, pd.DataFrame):
                df.to_excel(writer, sheet_name=safe_sheet, index=False)
            else:
                pd.DataFrame(df).to_excel(writer, sheet_name=safe_sheet, index=False)


def run_single_demand_mode(
    flights: pd.DataFrame,
    vehicle_models: Dict[str, Dict[str, Any]],
    config: OptimiserConfig,
) -> Dict[str, pd.DataFrame]:
    """
    Run optimiser and passenger-based SLA reporting for a single flight table.
    """

    outputs = optimise_prm_fleet_v2(
        flights=flights,
        vehicle_models=vehicle_models,
        config=config,
    )

    outputs = add_passenger_sla_reporting(
        outputs=outputs,
        config=config,
    )

    return outputs

# ---------------------------------------------------------------------
# Fleet-report trigger helpers
# ---------------------------------------------------------------------

def _current_vehicle_models_only(
    vehicle_models: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Return vehicle models excluding future purchase options.

    Used when we want to test whether the current physical fleet is sufficient
    before allowing the optimiser to buy future minibuses.
    """
    return {
        k: v
        for k, v in vehicle_models.items()
        if not bool(v.get("is_future", False))
    }


def _arrival_sla_pct(
    outputs: Dict[str, pd.DataFrame],
) -> float:
    """
    Extract passenger-based arrival SLA percentage from outputs.
    """
    sla_df = outputs.get("sla_summary", pd.DataFrame())

    if sla_df is None or len(sla_df) == 0:
        return 0.0

    row = sla_df.iloc[0]

    if "arrival_sla_pct" in row:
        return float(row.get("arrival_sla_pct", 0.0))

    if "arrival_flight_sla_pct" in row:
        return float(row.get("arrival_flight_sla_pct", 0.0))

    return 0.0


def _shortfall_total(
    outputs: Dict[str, pd.DataFrame],
) -> float:
    """
    Total optimiser shortfall value.
    """
    short = outputs.get("shortfalls", pd.DataFrame())

    if short is None or len(short) == 0:
        return 0.0

    if "shortfall_value" not in short.columns:
        return 0.0

    return float(
        pd.to_numeric(
            short["shortfall_value"],
            errors="coerce",
        ).fillna(0.0).sum()
    )


def _fleet_gap_total(
    outputs: Dict[str, pd.DataFrame],
) -> int:
    """
    Total current-fleet gap from fleet utilisation output.
    """
    fleet_util = outputs.get("fleet_utilisation", pd.DataFrame())

    if fleet_util is None or len(fleet_util) == 0:
        return 0

    if "gap_vs_current" not in fleet_util.columns:
        return 0

    return int(
        pd.to_numeric(
            fleet_util["gap_vs_current"],
            errors="coerce",
        ).fillna(0).sum()
    )


def _current_fleet_is_sufficient(
    outputs: Dict[str, pd.DataFrame],
) -> bool:
    """
    Current fleet is sufficient if:
      - there are no capacity shortfalls
      - fleet utilisation shows no current-fleet gap
    """
    return (
        _shortfall_total(outputs) <= 1e-6
        and _fleet_gap_total(outputs) == 0
    )


def _needs_fleet_expansion(
    outputs: Dict[str, pd.DataFrame],
    config: OptimiserConfig,
) -> bool:
    """
    Decide whether to run future fleet expansion.

    We run future fleet only if:
      - arrival SLA is below target, OR
      - current fleet is not sufficient.
    """
    target_pct = float(config.arrival_sla_target_pct) * 100.0
    sla_pct = _arrival_sla_pct(outputs)

    current_ok = _current_fleet_is_sufficient(outputs)

    return (
        sla_pct < target_pct
        or not current_ok
    )


def _run_incremental_fleet_expansion(
    *,
    flights: pd.DataFrame,
    vehicle_models: Dict[str, Dict[str, Any]],
    base_config: OptimiserConfig,
    max_future_copies: int,
    demand_mode_label: str,
) -> tuple[pd.DataFrame, Dict[str, Dict[str, pd.DataFrame]], pd.DataFrame]:
    """
    Incremental future fleet search.

    Logic:
      1. Try +1 future copy per future model.
      2. Check SLA and current/future sufficiency.
      3. Stop as soon as the scenario passes.
      4. If no scenario passes, return all attempted scenarios.

    This avoids brute-forcing every fleet scenario once a passing scenario is found.
    """

    report_rows = []
    detailed_outputs = {}

    for n in range(1, int(max_future_copies) + 1):

        scenario_name = f"{demand_mode_label}_future_plus_{n}"

        scenario_config = replace(
            base_config,
            max_future_copies_per_model=n,
        )

        print(
            f"\n[{demand_mode_label.upper()}] Running fleet expansion scenario: "
            f"+{n} future copy per future model"
        )

        outputs = run_single_demand_mode(
            flights=flights,
            vehicle_models=vehicle_models,
            config=scenario_config,
        )

        detailed_outputs[scenario_name] = outputs

        sla_pct = _arrival_sla_pct(outputs)
        shortfall_total = _shortfall_total(outputs)
        gap_total = _fleet_gap_total(outputs)
        current_ok = _current_fleet_is_sufficient(outputs)

        fleet = outputs.get("fleet_summary", pd.DataFrame())

        if fleet is not None and len(fleet) > 0 and "bought" in fleet.columns:
            future_bought = int(
                fleet.loc[
                    fleet["is_future"].astype(int) == 1,
                    "bought",
                ].sum()
            )
        else:
            future_bought = 0

        target_pct = float(scenario_config.arrival_sla_target_pct) * 100.0

        meets_sla = int(sla_pct >= target_pct)
        scenario_passes = (
            meets_sla == 1
            and shortfall_total <= 1e-6
        )

        report_rows.append(
            {
                "scenario": scenario_name,
                "future_copies_per_model": n,
                "arrival_sla_pct": sla_pct,
                "arrival_target_pct": target_pct,
                "meets_sla": meets_sla,
                "shortfall_total": shortfall_total,
                "gap_vs_current_total": gap_total,
                "current_fleet_sufficient": int(current_ok),
                "future_bought": future_bought,
                "scenario_passes": int(scenario_passes),
            }
        )

        if scenario_passes:
            print(
                f"[{demand_mode_label.upper()}] Stopping fleet expansion: "
                f"scenario +{n} meets SLA and has no shortfall."
            )
            break

    fleet_report = pd.DataFrame(report_rows)

    if len(fleet_report) > 0:
        passed = fleet_report[
            fleet_report["scenario_passes"].astype(int) == 1
        ].copy()

        if len(passed) > 0:
            recommended = passed.head(1).reset_index(drop=True)
        else:
            recommended = (
                fleet_report
                .sort_values(
                    [
                        "arrival_sla_pct",
                        "shortfall_total",
                        "future_bought",
                    ],
                    ascending=[False, True, True],
                )
                .head(1)
                .reset_index(drop=True)
            )
    else:
        recommended = pd.DataFrame()

    return fleet_report, detailed_outputs, recommended


def run_s25_s26_v2(
    *,
    s25_start: str | None = None,
    s25_end: str | None = None,
    s26_start: str | None = None,
    s26_end: str | None = None,
    run_s25: bool = True,
    run_s26: bool = True,
    penetration_rates: pd.DataFrame | None = None,
    ssr_mix: pd.DataFrame | None = None,
    stand_actuals: pd.DataFrame | None = None,
    stand_dist: pd.DataFrame | None = None,
    vehicle_models: Dict[str, Dict[str, Any]] | None = None,
    penetration_uplift: float = 1.0,
    seed: int = 42,
    output_dir: str = "outputs/prm_v2",
    output_xlsx_name: str = "prm_v2_outputs.xlsx",
    run_p100: bool = True,
    run_p90: bool = True,
   run_fleet_report: bool = True,
    fleet_report_policy: str = "if_needed",
    current_fleet_only_first: bool = True,
    max_ev10: int = 5,
    max_ev18: int = 5,
    config: OptimiserConfig | None = None,
) -> Dict[str, Any]:
    """
    Main orchestration function.

    Returns a dictionary containing:
        passengers
        flights_p100
        flights_p90
        outputs_p100
        outputs_p90
        fleet_requirements_p100
        fleet_requirements_p90
        recommended_p100
        recommended_p90
        output_workbook_path
    """

    t = time.perf_counter()

    if config is None:
        config = OptimiserConfig()

    if vehicle_models is None:
        raise ValueError("vehicle_models must be provided.")

    if current_fleet_only_first:
        base_vehicle_models = _current_vehicle_models_only(vehicle_models)
    else:
        base_vehicle_models = vehicle_models

    passenger_frames = []

    # --------------------------------------------------
    # S25 historical ingest
    # --------------------------------------------------
    if run_s25:
        if not s25_start or not s25_end:
            raise ValueError("s25_start and s25_end required when run_s25=True.")

        passengers_s25 = ingest_s25_v2(
            start=s25_start,
            end=s25_end,
            seed=seed,
        )

        t = step(
            t,
            f"S25 ingest complete ({len(passengers_s25):,} passengers)"
        )

        passenger_frames.append(passengers_s25)

    # --------------------------------------------------
    # S26 future ingest
    # --------------------------------------------------
    if run_s26:
        if not s26_start or not s26_end:
            raise ValueError("s26_start and s26_end required when run_s26=True.")
        
        if (
            penetration_rates is None
            or ssr_mix is None
            or stand_actuals is None
            or stand_dist is None
        ):

            assumption_end = str(
                date.today() - timedelta(days=1)
            )

            print(
                "\n[V2] Building assumptions using "
                f"historical data through {assumption_end}"
            )

            assumptions = build_assumptions_v2(
                s25_start=s25_start,
                s25_end=assumption_end,
            )

            penetration_rates = assumptions["penetration_rates"]

            ssr_mix = assumptions["ssr_mix"]

            stand_actuals = assumptions["stand_actuals"]

            stand_dist = assumptions["stand_dist"]

            tau_mode_params = assumptions["tau_mode_params"]

            print(
                "\n[V2] Assumptions built automatically."
            )

        passengers_s26 = ingest_s26_v2(
            start=s26_start,
            end=s26_end,
            penetration_rates=penetration_rates,
            ssr_mix=ssr_mix,
            stand_actuals=stand_actuals,
            stand_dist=stand_dist,
            penetration_uplift=penetration_uplift,
            seed=seed,
        )

        passenger_frames.append(passengers_s26)

    if len(passenger_frames) == 0:
        raise ValueError("No passenger data was generated. Set run_s25 and/or run_s26.")

    passengers = pd.concat(passenger_frames, ignore_index=True)

    t = step(
            t,
            f"Passenger dataset combined ({len(passengers):,} passenger rows)"
        )

    result = {
        "passengers": passengers,
    }

    workbook_sheets = {
        "passengers_sample": passengers.head(5000),
    }

    # --------------------------------------------------
    # P100
    # --------------------------------------------------
    if run_p100:
        flights_p100 = build_flights_v2(
            passengers,
            demand_mode="p100",
            boarding_offset_mins=config.boarding_offset_mins,
        )
        
        t = step(
            t,
            f"P100 built ({len(flights_p100):,} flights)"
        )

        print(
            f"    P100 demand summary: "
            f"{flights_p100['P_total'].sum():,} PRMs across "
            f"{len(flights_p100):,} flights"
        )

        outputs_p100 = run_single_demand_mode(
            flights=flights_p100,
            vehicle_models=base_vehicle_models,
            config=config,
        )



        t = step(
            t,
            f"P100 optimisation complete"
        )

        if "fleet_utilisation" in outputs_p100:
            print("\n[P100 fleet utilisation]")
            print(outputs_p100["fleet_utilisation"])

        result["flights_p100"] = flights_p100
        result["outputs_p100"] = outputs_p100

        workbook_sheets.update(
            {
                "p100_flights": flights_p100,
                "p100_flight_assignments": outputs_p100["flight_assignments"],
                "p100_vehicle_schedule": outputs_p100["vehicle_schedule"],
                "p100_fleet_summary": outputs_p100["fleet_summary"],
                "p100_fleet_requirements": outputs_p100["fleet_requirements"],
                "p100_fleet_utilisation": outputs_p100["fleet_utilisation"],
                "p100_staff_summary": outputs_p100["staff_summary"],
                "p100_staff_jobs": outputs_p100["staff_jobs"],
                "p100_shortfalls": outputs_p100["shortfalls"],
                "p100_sla_summary": outputs_p100["sla_summary"],
                "p100_arrival_breaches": outputs_p100.get(
                    "arrival_breach_details",
                    pd.DataFrame(),
                ),
                "p100_solver_results": outputs_p100["solver_results"],
            }
        )

        if run_fleet_report:

            needs_expansion_p100 = _needs_fleet_expansion(
                outputs=outputs_p100,
                config=config,
            )

            p100_base_check = pd.DataFrame(
                [
                    {
                        "demand_mode": "P100",
                        "arrival_sla_pct": _arrival_sla_pct(outputs_p100),
                        "arrival_target_pct": config.arrival_sla_target_pct * 100.0,
                        "shortfall_total": _shortfall_total(outputs_p100),
                        "gap_vs_current_total": _fleet_gap_total(outputs_p100),
                        "current_fleet_sufficient": int(
                            _current_fleet_is_sufficient(outputs_p100)
                        ),
                        "needs_fleet_expansion": int(needs_expansion_p100),
                    }
                ]
            )

            result["p100_base_check"] = p100_base_check
            workbook_sheets["p100_base_check"] = p100_base_check

            if (
                fleet_report_policy == "always"
                or (
                    fleet_report_policy == "if_needed"
                    and needs_expansion_p100
                )
            ):
                fleet_report_p100, fleet_details_p100, recommended_p100 = (
                    _run_incremental_fleet_expansion(
                        flights=flights_p100,
                        vehicle_models=vehicle_models,
                        base_config=config,
                        max_future_copies=max(max_ev10, max_ev18),
                        demand_mode_label="p100",
                    )
                )

                t = step(
                    t,
                    "P100 fleet expansion search complete"
                )

                result["fleet_requirements_p100"] = fleet_report_p100
                result["fleet_details_p100"] = fleet_details_p100
                result["recommended_p100"] = recommended_p100

                workbook_sheets["p100_fleet_requirements"] = fleet_report_p100
                workbook_sheets["p100_recommended"] = recommended_p100

            else:
                print(
                    "\n[P100] Current fleet meets SLA and has no current-fleet gap. "
                    "Skipping fleet expansion search."
                )

                empty_report = pd.DataFrame(
                    columns=[
                        "scenario",
                        "future_copies_per_model",
                        "arrival_sla_pct",
                        "arrival_target_pct",
                        "meets_sla",
                        "shortfall_total",
                        "gap_vs_current_total",
                        "current_fleet_sufficient",
                        "future_bought",
                        "scenario_passes",
                    ]
                )

                result["fleet_requirements_p100"] = empty_report
                result["fleet_details_p100"] = {}
                result["recommended_p100"] = p100_base_check

                workbook_sheets["p100_fleet_requirements"] = empty_report
                workbook_sheets["p100_recommended"] = p100_base_check

    # --------------------------------------------------
    # P90
    # --------------------------------------------------
    if run_p90:
        flights_p90 = build_flights_v2(
            passengers,
            demand_mode="p90",
            p90_quantile=0.90,
            boarding_offset_mins=config.boarding_offset_mins,
        )

        t = step(
            t,
            f"P90 built ({len(flights_p90):,} flights)"
        )

        print(
            f"    P90 demand summary: "
            f"{flights_p90['P_total'].sum():,} PRMs across "
            f"{len(flights_p90):,} flights"
        )


        outputs_p90 = run_single_demand_mode(
            flights=flights_p90,
            vehicle_models=base_vehicle_models,
            config=config,
        )

        t = step(
            t,
            f"P90 optimisation complete"
        )

        if "fleet_utilisation" in outputs_p90:
            print("\n[P90 fleet utilisation]")
            print(outputs_p90["fleet_utilisation"])

        result["flights_p90"] = flights_p90
        result["outputs_p90"] = outputs_p90

        workbook_sheets.update(
            {
                "p90_flights": flights_p90,
                "p90_flight_assignments": outputs_p90["flight_assignments"],
                "p90_vehicle_schedule": outputs_p90["vehicle_schedule"],
                "p90_fleet_summary": outputs_p90["fleet_summary"],
                "p90_fleet_requirements": outputs_p90["fleet_requirements"],
                "p90_fleet_utilisation": outputs_p90["fleet_utilisation"],
                "p90_staff_summary": outputs_p90["staff_summary"],
                "p90_staff_jobs": outputs_p90["staff_jobs"],
                "p90_shortfalls": outputs_p90["shortfalls"],
                "p90_sla_summary": outputs_p90["sla_summary"],
                "p90_arrival_breaches": outputs_p90.get(
                    "arrival_breach_details",
                    pd.DataFrame(),
                ),
                "p90_solver_results": outputs_p90["solver_results"],
            }
        )

        if run_fleet_report:

            needs_expansion_p90 = _needs_fleet_expansion(
                outputs=outputs_p90,
                config=config,
            )

            p90_base_check = pd.DataFrame(
                [
                    {
                        "demand_mode": "P90",
                        "arrival_sla_pct": _arrival_sla_pct(outputs_p90),
                        "arrival_target_pct": config.arrival_sla_target_pct * 100.0,
                        "shortfall_total": _shortfall_total(outputs_p90),
                        "gap_vs_current_total": _fleet_gap_total(outputs_p90),
                        "current_fleet_sufficient": int(
                            _current_fleet_is_sufficient(outputs_p90)
                        ),
                        "needs_fleet_expansion": int(needs_expansion_p90),
                    }
                ]
            )

            result["p90_base_check"] = p90_base_check
            workbook_sheets["p90_base_check"] = p90_base_check

            if (
                fleet_report_policy == "always"
                or (
                    fleet_report_policy == "if_needed"
                    and needs_expansion_p90
                )
            ):
                fleet_report_p90, fleet_details_p90, recommended_p90 = (
                    _run_incremental_fleet_expansion(
                        flights=flights_p90,
                        vehicle_models=vehicle_models,
                        base_config=config,
                        max_future_copies=max(max_ev10, max_ev18),
                        demand_mode_label="p90",
                    )
                )

                t = step(
                    t,
                    "P90 fleet expansion search complete"
                )

                result["fleet_requirements_p90"] = fleet_report_p90
                result["fleet_details_p90"] = fleet_details_p90
                result["recommended_p90"] = recommended_p90

                workbook_sheets["p90_fleet_requirements"] = fleet_report_p90
                workbook_sheets["p90_recommended"] = recommended_p90

            else:
                print(
                    "\n[P90] Current fleet meets SLA and has no current-fleet gap. "
                    "Skipping fleet expansion search."
                )

                empty_report = pd.DataFrame(
                    columns=[
                        "scenario",
                        "future_copies_per_model",
                        "arrival_sla_pct",
                        "arrival_target_pct",
                        "meets_sla",
                        "shortfall_total",
                        "gap_vs_current_total",
                        "current_fleet_sufficient",
                        "future_bought",
                        "scenario_passes",
                    ]
                )

                result["fleet_requirements_p90"] = empty_report
                result["fleet_details_p90"] = {}
                result["recommended_p90"] = p90_base_check

                workbook_sheets["p90_fleet_requirements"] = empty_report
                workbook_sheets["p90_recommended"] = p90_base_check

    # --------------------------------------------------
    # Export workbook
    # --------------------------------------------------
    output_dir_path = Path(output_dir)
    output_workbook_path = output_dir_path / output_xlsx_name

    _write_outputs_to_excel(
        output_path=output_workbook_path,
        outputs_by_sheet=workbook_sheets,
    )

    t = step(
            t,
            f"Excel outputs written to {output_workbook_path}"
        )

    result["output_workbook_path"] = str(output_workbook_path)

    return result