# scripts/prm_opt/run_s25_s26_v2.py

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd
import time


from modules.utils.progress import step
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

        if penetration_rates is None:
            raise ValueError("penetration_rates required when run_s26=True.")

        if ssr_mix is None:
            raise ValueError("ssr_mix required when run_s26=True.")

        if stand_actuals is None:
            raise ValueError("stand_actuals required when run_s26=True.")

        if stand_dist is None:
            raise ValueError("stand_dist required when run_s26=True.")

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
            vehicle_models=vehicle_models,
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
            fleet_report_p100, fleet_details_p100 = run_fleet_requirements_report_v2c(
                flights=flights_p100,
                vehicle_models=vehicle_models,
                base_config=config,
                max_ev10=max_ev10,
                max_ev18=max_ev18,
            )

            t = step(
                t,
                f"P100 fleet requirements report complete"
            )

            recommended_p100 = get_recommended_fleet_scenario(
                fleet_report_p100
            )

            result["fleet_requirements_p100"] = fleet_report_p100
            result["fleet_details_p100"] = fleet_details_p100
            result["recommended_p100"] = recommended_p100

            workbook_sheets["p100_fleet_requirements"] = fleet_report_p100
            workbook_sheets["p100_recommended"] = recommended_p100

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
            vehicle_models=vehicle_models,
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
            fleet_report_p90, fleet_details_p90 = run_fleet_requirements_report_v2c(
                flights=flights_p90,
                vehicle_models=vehicle_models,
                base_config=config,
                max_ev10=max_ev10,
                max_ev18=max_ev18,
            )

            t = step(
                t,
                f"P90 fleet requirements report complete"
            )

            recommended_p90 = get_recommended_fleet_scenario(
                fleet_report_p90
            )

            result["fleet_requirements_p90"] = fleet_report_p90
            result["fleet_details_p90"] = fleet_details_p90
            result["recommended_p90"] = recommended_p90

            workbook_sheets["p90_fleet_requirements"] = fleet_report_p90
            workbook_sheets["p90_recommended"] = recommended_p90

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