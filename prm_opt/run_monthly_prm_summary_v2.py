from __future__ import annotations

from datetime import date, timedelta
from calendar import monthrange

import pandas as pd

from prm_opt.run_s25_s26_v2 import run_s25_s26_v2


def _safe_max_staff(outputs) -> float:
    staff = outputs.get("staff_summary", pd.DataFrame())

    if staff is None or len(staff) == 0:
        return 0.0

    if "staff_required" not in staff.columns:
        return 0.0

    return float(
        pd.to_numeric(
            staff["staff_required"],
            errors="coerce",
        ).fillna(0.0).max()
    )


def _safe_sla(outputs) -> float:
    sla = outputs.get("sla_summary", pd.DataFrame())

    if sla is None or len(sla) == 0:
        return 0.0

    row = sla.iloc[0]

    if "arrival_sla_pct" in row:
        return float(row.get("arrival_sla_pct", 0.0))

    if "arrival_flight_sla_pct" in row:
        return float(row.get("arrival_flight_sla_pct", 0.0))

    return 0.0


def _safe_shortfall_total(outputs) -> float:
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


def _safe_peak_resource(outputs, vehicle_type: str) -> int:
    fleet_util = outputs.get("fleet_utilisation", pd.DataFrame())

    if fleet_util is None or len(fleet_util) == 0:
        return 0

    if "required_for_schedule" not in fleet_util.columns:
        return 0

    return int(
        pd.to_numeric(
            fleet_util.loc[
                fleet_util["vehicle_type"] == vehicle_type,
                "required_for_schedule",
            ],
            errors="coerce",
        ).fillna(0).sum()
    )


def _safe_gap_total(outputs) -> int:
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


def _recommended_extra_fleet(result, mode: str) -> int:
    key = f"recommended_{mode}"
    rec = result.get(key, pd.DataFrame())

    if rec is None or len(rec) == 0:
        return 0

    if "future_bought" in rec.columns:
        return int(
            pd.to_numeric(
                rec["future_bought"],
                errors="coerce",
            ).fillna(0).max()
        )

    return 0


def run_monthly_prm_summary_v2(
    *,
    year: int,
    vehicle_models,
    config,
):
    """
    Monthly PRM summary.

    For each month:
      1. Run current fleet P100/P90.
      2. If SLA passes and current fleet is sufficient, stop.
      3. If SLA fails or current fleet is insufficient, run incremental fleet expansion.
      4. Stop fleet expansion once a passing future-fleet scenario is found.
    """

    today = date.today()

    rows = []

    for month in range(1, 13):

        month_start = date(year, month, 1)

        month_end = date(
            year,
            month,
            monthrange(year, month)[1],
        )

        print("\n" + "=" * 80)
        print(f"Running {month_start.strftime('%Y-%m')}")
        print("=" * 80)

        run_s25 = False
        run_s26 = False

        kwargs = {}

        # Historical month
        if month_end < today:

            run_s25 = True

            kwargs["s25_start"] = str(month_start)
            kwargs["s25_end"] = str(month_end)

        # Future month
        elif month_start > today:

            run_s26 = True

            kwargs["s26_start"] = str(month_start)
            kwargs["s26_end"] = str(month_end)

        # Current month split
        else:

            run_s25 = True
            run_s26 = True

            yesterday = today - timedelta(days=1)

            kwargs["s25_start"] = str(month_start)
            kwargs["s25_end"] = str(yesterday)

            kwargs["s26_start"] = str(today)
            kwargs["s26_end"] = str(month_end)

        result = run_s25_s26_v2(
            run_s25=run_s25,
            run_s26=run_s26,
            vehicle_models=vehicle_models,
            config=config,

            # Important:
            # this now means "run fleet expansion only if needed"
            run_fleet_report=True,
            fleet_report_policy="if_needed",
            current_fleet_only_first=True,

            run_p100=True,
            run_p90=True,

            **kwargs,
        )

        for mode in ["p100", "p90"]:

            outputs = result[f"outputs_{mode}"]

            peak_amb = _safe_peak_resource(
                outputs,
                "Amb",
            )

            peak_mini = _safe_peak_resource(
                outputs,
                "Mini",
            )

            peak_staff = _safe_max_staff(outputs)

            sla = _safe_sla(outputs)

            shortfall_total = _safe_shortfall_total(outputs)

            gap_total = _safe_gap_total(outputs)

            current_fleet_sufficient = int(
                shortfall_total <= 1e-6
                and gap_total == 0
            )

            needs_fleet_expansion = int(
                sla < config.arrival_sla_target_pct * 100.0
                or current_fleet_sufficient == 0
            )

            extra_fleet_recommended = _recommended_extra_fleet(
                result,
                mode,
            )

            rows.append(
                {
                    "month": month_start.strftime("%Y-%m"),
                    "demand_mode": mode.upper(),

                    "arrival_sla_pct": sla,
                    "arrival_target_pct": config.arrival_sla_target_pct * 100.0,

                    "shortfall_total": shortfall_total,
                    "gap_vs_current_total": gap_total,

                    "current_fleet_sufficient": current_fleet_sufficient,
                    "needs_fleet_expansion": needs_fleet_expansion,
                    "extra_fleet_recommended": extra_fleet_recommended,

                    "peak_ambulifts": peak_amb,
                    "peak_minibuses": peak_mini,
                    "peak_staff": peak_staff,
                }
            )

    return pd.DataFrame(rows)