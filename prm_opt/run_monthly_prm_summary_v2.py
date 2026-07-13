from __future__ import annotations

from datetime import date, timedelta
from calendar import monthrange

import pandas as pd

from prm_opt.run_s25_s26_v2 import run_s25_s26_v2


def run_monthly_prm_summary_v2(
    *,
    year: int,
    vehicle_models,
    config,
):

    today = date.today()

    rows = []

    for month in range(1, 13):

        month_start = date(year, month, 1)

        month_end = date(
            year,
            month,
            monthrange(year, month)[1]
        )

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

        # Current month (split)
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
            run_fleet_report=True,
            run_p100=True,
            run_p90=True,
            **kwargs,
        )

        for mode in ["p100", "p90"]:

            outputs = result[f"outputs_{mode}"]

            fleet_util = outputs["fleet_utilisation"]

            peak_amb = int(
                fleet_util.loc[
                    fleet_util["vehicle_type"] == "Amb",
                    "required_for_schedule"
                ].sum()
            )

            peak_mini = int(
                fleet_util.loc[
                    fleet_util["vehicle_type"] == "Mini",
                    "required_for_schedule"
                ].sum()
            )

            peak_staff = float(
                outputs["staff_summary"]["staff_required"].max()
            )

            sla = float(
                outputs["sla_summary"]
                .iloc[0]
                .get("arrival_sla_pct", 0)
            )

            rows.append(
                {
                    "month": month_start.strftime("%Y-%m"),
                    "demand_mode": mode.upper(),
                    "arrival_sla_pct": sla,
                    "peak_ambulifts": peak_amb,
                    "peak_minibuses": peak_mini,
                    "peak_staff": peak_staff,
                }
            )

    return pd.DataFrame(rows)