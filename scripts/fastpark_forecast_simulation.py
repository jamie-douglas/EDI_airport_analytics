"""
FASTPARK FORECAST SIMULATION
============================

Leakage-safe historical backtest of daily FastPark entries and exits at
multiple forecast horizons. The script tests evidence-based entry and exit
scenarios, combines the selected forecasts into movements, compares hourly
allocation methods, and exports a concise Excel summary.

Important interpretation
------------------------
1. A booking is active at cutoff C only when createdAt <= C and it had not been
   cancelled by C.
2. Planned entry/exit information is used to forecast. Actual event dates are
   used to score the operational forecast.
3. Every fitted parameter uses records strictly before the forecast cutoff.
4. Historical actual passenger totals are an oracle benchmark until genuine
   point-in-time passenger forecast snapshots are available.
5. Reconstructed occupancy and achieved-price correlations are excluded from
   primary scenarios because the V6 analysis does not establish them as clean,
   usable point-in-time causal drivers.
"""

from __future__ import annotations

import importlib
import math
import pathlib
import sys
import time
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from modules.utils.db import get_engine
from modules.utils.progress import step

ANALYSIS_MODULE = "fastpark_forecast_analysis"

try:
    hist = importlib.import_module(ANALYSIS_MODULE)
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        f"Could not import {ANALYSIS_MODULE!r}. Set ANALYSIS_MODULE to the "
        "existing historical analysis filename without '.py'."
    ) from exc


# ============================================================
# 0. CONFIGURATION
# ============================================================

def get_simulation_config() -> dict:
    return {
        "analysis_start_date": "2024-01-01",
        "analysis_end_date": "2026-04-09",
        "test_start_date": "2025-07-13",
        "test_end_date": "2026-03-28",
        "test_day_min": 13,
        "test_day_max": 28,
        "forecast_horizons": [56, 42, 35, 28, 21, 14, 10, 7, 6, 5, 4, 3, 2, 1, 0],
        "same_weekday_windows": [4, 8, 12],
        "duration_bins_days": [0, 1, 3, 7, 10, 14, 21, 999],
        "duration_labels": [
            "0-1 days", "2-3 days", "4-7 days", "8-10 days",
            "11-14 days", "15-21 days", "22+ days",
        ],
        "excluded_duration_bands": ["0-1 days"],
        "minimum_segment_dates": 3,
        "hybrid_weight_grid": [x / 20 for x in range(21)],
        "hourly_blend_grid": [x / 10 for x in range(11)],
        "entry_departure_offset_hours": 2,
        "exit_arrival_offset_hours": 1,
        "return_offset_days": 3,
        "underforecast_cost": 2.0,
        "overforecast_cost": 1.0,
        "output_path": r"output\fastpark\reports\fastpark_forecast_simulation.xlsx",
        "passenger_input_mode": "actual_oracle",
    }


# ============================================================
# 1. UTILITIES
# ============================================================

def safe_divide(numerator, denominator):
    if denominator is None or pd.isna(denominator) or denominator == 0:
        return np.nan
    return numerator / denominator


def weighted_mean(values: pd.Series) -> float:
    values = values.dropna().astype(float)
    if values.empty:
        return np.nan
    weights = np.arange(1, len(values) + 1, dtype=float)
    return float(np.average(values.to_numpy(), weights=weights))


def active_at_cutoff_mask(bookings: pd.DataFrame, cutoff: pd.Timestamp) -> pd.Series:
    """Return bookings that genuinely existed and were active at the cutoff."""
    return (
        bookings["createdAt"].le(cutoff)
        & (bookings["cancelledAt"].isna() | bookings["cancelledAt"].gt(cutoff))
    )


def select_test_dates(config: dict, available_dates: Iterable) -> list[pd.Timestamp]:
    start = pd.Timestamp(config["test_start_date"])
    end = pd.Timestamp(config["test_end_date"])
    dates = pd.DatetimeIndex(pd.to_datetime(list(available_dates))).normalize()
    return sorted({
        d for d in dates
        if start <= d <= end
        and config["test_day_min"] <= d.day <= config["test_day_max"]
    })


# ============================================================
# 2. LOAD AND PREPARE DATA
# ============================================================

def load_simulation_data(sql_connection, config: dict) -> dict:
    hist_config = hist.get_analysis_config()
    hist_config["duration_bins_days"] = config["duration_bins_days"]
    hist_config["duration_labels"] = config["duration_labels"]

    bookings_raw = hist.get_fastpark_bookings(
        start=config["analysis_start_date"], end=config["analysis_end_date"],
        statuses=["B", "CX", "F"], asset_name="FastPark", engine=sql_connection,
    )
    operations_raw = hist.get_fastpark_entry_exits(
        start=config["analysis_start_date"], end=config["analysis_end_date"],
        engine=sql_connection,
    )
    flights_raw = hist.get_historical_flight_performance(
        start=config["analysis_start_date"], end=config["analysis_end_date"],
        engine=sql_connection,
    )

    bookings = hist.clean_bookings(bookings_raw, hist_config)
    operations = hist.clean_operations(operations_raw, hist_config)
    flights = hist.clean_flights(flights_raw, hist_config)
    master = hist.reconcile_bookings_to_operations(bookings, operations)

    for df in (bookings, master):
        for col in [
            "createdAt", "cancelledAt", "entryDate", "exitDate",
            "actual_entry_ts", "actual_exit_ts", "ExpectedReturnDate",
        ]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        df["planned_entry_day"] = df["entryDate"].dt.normalize()
        df["planned_exit_day"] = df["exitDate"].dt.normalize()
        df["duration_band"] = df["planned_duration_band"].astype(str)

    master["actual_entry_day"] = master["actual_entry_ts"].dt.normalize()
    master["actual_exit_day"] = master["actual_exit_ts"].dt.normalize()

    daily_actuals = hist.create_daily_fastpark_actuals(master)
    daily_actuals["date"] = pd.to_datetime(daily_actuals["date"]).dt.normalize()
    daily_pax = hist.create_daily_passenger_summary(flights, hist_config)
    daily_pax["date"] = pd.to_datetime(daily_pax["date"]).dt.normalize()
    hourly_actuals = hist.create_hourly_fastpark_actuals(master)
    hourly_actuals["datetime_hour"] = pd.to_datetime(hourly_actuals["datetime_hour"])
    hourly_actuals["date"] = pd.to_datetime(hourly_actuals["date"]).dt.normalize()
    hourly_pax = hist.create_hourly_passenger_summary(flights, hist_config)
    hourly_pax["date"] = pd.to_datetime(hourly_pax["date"]).dt.normalize()

    return {
        "bookings": bookings, "master": master, "flights": flights,
        "daily_actuals": daily_actuals, "daily_pax": daily_pax,
        "hourly_actuals": hourly_actuals, "hourly_pax": hourly_pax,
    }


def build_daily_modelling_table(data: dict) -> pd.DataFrame:
    actuals = data["daily_actuals"]
    pax = data["daily_pax"]
    calendar = pd.DataFrame({
        "date": pd.date_range(
            min(actuals["date"].min(), pax["date"].min()),
            max(actuals["date"].max(), pax["date"].max()),
            freq="D",
        )
    })
    daily = calendar.merge(actuals, on="date", how="left").merge(pax, on="date", how="left")
    daily["weekday"] = daily["date"].dt.day_name()
    daily["month"] = daily["date"].dt.month
    return daily


# ============================================================
# 3. LEAKAGE-SAFE AS-OF CURVES
# ============================================================

def build_asof_curve_tables(data: dict, config: dict) -> dict:
    """Build overall and duration-specific as-of curves.

    Why included
    ------------
    V6 shows that visible booking demand changes materially with forecast
    horizon and planned duration. These tables count bookings that were active
    at each historical cutoff and score them against actual operational event
    dates, without using final booking status from the future.
    """
    bookings = data["bookings"]
    master = data["master"]
    actuals = data["daily_actuals"]

    actual_entry = actuals.set_index("date")["entries"].to_dict()
    actual_exit = actuals.set_index("date")["exits"].to_dict()
    actual_entry_duration = (
        master.dropna(subset=["actual_entry_day"])
        .groupby(["actual_entry_day", "duration_band"])["bookingId"].nunique().to_dict()
    )
    actual_exit_duration = (
        master.dropna(subset=["actual_exit_day"])
        .groupby(["actual_exit_day", "duration_band"])["bookingId"].nunique().to_dict()
    )

    rows = {"entry": [], "exit": [], "entry_duration": [], "exit_duration": []}
    specifications = [
        ("entry", "planned_entry_day", actual_entry, actual_entry_duration),
        ("exit", "planned_exit_day", actual_exit, actual_exit_duration),
    ]

    for horizon in config["forecast_horizons"]:
        for target, day_col, actual_lookup, duration_lookup in specifications:
            cutoff_by_booking = bookings[day_col] - pd.Timedelta(days=horizon)
            active = (
                bookings["createdAt"].le(cutoff_by_booking)
                & (bookings["cancelledAt"].isna() | bookings["cancelledAt"].gt(cutoff_by_booking))
            )
            subset = bookings.loc[active]

            for day, known in subset.groupby(day_col)["bookingId"].nunique().items():
                rows[target].append({
                    "target_date": day, "horizon": horizon,
                    "known_bookings": known, "actual": actual_lookup.get(day, 0),
                })

            grouped = subset.groupby([day_col, "duration_band"])["bookingId"].nunique()
            for (day, band), known in grouped.items():
                rows[f"{target}_duration"].append({
                    "target_date": day, "horizon": horizon,
                    "duration_band": band, "known_bookings": known,
                    "actual": duration_lookup.get((day, band), 0),
                })

    curves = {}
    for name, records in rows.items():
        frame = pd.DataFrame(records)
        frame["target_date"] = pd.to_datetime(frame["target_date"]).dt.normalize()
        frame["weekday"] = frame["target_date"].dt.day_name()
        curves[name] = frame
    return curves


# ============================================================
# 4. TRAINING ESTIMATORS
# ============================================================

def previous_same_weekdays(daily, target_date, cutoff, value_col, n):
    return (
        daily[daily["date"].lt(cutoff) & daily["weekday"].eq(target_date.day_name())]
        .sort_values("date").tail(n)[value_col]
    )


def fit_penetration(daily, target_date, cutoff, target_col, pax_col, n, weighted):
    history = (
        daily[daily["date"].lt(cutoff) & daily["weekday"].eq(target_date.day_name())]
        .sort_values("date").tail(n).copy()
    )
    if len(history) < max(2, n // 2):
        return np.nan
    if not weighted:
        return safe_divide(history[target_col].sum(), history[pax_col].sum())
    penetration = history[target_col] / history[pax_col].replace(0, np.nan)
    return weighted_mean(penetration)


def fit_curve_multiplier(curve, target_date, cutoff, horizon, duration_band=None):
    history = curve[
        curve["horizon"].eq(horizon)
        & curve["target_date"].lt(cutoff)
        & curve["weekday"].eq(target_date.day_name())
        & curve["known_bookings"].gt(0)
    ]
    if duration_band is not None:
        history = history[history["duration_band"].eq(duration_band)]
    history = history.sort_values("target_date").tail(8)
    if len(history) < 3:
        return np.nan
    return safe_divide(history["actual"].sum(), history["known_bookings"].sum())


def get_known_value(curve, target_date, horizon):
    row = curve[curve["target_date"].eq(target_date) & curve["horizon"].eq(horizon)]
    return float(row["known_bookings"].iloc[0]) if len(row) else 0.0


def forecast_duration_curve(curve, target_date, cutoff, horizon, fallback, excluded):
    """Forecast using a separate as-of multiplier for each duration band.

    Why included
    ------------
    V6 shows that short stays are booked later than long stays. This difference
    is particularly important for exits because short bookings can create exit
    demand that was invisible at longer horizons. A single multiplier can hide
    this behaviour, so the scenario estimates each duration band separately.
    """
    current = curve[
        curve["target_date"].eq(target_date)
        & curve["horizon"].eq(horizon)
        & ~curve["duration_band"].isin(excluded)
    ]
    forecast = 0.0
    known_total = 0.0
    for _, row in current.iterrows():
        multiplier = fit_curve_multiplier(
            curve, target_date, cutoff, horizon, row["duration_band"]
        )
        if pd.isna(multiplier):
            multiplier = fallback
        if pd.notna(multiplier):
            forecast += row["known_bookings"] * multiplier
            known_total += row["known_bookings"]
    return forecast, known_total


def known_expected_returns_at_cutoff(master, target_date, cutoff):
    active = master.loc[active_at_cutoff_mask(master, cutoff)].copy()
    checked_in = active["actual_entry_ts"].notna() & active["actual_entry_ts"].le(cutoff)
    active["known_return_ts"] = active["exitDate"]
    active.loc[checked_in, "known_return_ts"] = active.loc[
        checked_in, "ExpectedReturnDate"
    ].fillna(active.loc[checked_in, "exitDate"])
    return int(active.loc[
        active["known_return_ts"].dt.normalize().eq(target_date), "bookingId"
    ].nunique())


def fit_return_offset_distribution(master, cutoff, duration_band, max_days):
    """Learn duration-specific early/late return-day probabilities.

    Why included
    ------------
    V6 contains ExpectedReturnDate and actual checkout timestamps and shows that
    customers do not always exit on the advised date. This scenario redistributes
    known returns across nearby dates using only deviations observed before the
    forecast cutoff for the same planned-duration band.
    """
    history = master[
        master["actual_exit_ts"].notna()
        & master["actual_exit_ts"].lt(cutoff)
        & master["ExpectedReturnDate"].notna()
        & master["duration_band"].eq(duration_band)
    ].copy()
    if len(history) < 30:
        history = master[
            master["actual_exit_ts"].notna()
            & master["actual_exit_ts"].lt(cutoff)
            & master["ExpectedReturnDate"].notna()
        ].copy()
    if history.empty:
        return pd.Series({0: 1.0})
    offsets = (
        history["actual_exit_ts"].dt.normalize()
        - history["ExpectedReturnDate"].dt.normalize()
    ).dt.days.clip(-max_days, max_days)
    return offsets.value_counts(normalize=True)


def forecast_early_late_adjusted_exits(master, target_date, cutoff, config):
    active = master.loc[active_at_cutoff_mask(master, cutoff)].copy()
    checked_in = active["actual_entry_ts"].notna() & active["actual_entry_ts"].le(cutoff)
    active["known_return_ts"] = active["exitDate"]
    active.loc[checked_in, "known_return_ts"] = active.loc[
        checked_in, "ExpectedReturnDate"
    ].fillna(active.loc[checked_in, "exitDate"])
    active = active.dropna(subset=["known_return_ts", "duration_band"])

    forecast = 0.0
    contributing = 0
    for band, group in active.groupby("duration_band"):
        probabilities = fit_return_offset_distribution(
            master, cutoff, band, config["return_offset_days"]
        )
        required_offset = target_date - group["known_return_ts"].dt.normalize()
        required_offset = required_offset.dt.days
        forecast += required_offset.map(probabilities).fillna(0).sum()
        contributing += int(required_offset.abs().le(config["return_offset_days"]).sum())
    return float(forecast), contributing


def learn_best_blend(predictions, target, horizon, cutoff, method_a, method_b, grid):
    history = predictions[
        predictions["target"].eq(target)
        & predictions["horizon"].eq(horizon)
        & predictions["target_date"].lt(cutoff)
        & predictions["method"].isin([method_a, method_b])
    ]
    wide = history.pivot_table(
        index=["target_date", "actual"], columns="method", values="forecast",
        aggfunc="first",
    ).reset_index()
    if method_a not in wide.columns or method_b not in wide.columns or len(wide) < 8:
        return 0.5
    return min(
        grid,
        key=lambda weight: (
            weight * wide[method_a] + (1 - weight) * wide[method_b] - wide["actual"]
        ).abs().mean(),
    )


# ============================================================
# 5. DAILY FORECAST SCENARIOS
# ============================================================

def create_scenario_catalogue() -> pd.DataFrame:
    """Document every scenario and its V6 evidence in the Excel output."""
    rows = [
        ("Entry", "E0_same_weekday_4/8/12", "Recent same-weekday actual baseline", "V6 shows material weekday differences in entry volumes. Every complex method must beat a simple operational benchmark."),
        ("Entry", "E1_pax_ratio_4/8/12", "Departing passenger penetration", "V6 identifies departing passenger demand as an external entry driver."),
        ("Entry", "E1_pax_weighted_4/8/12", "Recency-weighted departing penetration", "Tests whether recent same-weekday penetration better represents current demand."),
        ("Entry", "E2_booking_curve", "Overall as-of entry booking curve", "Known planned entries are the closest direct signal of future FastPark entry demand."),
        ("Entry", "E3_duration_specific_curve", "Duration-specific entry booking curve", "V6 booking curves show that visibility differs by planned stay length."),
        ("Entry", "E4_hybrid_booking_pax", "Booking plus passenger hybrid", "Combines direct booking visibility with broader airport demand; the weight is learned by horizon."),
        ("Exit", "X0_same_weekday_4/8/12", "Recent same-weekday actual baseline", "V6 shows that exit volumes have a distinct weekday pattern."),
        ("Exit", "X1_pax_ratio_4/8/12", "Arriving passenger penetration", "V6 identifies arriving passengers as an external exit benchmark."),
        ("Exit", "X1_pax_weighted_4/8/12", "Recency-weighted arrival penetration", "Tests whether recent same-weekday exit penetration better represents current behaviour."),
        ("Exit", "X2_known_expected_returns", "Latest return date known at cutoff", "V6 includes ExpectedReturnDate, which can improve on the original exitDate after check-in."),
        ("Exit", "X3_duration_specific_curve", "Duration-specific exit booking curve", "Short stays create later-visible exits while long stays are visible earlier."),
        ("Exit", "X4_planned_exit_curve", "Overall planned-exit booking curve", "Provides the direct overall comparator to the duration-specific exit model."),
        ("Exit", "X5_early_late_adjusted", "Early/late adjusted expected returns", "V6 return-deviation outputs show that expected and actual return dates can differ, including by duration."),
        ("Exit", "X6_hybrid_return_pax", "Adjusted returns plus passenger hybrid", "Combines customer-level return visibility with external arrival demand."),
        ("Hourly", "H1_weekday_profile", "Historical weekday hourly profile", "V6 hourly profiles show different intraday entry and exit shapes by weekday."),
        ("Hourly", "H2_flight_offset", "Passenger flight-offset profile", "V6 finds strongest entry alignment with departures two hours later and exit alignment with arrivals one hour earlier."),
        ("Hourly", "H3_blend", "Blended weekday and flight profile", "Tests whether stable historical shape and target-day flight timing work better together."),
    ]
    return pd.DataFrame(rows, columns=["target", "scenario", "description", "why_included"])


def add_prediction(rows, target_date, cutoff, horizon, target, method, forecast,
                   actual, known=np.nan, passenger_base=np.nan, note=""):
    if pd.notna(forecast):
        rows.append({
            "target_date": target_date, "forecast_cutoff": cutoff,
            "horizon": horizon, "target": target, "method": method,
            "forecast": max(0.0, float(forecast)), "actual": float(actual),
            "known_bookings": known, "passenger_base": passenger_base,
            "note": note,
        })


def run_daily_simulation(data: dict, curves: dict, config: dict) -> pd.DataFrame:
    daily = build_daily_modelling_table(data)
    available = daily.loc[daily[["entries", "exits"]].notna().all(axis=1), "date"]
    test_dates = select_test_dates(config, available)
    rows = []

    for target_date in test_dates:
        target_values = daily.loc[daily["date"].eq(target_date)].iloc[0]
        for horizon in config["forecast_horizons"]:
            cutoff = target_date - pd.Timedelta(days=horizon)

            for target, actual_col, pax_col, prefix in [
                ("entry", "entries", "departing_pax", "E"),
                ("exit", "exits", "arriving_pax", "X"),
            ]:
                actual = target_values[actual_col]
                passenger_base = target_values[pax_col]
                for window in config["same_weekday_windows"]:
                    values = previous_same_weekdays(
                        daily, target_date, cutoff, actual_col, window
                    )
                    if len(values):
                        add_prediction(
                            rows, target_date, cutoff, horizon, target,
                            f"{prefix}0_same_weekday_{window}", values.mean(), actual,
                        )
                    for weighted, label in [(False, "ratio"), (True, "weighted")]:
                        rate = fit_penetration(
                            daily, target_date, cutoff, actual_col, pax_col,
                            window, weighted,
                        )
                        if pd.notna(rate) and pd.notna(passenger_base):
                            add_prediction(
                                rows, target_date, cutoff, horizon, target,
                                f"{prefix}1_pax_{label}_{window}",
                                passenger_base * rate, actual,
                                passenger_base=passenger_base,
                                note=config["passenger_input_mode"],
                            )

            entry_multiplier = fit_curve_multiplier(
                curves["entry"], target_date, cutoff, horizon
            )
            known_entry = get_known_value(curves["entry"], target_date, horizon)
            add_prediction(
                rows, target_date, cutoff, horizon, "entry", "E2_booking_curve",
                known_entry * entry_multiplier if pd.notna(entry_multiplier) else np.nan,
                target_values["entries"], known=known_entry,
            )
            entry_duration_forecast, entry_duration_known = forecast_duration_curve(
                curves["entry_duration"], target_date, cutoff, horizon,
                entry_multiplier, config["excluded_duration_bands"],
            )
            add_prediction(
                rows, target_date, cutoff, horizon, "entry",
                "E3_duration_specific_curve",
                entry_duration_forecast if entry_duration_known else np.nan,
                target_values["entries"], known=entry_duration_known,
                note="0-1 day band excluded because V6 indicates an operational matching issue",
            )

            exit_multiplier = fit_curve_multiplier(
                curves["exit"], target_date, cutoff, horizon
            )
            known_exit = get_known_value(curves["exit"], target_date, horizon)
            add_prediction(
                rows, target_date, cutoff, horizon, "exit", "X4_planned_exit_curve",
                known_exit * exit_multiplier if pd.notna(exit_multiplier) else np.nan,
                target_values["exits"], known=known_exit,
            )
            known_returns = known_expected_returns_at_cutoff(
                data["master"], target_date, cutoff
            )
            add_prediction(
                rows, target_date, cutoff, horizon, "exit",
                "X2_known_expected_returns",
                known_returns * exit_multiplier if pd.notna(exit_multiplier) else np.nan,
                target_values["exits"], known=known_returns,
            )
            exit_duration_forecast, exit_duration_known = forecast_duration_curve(
                curves["exit_duration"], target_date, cutoff, horizon,
                exit_multiplier, config["excluded_duration_bands"],
            )
            add_prediction(
                rows, target_date, cutoff, horizon, "exit",
                "X3_duration_specific_curve",
                exit_duration_forecast if exit_duration_known else np.nan,
                target_values["exits"], known=exit_duration_known,
            )
            adjusted_exit, adjusted_known = forecast_early_late_adjusted_exits(
                data["master"], target_date, cutoff, config
            )
            add_prediction(
                rows, target_date, cutoff, horizon, "exit",
                "X5_early_late_adjusted",
                adjusted_exit if adjusted_known else np.nan,
                target_values["exits"], known=adjusted_known,
            )

    predictions = pd.DataFrame(rows)
    hybrid_rows = []
    for (target_date, horizon), current in predictions.groupby(["target_date", "horizon"]):
        cutoff = current["forecast_cutoff"].iloc[0]
        specifications = [
            ("entry", "E2_booking_curve", "E1_pax_weighted_8", "E4_hybrid_booking_pax"),
            ("exit", "X5_early_late_adjusted", "X1_pax_weighted_8", "X6_hybrid_return_pax"),
        ]
        for target, method_a, method_b, hybrid_name in specifications:
            first = current[current["target"].eq(target) & current["method"].eq(method_a)]
            second = current[current["target"].eq(target) & current["method"].eq(method_b)]
            if first.empty or second.empty:
                continue
            weight = learn_best_blend(
                predictions, target, horizon, cutoff, method_a, method_b,
                config["hybrid_weight_grid"],
            )
            add_prediction(
                hybrid_rows, target_date, cutoff, horizon, target, hybrid_name,
                weight * first["forecast"].iloc[0] + (1 - weight) * second["forecast"].iloc[0],
                first["actual"].iloc[0],
                note=f"weight_on_{method_a}={weight:.2f}",
            )

    predictions = pd.concat([predictions, pd.DataFrame(hybrid_rows)], ignore_index=True)
    predictions["error"] = predictions["forecast"] - predictions["actual"]
    predictions["absolute_error"] = predictions["error"].abs()
    predictions["squared_error"] = predictions["error"] ** 2
    predictions["underforecast"] = predictions["forecast"].lt(predictions["actual"])
    predictions["within_10"] = predictions["absolute_error"].le(10)
    predictions["within_25"] = predictions["absolute_error"].le(25)
    predictions["asymmetric_cost"] = (
        config["underforecast_cost"]
        * (predictions["actual"] - predictions["forecast"]).clip(lower=0)
        + config["overforecast_cost"]
        * (predictions["forecast"] - predictions["actual"]).clip(lower=0)
    )
    return predictions


# ============================================================
# 6. PERFORMANCE AND MOVEMENTS
# ============================================================

def summarise_predictions(predictions: pd.DataFrame, groups: list[str]) -> pd.DataFrame:
    summary = predictions.groupby(groups, dropna=False).agg(
        mae=("absolute_error", "mean"),
        rmse=("squared_error", lambda x: math.sqrt(x.mean())),
        bias=("error", "mean"),
        total_absolute_error=("absolute_error", "sum"),
        total_actual=("actual", "sum"),
        underforecast_rate=("underforecast", "mean"),
        within_10_rate=("within_10", "mean"),
        within_25_rate=("within_25", "mean"),
        asymmetric_cost=("asymmetric_cost", "mean"),
        records=("forecast", "count"),
    ).reset_index()
    summary["wape"] = (
        summary["total_absolute_error"] / summary["total_actual"].replace(0, np.nan)
    )
    return summary


def create_movement_predictions(predictions, best_daily):
    chosen = predictions.merge(
        best_daily[["target", "horizon", "method"]],
        on=["target", "horizon", "method"], how="inner",
    )
    wide = chosen.pivot_table(
        index=["target_date", "forecast_cutoff", "horizon"],
        columns="target", values=["forecast", "actual"], aggfunc="first",
    ).reset_index()
    if ("forecast", "entry") not in wide or ("forecast", "exit") not in wide:
        return pd.DataFrame()
    result = pd.DataFrame({
        "target_date": wide["target_date"],
        "forecast_cutoff": wide["forecast_cutoff"],
        "horizon": wide["horizon"],
        "forecast": wide[("forecast", "entry")] + wide[("forecast", "exit")],
        "actual": wide[("actual", "entry")] + wide[("actual", "exit")],
    })
    result["error"] = result["forecast"] - result["actual"]
    result["absolute_error"] = result["error"].abs()
    result["squared_error"] = result["error"] ** 2
    return result


# ============================================================
# 7. HOURLY ALLOCATION
# ============================================================

def hourly_profiles(data, target_date, cutoff, target, config):
    actuals = data["hourly_actuals"]
    pax = data["hourly_pax"]
    value_col = "entries" if target == "entry" else "exits"

    history = actuals[
        actuals["datetime_hour"].lt(cutoff)
        & actuals["datetime_hour"].dt.day_name().eq(target_date.day_name())
    ]
    eligible_dates = sorted(history["date"].unique())[-12:]
    historical = (
        history[history["date"].isin(eligible_dates)]
        .groupby("hour")[value_col].sum().reindex(range(24), fill_value=0)
    )
    historical = historical / historical.sum()

    target_pax = pax[pax["date"].eq(target_date)].copy()
    if target == "entry":
        target_pax["operational_hour"] = (
            target_pax["hour"] - config["entry_departure_offset_hours"]
        ) % 24
        pax_col = "departing_pax"
    else:
        target_pax["operational_hour"] = (
            target_pax["hour"] + config["exit_arrival_offset_hours"]
        ) % 24
        pax_col = "arriving_pax"
    flight = target_pax.groupby("operational_hour")[pax_col].sum().reindex(
        range(24), fill_value=0
    )
    flight = flight / flight.sum()
    return historical.fillna(1 / 24), flight.fillna(1 / 24)


def run_hourly_comparison(predictions, best_daily, data, config):
    chosen = predictions.merge(
        best_daily[["target", "horizon", "method"]],
        on=["target", "horizon", "method"], how="inner",
    )
    rows = []
    for _, record in chosen.iterrows():
        historical, flight = hourly_profiles(
            data, record["target_date"], record["forecast_cutoff"],
            record["target"], config,
        )
        value_col = "entries" if record["target"] == "entry" else "exits"
        actual = (
            data["hourly_actuals"][data["hourly_actuals"]["date"].eq(record["target_date"])]
            .groupby("hour")[value_col].sum().reindex(range(24), fill_value=0)
        )
        profiles = {
            "H1_weekday_profile": historical,
            "H2_flight_offset": flight,
        }
        profiles.update({
            f"H3_blend_{weight:.1f}": weight * flight + (1 - weight) * historical
            for weight in config["hourly_blend_grid"]
        })
        for method, profile in profiles.items():
            for hour in range(24):
                rows.append({
                    "target_date": record["target_date"],
                    "horizon": record["horizon"],
                    "target": record["target"],
                    "daily_method": record["method"],
                    "hourly_method": method,
                    "hour": hour,
                    "forecast": record["forecast"] * profile.loc[hour],
                    "actual": actual.loc[hour],
                })
    hourly = pd.DataFrame(rows)
    hourly["error"] = hourly["forecast"] - hourly["actual"]
    hourly["absolute_error"] = hourly["error"].abs()
    hourly["squared_error"] = hourly["error"] ** 2
    return hourly


# ============================================================
# 8. OUTPUTS
# ============================================================

def export_simulation(outputs: dict, output_path: str | Path) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, frame in outputs.items():
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                frame.to_excel(writer, sheet_name=name[:31], index=False)
        for worksheet in writer.book.worksheets:
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions
            worksheet.sheet_view.showGridLines = False
            for cell in worksheet[1]:
                cell.font = cell.font.copy(bold=True, color="FFFFFF")
                cell.fill = cell.fill.copy(fill_type="solid", fgColor="1F4E78")
            for cells in worksheet.columns:
                values = [str(cell.value or "") for cell in cells[:200]]
                width = min(max(max(map(len, values), default=8) + 2, 10), 45)
                worksheet.column_dimensions[cells[0].column_letter].width = width
    print(f"Saved simulation workbook: {output_path}")


# ============================================================
# 9. MAIN PIPELINE
# ============================================================

def run_fastpark_forecast_simulation(sql_connection, output_path=None):
    """Run the full FastPark forecast simulation with stage progress timings."""
    print("FASTPARK FORECAST SIMULATION PIPELINE")
    config = get_simulation_config()
    if output_path is not None:
        config["output_path"] = output_path

    print(f"Analysis window   : {config['analysis_start_date']} to {config['analysis_end_date']}")
    print(f"Simulation window : {config['test_start_date']} to {config['test_end_date']}")
    print(f"Forecast horizons : {config['forecast_horizons']}")
    t0 = time.perf_counter()

    print("[1/8] Loading and Preparing Historical Data...")
    data = load_simulation_data(sql_connection, config)
    t1 = step(t0, f"Loaded Historical Data ({len(data['bookings']):,} bookings)")

    print("[2/8] Building Leakage-Safe As-Of Curves...")
    curves = build_asof_curve_tables(data, config)
    t2 = step(t1, "Built Leakage-Safe Entry and Exit Curves")

    print("[3/8] Running Daily Entry and Exit Scenarios...")
    predictions = run_daily_simulation(data, curves, config)
    t3 = step(t2, f"Ran Daily Scenarios ({len(predictions):,} predictions)")

    print("[4/8] Evaluating Daily Forecast Methods...")
    by_horizon = summarise_predictions(predictions, ["target", "horizon", "method"])
    overall = summarise_predictions(predictions, ["target", "method"])
    eligible = by_horizon[by_horizon["records"].ge(5)]
    best_daily = (
        eligible.sort_values(["target", "horizon", "mae", "wape", "asymmetric_cost"])
        .groupby(["target", "horizon"], as_index=False).head(1).reset_index(drop=True)
    )
    t4 = step(t3, "Evaluated Daily Forecast Methods")

    print("[5/8] Building Combined Movement Forecasts...")
    movements = create_movement_predictions(predictions, best_daily)
    if movements.empty:
        movement_summary = pd.DataFrame()
    else:
        movement_summary = movements.groupby("horizon").agg(
            mae=("absolute_error", "mean"),
            rmse=("squared_error", lambda x: math.sqrt(x.mean())),
            bias=("error", "mean"),
            total_absolute_error=("absolute_error", "sum"),
            total_actual=("actual", "sum"),
            records=("forecast", "count"),
        ).reset_index()
        movement_summary["wape"] = (
            movement_summary["total_absolute_error"]
            / movement_summary["total_actual"].replace(0, np.nan)
        )
    t5 = step(t4, "Built Combined Movement Forecasts")

    print("[6/8] Comparing Hourly Allocation Methods...")
    hourly_detail = run_hourly_comparison(predictions, best_daily, data, config)
    hourly_input = hourly_detail.copy()
    hourly_input["underforecast"] = hourly_input["forecast"].lt(hourly_input["actual"])
    hourly_input["within_10"] = hourly_input["absolute_error"].le(10)
    hourly_input["within_25"] = hourly_input["absolute_error"].le(25)
    hourly_input["asymmetric_cost"] = (
        config["underforecast_cost"]
        * (hourly_input["actual"] - hourly_input["forecast"]).clip(lower=0)
        + config["overforecast_cost"]
        * (hourly_input["forecast"] - hourly_input["actual"]).clip(lower=0)
    )
    hourly_input = hourly_input.rename(columns={"hourly_method": "method"})
    hourly_summary = summarise_predictions(
        hourly_input, ["target", "horizon", "method"]
    )
    best_hourly = (
        hourly_summary.sort_values(["target", "horizon", "mae", "wape"])
        .groupby(["target", "horizon"], as_index=False).head(1).reset_index(drop=True)
    )
    t6 = step(t5, "Compared Hourly Allocation Methods")

    print("[7/8] Building Summary and Output Tables...")
    summary = pd.concat([
        best_daily.assign(section="Best daily method"),
        best_hourly.assign(section="Best hourly method"),
    ], ignore_index=True, sort=False)
    outputs = {
        "Summary": summary,
        "Scenario Catalogue": create_scenario_catalogue(),
        "Best Daily": best_daily,
        "Best Hourly": best_hourly,
        "Movement Summary": movement_summary,
        "All Daily Methods": by_horizon,
        "Overall Methods": overall,
        "Daily Predictions": predictions,
        "Movement Detail": movements,
        "Hourly Summary": hourly_summary,
        "Hourly Detail": hourly_detail,
        "Entry Curves": curves["entry"],
        "Entry Duration Curves": curves["entry_duration"],
        "Exit Curves": curves["exit"],
        "Exit Duration Curves": curves["exit_duration"],
    }
    t7 = step(t6, f"Built Output Tables ({len(outputs)} tables)")

    print(f"[8/8] Exporting Results to {config['output_path']}...")
    export_simulation(outputs, config["output_path"])
    t8 = step(t7, f"Exported Results to {config['output_path']}")

    print(f"Completed FastPark Forecast Simulation in {t8 - t0:.2f} seconds.")
    return outputs


if __name__ == "__main__":
    sql_connection = get_engine(
        dsn="AzureConnection",
        username="jamie_douglas",
    )
    outputs = run_fastpark_forecast_simulation(
        sql_connection=sql_connection,
        output_path=r"output\fastpark\reports\fastpark_forecast_simulation.xlsx",
    )
