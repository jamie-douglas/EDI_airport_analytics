
# scripts/tactical.py
"""
Tactical Readiness — Orchestration (FastPark-style)

Windows:
  - 2-week forward
  - 6-week forward
  - Summer season (from config)

Flow:
  - Load flights & security (script)
  - Summaries & peaks (domain)
  - Charts (viz)
  - Optional saving to <outdir>/tactical/plots/<start>_to_<end>/

Note:
  - SUMMER_START, SUMMER_END, SECURITY_CAX come from modules.config
  - Uses progress.step(...) for compact timing logs like FastPark
"""

from __future__ import annotations

import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
import time

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# --- utils ---
from modules.utils.query import query
from modules.utils.dates import to_datetime
from modules.utils.progress import step

# --- config (season + security capacity) ---
from modules.config import SUMMER_START, SUMMER_END, SECURITY_CAX

# --- domain ---
from modules.domain.tactical import (
    daily_summary,
    arrivals_per_hour,
    arrivals_per_15min,
    peak_arrival_day,
    security_rolling_hour,
    peak_security_hour,
    peak_security_day,
    immigration_queue_15m,
)

# --- viz ---
from modules.viz.tactical import (
    plot_daily_pax_summary,
    plot_hourly_pax,
    plot_weekly_abc,
    plot_security_forecast,
    plot_peak_day_all_sectors,
    plot_peak_security,
    plot_peak_international_immigration,
)

# --------------------------
# HELPERS (loaders live in script)
# --------------------------
def forward_window(weeks: int) -> tuple[str, str]:
    """
    Compute a forward-looking [start, end) date window beginning 'tomorrow'.

    Parameters
    ----------
    weeks : int
        Number of weeks ahead.

    Returns
    -------
    (str, str)
        ISO date strings (YYYY-MM-DD) for [start, end).
    """
    start = datetime.today() + timedelta(days=1)
    end = start + timedelta(days=weeks * 7)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def load_flights(start: str, end: str) -> pd.DataFrame:
    """
    Load future flights for [start, end) with canonical columns.

    Parameters
    ----------
    start : str
        Inclusive start date (YYYY-MM-DD).
    end : str
        Exclusive end date (YYYY-MM-DD).

    Returns
    -------
    pandas.DataFrame
        Columns: 'Schedule' (datetime64[ns]), 'A/D' ('A'/'D'), 'Pax', 'Sector'.
    """
    df = query(
        table="Eal.FlightPerformance_FutureFlights",
        columns=[
            "ScheduledDateTime_Local AS [Schedule]",
            "ArrDeptureCode         AS [A/D]",
            "PAX_MostConfident      AS [Pax]",
            "Sector",
        ],
        date_column="ScheduledDateTime_Local",
        start=start, end=end,
    )
    df = to_datetime(df, "Schedule")
    return df


def load_security(start: str, end: str) -> pd.DataFrame:
    """
    Load security forecast for [start, end) with convenience columns.

    Parameters
    ----------
    start : str
        Inclusive start date (YYYY-MM-DD).
    end : str
        Exclusive end date (YYYY-MM-DD).

    Returns
    -------
    pandas.DataFrame
        Columns: 'Forecast DateTime' (datetime64[ns]), 'Pax', 'Staff', 'Total',
                 'Date' (date), 'Hour' (int).
    """
    df = query(
        table="Planning.V_OperationsForecast",
        columns=[
            "ForecastDateTime AS [Forecast DateTime]",
            "Pax",
            "Staff",
            "Total",
        ],
        date_column="ForecastDateTime",
        start=start, end=end,
    )
    df = to_datetime(df, "Forecast DateTime")
    df["Date"] = df["Forecast DateTime"].dt.date
    df["Hour"] = df["Forecast DateTime"].dt.hour
    return df


def build_plot_dir(base_outdir: str | None, start: str, end: str) -> Path | None:
    """
    Create (or return) the output directory for a given window.

    Parameters
    ----------
    base_outdir : str or None
        Base output directory (e.g., "output"). If None, returns None (no saving).
    start : str
        Window start (YYYY-MM-DD).
    end : str
        Window end (YYYY-MM-DD).

    Returns
    -------
    pathlib.Path or None
        The created directory path: <base_outdir>/tactical/plots/<start>_to_<end>/,
        or None if base_outdir is None.
    """
    if not base_outdir:
        return None
    d = Path(base_outdir) / "tactical" / "plots" / f"{start}_to_{end}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def sp(dirpath: Path | None, filename: str) -> str | None:
    """
    Return a full save path inside the given directory, or None if dir is None.

    Parameters
    ----------
    dirpath : pathlib.Path or None
        Directory to place the file in.
    filename : str
        File name (e.g., "daily.png").

    Returns
    -------
    str or None
        String path if dirpath is provided; otherwise None.
    """
    return str(dirpath / filename) if dirpath else None


# --------------------------
# MAIN ORCHESTRATION
# --------------------------
def main(weeks_short: int, weeks_long: int, summer_start: str, summer_end: str, outdir: str | None) -> None:
    """
    Run Tactical Readiness orchestration for 2-week, 6-week, and Summer windows.

    Parameters
    ----------
    weeks_short : int
        Short forward window in weeks (e.g., 2).
    weeks_long : int
        Long forward window in weeks (e.g., 6).
    summer_start : str
        Summer season start (YYYY-MM-DD).
    summer_end : str
        Summer season end (YYYY-MM-DD).
    outdir : str or None
        Base output directory to save plots; if None, figures are just shown.

    Returns
    -------
    None
        Plots are rendered (and optionally saved). Console prints a brief summary.
    """
    print("\nTACTICAL READINESS — Orchestration (2-week, 6-week, Summer)")
    print(f"Short window : {weeks_short} weeks")
    print(f"Long window  : {weeks_long} weeks")
    print(f"Summer       : {summer_start} → {summer_end}")
    if outdir:
        print(f"Output dir   : {Path(outdir).resolve()}")
    print()

    t0 = time.perf_counter()
    t0 = step(t0, "Start")

    # [1/6] Load Summer (threshold baseline + season views)
    print("[1/6] Loading Summer flights/security…")
    fl_su = load_flights(summer_start, summer_end)
    sec_su = load_security(summer_start, summer_end)
    t1 = step(t0, f"Summer flights/security: {len(fl_su):,} / {len(sec_su):,}")

    su_summary, su_A, su_B = daily_summary(fl_su)
    t2 = step(t1, f"Derived thresholds from Summer (A={int(su_A):,}, B={int(su_B):,})")

    # [2/6] 2-week
    print("[2/6] Loading 2-week forward window…")
    s2w, e2w = forward_window(weeks_short)
    fl_2w = load_flights(s2w, e2w)
    sec_2w = load_security(s2w, e2w)
    t3 = step(t2, f"2w flights/security: {len(fl_2w):,} / {len(sec_2w):,}")

    # [3/6] 6-week
    print("[3/6] Loading 6-week forward window…")
    s6w, e6w = forward_window(weeks_long)
    fl_6w = load_flights(s6w, e6w)
    sec_6w = load_security(s6w, e6w)
    t4 = step(t3, f"6w flights/security: {len(fl_6w):,} / {len(sec_6w):,}")

    # [4/6] Daily A/B/C + Hourly arrivals
    print("[4/6] Computing daily A/B/C rankings and hourly arrivals…")
    sum_2w, _, _ = daily_summary(fl_2w, a_threshold=su_A, b_threshold=su_B)
    sum_6w, _, _ = daily_summary(fl_6w, a_threshold=su_A, b_threshold=su_B)
    sum_su, _, _ = daily_summary(fl_su, a_threshold=su_A, b_threshold=su_B)

    hr_2w = arrivals_per_hour(fl_2w)
    hr_6w = arrivals_per_hour(fl_6w)
    hr_su = arrivals_per_hour(fl_su)

    sectors_2w = [c for c in hr_2w.columns if c not in ["Date", "Hour", "Hour_Label"]]
    sectors_6w = [c for c in hr_6w.columns if c not in ["Date", "Hour", "Hour_Label"]]
    sectors_su = [c for c in hr_su.columns if c not in ["Date", "Hour", "Hour_Label"]]
    t5 = step(t4, "Daily A/B/C + hourly arrivals complete")

    # [5/6] Peaks (arrivals + security)
    print("[5/6] Finding peaks (arrivals, security day, security rolling hour)…")
    pk_day_2w, pk_val_2w = peak_arrival_day(hr_2w, sectors_2w)
    pk_day_6w, pk_val_6w = peak_arrival_day(hr_6w, sectors_6w)
    pk_day_su, pk_val_su = peak_arrival_day(hr_su, sectors_su)

    sec_2w_rh = security_rolling_hour(sec_2w)
    sec_6w_rh = security_rolling_hour(sec_6w)
    sec_su_rh = security_rolling_hour(sec_su)

    pk_sec_2w = peak_security_hour(sec_2w_rh)
    pk_sec_6w = peak_security_hour(sec_6w_rh)
    pk_sec_su = peak_security_hour(sec_su_rh)

    pk_sec_day_su = peak_security_day(sec_su)
    t6 = step(t5, "Peaks computed")

    # [6/6] Immigration (15‑min) for peak International days
    print("[6/6] Building 15‑minute immigration + queue for peak International days…")
    pk_intl_2w = hr_2w.groupby("Date")["International"].sum().idxmax() if "International" in hr_2w.columns else None
    pk_intl_6w = hr_6w.groupby("Date")["International"].sum().idxmax() if "International" in hr_6w.columns else None
    pk_intl_su = hr_su.groupby("Date")["International"].sum().idxmax() if "International" in hr_su.columns else None

    a15_2w = arrivals_per_15min(fl_2w) if pk_intl_2w is not None else pd.DataFrame()
    a15_6w = arrivals_per_15min(fl_6w) if pk_intl_6w is not None else pd.DataFrame()
    a15_su = arrivals_per_15min(fl_su) if pk_intl_su is not None else pd.DataFrame()

    imm_2w = immigration_queue_15m(a15_2w, pk_intl_2w) if pk_intl_2w is not None else pd.DataFrame()
    imm_6w = immigration_queue_15m(a15_6w, pk_intl_6w) if pk_intl_6w is not None else pd.DataFrame()
    imm_su = immigration_queue_15m(a15_su, pk_intl_su) if pk_intl_su is not None else pd.DataFrame()

    step(t6, "Immigration queues ready")

    # --------------------------
    # OUTPUT DIRECTORIES
    # --------------------------
    dir_2w = build_plot_dir(outdir, s2w, e2w)
    dir_6w = build_plot_dir(outdir, s6w, e6w)
    dir_su = build_plot_dir(outdir, summer_start, summer_end)

    # --------------------------
    # PLOTS — 2-week
    # --------------------------
    plot_daily_pax_summary(sum_2w, title="Daily Arrivals / Departures (2-week)",
                           save_path=sp(dir_2w, "daily.png"))
    plot_hourly_pax(hr_2w, sectors_2w, title="Hourly Arrivals by Sector (2-week)",
                    international_only=True, show_ia2_baseline=False, show_total_imm_baseline=True,
                    save_path=sp(dir_2w, "hourly.png"))
    plot_weekly_abc(sum_2w, a_threshold=su_A, b_threshold=su_B, window_label="(2-week)",
                    save_path=sp(dir_2w, "weekly_abc.png"))
    plot_security_forecast(sec_2w_rh, capacity_line=SECURITY_CAX, title="Security Forecast (2-week)",
                           save_path=sp(dir_2w, "security_forecast.png"))
    plot_peak_day_all_sectors(hr_2w, sectors_2w, pk_day_2w,
                              title=f"Peak Arrival Day (2-week): {pd.to_datetime(pk_day_2w).strftime('%d %b')}",
                              save_path=sp(dir_2w, "peak_day.png"))
    plot_peak_security(sec_2w_rh, pk_sec_2w, capacity_line=SECURITY_CAX, title_prefix="Peak Security (2-week)",
                       save_path=sp(dir_2w, "peak_security.png"))
    if not imm_2w.empty:
        plot_peak_international_immigration(imm_2w, pk_intl_2w, window_label="(2-week)",
                                            save_path=sp(dir_2w, "immigration.png"))

    # --------------------------
    # PLOTS — 6-week
    # --------------------------
    plot_daily_pax_summary(sum_6w, title="Daily Arrivals / Departures (6-week)",
                           save_path=sp(dir_6w, "daily.png"))
    plot_hourly_pax(hr_6w, sectors_6w, title="Hourly Arrivals by Sector (6-week)",
                    international_only=True, show_ia2_baseline=False, show_total_imm_baseline=True,
                    save_path=sp(dir_6w, "hourly.png"))
    plot_weekly_abc(sum_6w, a_threshold=su_A, b_threshold=su_B, window_label="(6-week)",
                    save_path=sp(dir_6w, "weekly_abc.png"))
    plot_peak_day_all_sectors(hr_6w, sectors_6w, pk_day_6w,
                              title=f"Peak Arrival Day (6-week): {pd.to_datetime(pk_day_6w).strftime('%d %b')}",
                              save_path=sp(dir_6w, "peak_day.png"))
    plot_peak_security(sec_6w_rh, pk_sec_6w, capacity_line=SECURITY_CAX, title_prefix="Peak Security (6-week)",
                       save_path=sp(dir_6w, "peak_security.png"))
    if not imm_6w.empty:
        plot_peak_international_immigration(imm_6w, pk_intl_6w, window_label="(6-week)",
                                            save_path=sp(dir_6w, "immigration.png"))

    # --------------------------
    # PLOTS — Summer
    # --------------------------
    plot_peak_security(sec_su_rh, pk_sec_su, capacity_line=SECURITY_CAX, title_prefix="Peak Security (Summer)",
                       save_path=sp(dir_su, "peak_security.png"))
    if not imm_su.empty:
        plot_peak_international_immigration(imm_su, pk_intl_su, window_label="(Summer)",
                                            save_path=sp(dir_su, "immigration.png"))

    if outdir:
        step(t6, f"Plots saved under {Path(outdir) / 'tactical' / 'plots'}")

    # --------------------------
    # SUMMARY (console)
    # --------------------------
    print("\n--- SUMMARY ---")
    print(f"2-week Peak Arrival Day: {pk_day_2w} ({int(pk_val_2w):,} pax)")
    print(
        "2-week Peak Security Hour: "
        f"{pk_sec_2w['Window Start']}-{pk_sec_2w['Window End']} on {pk_sec_2w['Date']} "
        f"({pk_sec_2w['Total RH']} total)"
    )

    print(f"6-week Peak Arrival Day: {pk_day_6w} ({int(pk_val_6w):,} pax)")
    print(
        "6-week Peak Security Hour: "
        f"{pk_sec_6w['Window Start']}-{pk_sec_6w['Window End']} on {pk_sec_6w['Date']} "
        f"({pk_sec_6w['Total RH']} total)"
    )

    print("\n--- SUMMER SEASON ---")
    print(f"Period: {summer_start} to {summer_end}")

    abc_counts = sum_su["Ranking"].value_counts().reindex(["A", "B", "C"], fill_value=0)
    print("\nA/B/C Days:")
    print(f"  A Days: {abc_counts['A']}")
    print(f"  B Days: {abc_counts['B']}")
    print(f"  C Days: {abc_counts['C']}")

    print(f"\nPeak Arrival Day: {pk_day_su} ({int(pk_val_su):,} pax)")

    print("\n--- SUMMER PEAK SECURITY HOUR ---")
    print(f"Window: {pk_sec_su['Window Start']} - {pk_sec_su['Window End']}")
    print(f"Date: {pk_sec_su['Date']}")
    print(f"Passengers (RH): {pk_sec_su['Pax RH']}")
    print(f"Staff (RH): {pk_sec_su['Staff RH']}")
    print(f"Total (RH): {pk_sec_su['Total RH']}")

    # Extra (optional parity info)
    daily_total_arrivals = hr_su.groupby("Date")[sectors_su].sum().sum(axis=1)
    daily_intl_arrivals = hr_su.groupby("Date")["International"].sum() if "International" in hr_su.columns else pd.Series(dtype=float)

    if not daily_total_arrivals.empty:
        peak_total_day = daily_total_arrivals.idxmax()
        total_pax_on_peak_total_day = daily_total_arrivals.loc[peak_total_day]
        intl_pax_on_peak_total_day = daily_intl_arrivals.loc[peak_total_day] if not daily_intl_arrivals.empty else 0
        print(f"\nPeak Total Arrivals Day: {peak_total_day} -> Total: {int(total_pax_on_peak_total_day):,}, "
              f"International: {int(intl_pax_on_peak_total_day):,}")

    if not daily_intl_arrivals.empty:
        peak_intl_day = daily_intl_arrivals.idxmax()
        total_pax_on_peak_intl_day = daily_total_arrivals.loc[peak_intl_day]
        intl_pax_on_peak_intl_day = daily_intl_arrivals.loc[peak_intl_day]
        print(f"Peak International Arrivals Day: {peak_intl_day} -> Total: {int(total_pax_on_peak_intl_day):,}, "
              f"International: {int(intl_pax_on_peak_intl_day):,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tactical Readiness — 2-week, 6-week, and Summer views.")
    parser.add_argument("--weeks-short", type=int, default=2, help="Short forward window in weeks (default 2).")
    parser.add_argument("--weeks-long",  type=int, default=6, help="Long forward window in weeks (default 6).")
    parser.add_argument("--summer-start", default=SUMMER_START, help="Summer start (YYYY-MM-DD).")
    parser.add_argument("--summer-end",   default=SUMMER_END,   help="Summer end (YYYY-MM-DD).")
    parser.add_argument("--outdir", default=None, help="Base output directory for plots. If omitted, figures are only shown.")
    args = parser.parse_args()

    main(args.weeks_short, args.weeks_long, args.summer_start, args.summer_end, args.outdir)
