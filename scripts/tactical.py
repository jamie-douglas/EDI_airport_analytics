
# scripts/tactical.py
from __future__ import annotations

import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

from modules.utils.query import query
from modules.utils.dates import to_datetime
from modules.utils.progress import step

from modules.config import SUMMER_START, SUMMER_END, SECURITY_CAX

from modules.domain.tactical import (
    daily_summary,
    arrivals_per_hour,
    arrivals_per_15min,
    security_rolling_hour,
    peak_security_hour,
    immigration_queue_15m,
    immigration_queue_15m_all_days,
    immigration_overflow_windows,
    security_peak_utilisation,   # 🇬🇧
)

from modules.viz.tactical import (
    plot_daily_pax_summary,
    plot_weekly_abc,
    plot_peak_security,
    plot_peak_international_immigration,
    render_table_png,
)

# ------------------------- loaders -------------------------
def forward_window(weeks: int) -> tuple[str, str]:
    s = datetime.today() + timedelta(days=1)
    e = s + timedelta(days=weeks * 7)
    return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")


def load_flights(start: str, end: str) -> pd.DataFrame:
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
    return to_datetime(df, "Schedule")


def load_security(start: str, end: str) -> pd.DataFrame:
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


# ------------------------- outputs -------------------------
def build_plot_dir(base_outdir: str | None, start: str, end: str) -> Path | None:
    if not base_outdir:
        return None
    d = Path(base_outdir) / "tactical" / "plots" / f"{start}_to_{end}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def sp(dirpath: Path | None, filename: str) -> str | None:
    return str(dirpath / filename) if dirpath else None


def export_csv(df: pd.DataFrame, dirpath: Path | None, filename: str, print_saves: bool = False):
    if dirpath is None:
        return
    out = dirpath / filename
    df.to_csv(out, index=False)
    if print_saves:
        print(f"    Saved: {out}")


def format_overflow_table(tbl: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare overflow windows table for printing/rendering:
    columns → Date, Start, End, Duration (min), Max Overflow.
    """
    if tbl.empty:
        return tbl

    t = tbl.copy().sort_values(["Date", "Start"])
    t["Date"]  = pd.to_datetime(t["Date"]).dt.strftime("%d %b")
    t["Start"] = pd.to_datetime(t["Start"]).dt.strftime("%d %b %H:%M")
    t["End"]   = pd.to_datetime(t["End"]).dt.strftime("%d %b %H:%M")
    t["Duration (min)"] = t["Duration_Minutes"].astype(int)
    t["Max Overflow"]   = pd.to_numeric(t["Max_Overflow"], errors="coerce").round(0).astype("Int64")

    return t[["Date", "Start", "End", "Duration (min)", "Max Overflow"]]


# ------------------------- main -------------------------
def main(weeks_short: int, weeks_mid: int, summer_start: str, summer_end: str, outdir: str | None,
         print_saves: bool, to_csv: bool) -> None:

    print("\nTACTICAL READINESS — Orchestration (2-week, 4-week, Summer)")
    print(f"Short window : {weeks_short} weeks")
    print(f"Mid window   : {weeks_mid} weeks")
    print(f"Summer       : {summer_start} → {summer_end}")
    if outdir:
        print(f"Output dir   : {Path(outdir).resolve()}")
    print()

    t0 = step(0.0, "Start")

    # [1] Summer baseline (thresholds)
    print("[1/6] Loading Summer flights/security…")
    fl_su = load_flights(summer_start, summer_end)
    sec_su = load_security(summer_start, summer_end)
    t1 = step(t0, f"Summer flights/security: {len(fl_su):,} / {len(sec_su):,}")
    su_summary, su_A, su_B = daily_summary(fl_su)
    t2 = step(t1, f"Derived thresholds from Summer (A={int(su_A):,}, B={int(su_B):,})")

    # [2] 2-week
    print("[2/6] Loading 2-week forward window…")
    s2w, e2w = forward_window(weeks_short)
    fl_2w = load_flights(s2w, e2w)
    sec_2w = load_security(s2w, e2w)
    t3 = step(t2, f"2w flights/security: {len(fl_2w):,} / {len(sec_2w):,}")

    # [3] 4-week
    print("[3/6] Loading 4-week forward window…")
    s4w, e4w = forward_window(weeks_mid)
    fl_4w = load_flights(s4w, e4w)
    sec_4w = load_security(s4w, e4w)
    t4 = step(t3, f"4w flights/security: {len(fl_4w):,} / {len(sec_4w):,}")

    # [4] Daily A/B/C + Hourly
    print("[4/6] Computing daily A/B/C rankings and hourly arrivals…")
    sum_2w, _, _ = daily_summary(fl_2w, a_threshold=su_A, b_threshold=su_B)
    sum_4w, _, _ = daily_summary(fl_4w, a_threshold=su_A, b_threshold=su_B)
    sum_su, _, _ = daily_summary(fl_su, a_threshold=su_A, b_threshold=su_B)

    hr_2w = arrivals_per_hour(fl_2w)
    hr_4w = arrivals_per_hour(fl_4w)
    hr_su = arrivals_per_hour(fl_su)
    t5 = step(t4, "Daily A/B/C + hourly arrivals complete")

    # [5] Security RH + peak details + utilisation
    print("[5/6] Security rolling-hour + peaks…")
    sec_2w_rh = security_rolling_hour(sec_2w)
    sec_4w_rh = security_rolling_hour(sec_4w)
    sec_su_rh = security_rolling_hour(sec_su)

    pk_sec_2w = peak_security_hour(sec_2w_rh)
    pk_sec_4w = peak_security_hour(sec_4w_rh)
    pk_sec_su = peak_security_hour(sec_su_rh)

    util_2w = security_peak_utilisation(pk_sec_2w, SECURITY_CAX)
    util_4w = security_peak_utilisation(pk_sec_4w, SECURITY_CAX)
    util_su = security_peak_utilisation(pk_sec_su, SECURITY_CAX)
    t6 = step(t5, "Peaks computed")

    # [6] Immigration: 15-min arrivals, peak-day queue (charts), and all-days queue (overflow windows)
    print("[6/6] Immigration 15-min queues…")
    # Peak International day per window (from hourly arrivals)
    pk_intl_2w = hr_2w.groupby("Date")["International"].sum().idxmax() if "International" in hr_2w.columns else None
    pk_intl_4w = hr_4w.groupby("Date")["International"].sum().idxmax() if "International" in hr_4w.columns else None
    pk_intl_su = hr_su.groupby("Date")["International"].sum().idxmax() if "International" in hr_su.columns else None

    a15_2w = arrivals_per_15min(fl_2w) if pk_intl_2w is not None else pd.DataFrame()
    a15_4w = arrivals_per_15min(fl_4w) if pk_intl_4w is not None else pd.DataFrame()
    a15_su = arrivals_per_15min(fl_su) if pk_intl_su is not None else pd.DataFrame()

    imm_2w_peak = immigration_queue_15m(a15_2w, pk_intl_2w) if pk_intl_2w is not None else pd.DataFrame()
    imm_4w_peak = immigration_queue_15m(a15_4w, pk_intl_4w) if pk_intl_4w is not None else pd.DataFrame()
    imm_su_peak = immigration_queue_15m(a15_su, pk_intl_su) if pk_intl_su is not None else pd.DataFrame()

    imm_2w_all = immigration_queue_15m_all_days(a15_2w) if not a15_2w.empty else pd.DataFrame()
    imm_4w_all = immigration_queue_15m_all_days(a15_4w) if not a15_4w.empty else pd.DataFrame()
    imm_su_all = immigration_queue_15m_all_days(a15_su) if not a15_su.empty else pd.DataFrame()

    
    ov_2w = immigration_overflow_windows(imm_2w_all, criterion="queue_gt_capacity") if not imm_2w_all.empty else pd.DataFrame()
    ov_4w = immigration_overflow_windows(imm_4w_all, criterion="queue_gt_capacity") if not imm_4w_all.empty else pd.DataFrame()
    ov_su = immigration_overflow_windows(imm_su_all, criterion="queue_gt_capacity") if not imm_su_all.empty else pd.DataFrame()


    step(t6, "Immigration queues & overflow windows ready")

    # Output dirs
    dir_2w = build_plot_dir(outdir, s2w, e2w)
    dir_4w = build_plot_dir(outdir, s4w, e4w)
    dir_su = build_plot_dir(outdir, summer_start, summer_end)
    ps = bool(outdir) and bool(print_saves)

    # PLOTS — as requested
    plot_daily_pax_summary(sum_2w, title="Daily Arrivals / Departures (2-week)", save_path=sp(dir_2w, "daily.png"));      ps and print(f"    Saved: {sp(dir_2w,'daily.png')}")
    plot_daily_pax_summary(sum_4w, title="Daily Arrivals / Departures (4-week)", save_path=sp(dir_4w, "daily.png"));      ps and print(f"    Saved: {sp(dir_4w,'daily.png')}")

    plot_weekly_abc(sum_2w, a_threshold=su_A, b_threshold=su_B, window_label="(2-week)",  save_path=sp(dir_2w,"weekly_abc.png")); ps and print(f"    Saved: {sp(dir_2w,'weekly_abc.png')}")
    plot_weekly_abc(sum_4w, a_threshold=su_A, b_threshold=su_B, window_label="(4-week)",  save_path=sp(dir_4w,"weekly_abc.png")); ps and print(f"    Saved: {sp(dir_4w,'weekly_abc.png')}")
    plot_weekly_abc(sum_su, a_threshold=su_A, b_threshold=su_B, window_label="(Summer)",  save_path=sp(dir_su,"weekly_abc.png")); ps and print(f"    Saved: {sp(dir_su,'weekly_abc.png')}")

    plot_peak_security(sec_2w_rh, pk_sec_2w, capacity_line=SECURITY_CAX, title_prefix="Peak Security (2-week)", save_path=sp(dir_2w, "peak_security.png")); ps and print(f"    Saved: {sp(dir_2w,'peak_security.png')}")
    plot_peak_security(sec_4w_rh, pk_sec_4w, capacity_line=SECURITY_CAX, title_prefix="Peak Security (4-week)", save_path=sp(dir_4w, "peak_security.png")); ps and print(f"    Saved: {sp(dir_4w,'peak_security.png')}")
    plot_peak_security(sec_su_rh, pk_sec_su, capacity_line=SECURITY_CAX, title_prefix="Peak Security (Summer)", save_path=sp(dir_su, "peak_security.png")); ps and print(f"    Saved: {sp(dir_su,'peak_security.png')}")

    if not imm_2w_peak.empty:
        plot_peak_international_immigration(imm_2w_peak, pk_intl_2w, window_label="(2-week)", save_path=sp(dir_2w, "immigration.png")); ps and print(f"    Saved: {sp(dir_2w,'immigration.png')}")
    if not imm_4w_peak.empty:
        plot_peak_international_immigration(imm_4w_peak, pk_intl_4w, window_label="(4-week)", save_path=sp(dir_4w, "immigration.png")); ps and print(f"    Saved: {sp(dir_4w,'immigration.png')}")
    if not imm_su_peak.empty:
        plot_peak_international_immigration(imm_su_peak, pk_intl_su, window_label="(Summer)", save_path=sp(dir_su, "immigration.png")); ps and print(f"    Saved: {sp(dir_su,'immigration.png')}")

    # CSV exports (optional)
    if to_csv:
        export_csv(sum_2w, dir_2w, "daily_summary.csv", print_saves=ps)
        export_csv(sum_4w, dir_4w, "daily_summary.csv", print_saves=ps)
        export_csv(sum_su, dir_su, "daily_summary.csv", print_saves=ps)

        export_csv(sec_2w_rh, dir_2w, "security_rolling.csv", print_saves=ps)
        export_csv(sec_4w_rh, dir_4w, "security_rolling.csv", print_saves=ps)
        export_csv(sec_su_rh, dir_su, "security_rolling.csv", print_saves=ps)

        if not imm_2w_peak.empty: export_csv(imm_2w_peak, dir_2w, "immigration_peakday_15min.csv", print_saves=ps)
        if not imm_4w_peak.empty: export_csv(imm_4w_peak, dir_4w, "immigration_peakday_15min.csv", print_saves=ps)
        if not imm_su_peak.empty: export_csv(imm_su_peak, dir_su, "immigration_peakday_15min.csv", print_saves=ps)

        if not ov_2w.empty: export_csv(ov_2w, dir_2w, "immigration_overflow_windows.csv", print_saves=ps)
        if not ov_4w.empty: export_csv(ov_4w, dir_4w, "immigration_overflow_windows.csv", print_saves=ps)
        if not ov_su.empty: export_csv(ov_su, dir_su, "immigration_overflow_windows.csv", print_saves=ps)


    def _days_with_overflow(df_windows: pd.DataFrame) -> int:
        return 0 if df_windows.empty else df_windows["Date"].nunique()

    # Save PNG tables (top 30 rows, slide-sized)
    if outdir:
        if not ov_2w.empty:
            render_table_png(
                format_overflow_table(ov_2w),
                title="Immigration Overflow Windows (2‑week)",
                save_path=sp(dir_2w, "immigration_overflow_windows.png"),
                max_rows=30,
                col_widths=[0.8, 1.2, 1.2, 0.9, 0.9],
            ); ps and print(f"    Saved: {sp(dir_2w,'immigration_overflow_windows.png')}")
        if not ov_4w.empty:
            render_table_png(
                format_overflow_table(ov_4w),
                title="Immigration Overflow Windows (4‑week)",
                save_path=sp(dir_4w, "immigration_overflow_windows.png"),
                max_rows=30,
                col_widths=[0.8, 1.2, 1.2, 0.9, 0.9],
            ); ps and print(f"    Saved: {sp(dir_4w,'immigration_overflow_windows.png')}")
        if not ov_su.empty:
            render_table_png(
                format_overflow_table(ov_su),
                title="Immigration Overflow Windows (Summer)",
                save_path=sp(dir_su, "immigration_overflow_windows.png"),
                max_rows=30,
                col_widths=[0.8, 1.2, 1.2, 0.9, 0.9],
            ); ps and print(f"    Saved: {sp(dir_su,'immigration_overflow_windows.png')}")
    
    # ------------------------------------------------------------------
    # LEGACY-STYLE SUMMARY (added back; 4-week replaces the old 6-week)
    # ------------------------------------------------------------------
    print("\n--- SUMMARY ---")

    # 2-week Peak Arrival Day (sum across all sector columns for that day)
    sectors_2w = [c for c in hr_2w.columns if c not in ["Date", "Hour", "Hour_Label"]]
    daily_tot_2w = hr_2w.groupby("Date")[sectors_2w].sum().sum(axis=1) if sectors_2w else pd.Series(dtype=float)
    pk_arr_2w_date = daily_tot_2w.idxmax() if not daily_tot_2w.empty else None
    pk_arr_2w_val  = int(daily_tot_2w.loc[pk_arr_2w_date]) if pk_arr_2w_date is not None else 0
    print(f"2-week Peak Arrival Day: {pk_arr_2w_date} ({pk_arr_2w_val:,} pax)")

    print(
        f"2-week Peak Security Hour: "
        f"{pk_sec_2w['Window Start']}-{pk_sec_2w['Window End']} "
        f"on {pk_sec_2w['Date']} ({pk_sec_2w['Total RH']} total)"
    )

    # 4-week Peak Arrival Day (same method)
    sectors_4w = [c for c in hr_4w.columns if c not in ["Date", "Hour", "Hour_Label"]]
    daily_tot_4w = hr_4w.groupby("Date")[sectors_4w].sum().sum(axis=1) if sectors_4w else pd.Series(dtype=float)
    pk_arr_4w_date = daily_tot_4w.idxmax() if not daily_tot_4w.empty else None
    pk_arr_4w_val  = int(daily_tot_4w.loc[pk_arr_4w_date]) if pk_arr_4w_date is not None else 0
    print(f"4-week Peak Arrival Day: {pk_arr_4w_date} ({pk_arr_4w_val:,} pax)")

    print(
        f"4-week Peak Security Hour: "
        f"{pk_sec_4w['Window Start']}-{pk_sec_4w['Window End']} "
        f"on {pk_sec_4w['Date']} ({pk_sec_4w['Total RH']} total)"
    )

    # --- SUMMER SEASON ---
    print("\n--- SUMMER SEASON ---")
    print(f"Period: {summer_start} to {summer_end}")

    # A/B/C counts
    abc_counts = sum_su["Ranking"].value_counts().reindex(["A", "B", "C"], fill_value=0)
    print("\nA/B/C Days:")
    print(f"  A Days: {abc_counts['A']}")
    print(f"  B Days: {abc_counts['B']}")
    print(f"  C Days: {abc_counts['C']}")

    # Summer Peak Arrival Day (across all sectors)
    sectors_su = [c for c in hr_su.columns if c not in ["Date", "Hour", "Hour_Label"]]
    daily_tot_su = hr_su.groupby("Date")[sectors_su].sum().sum(axis=1) if sectors_su else pd.Series(dtype=float)
    pk_arr_su_date = daily_tot_su.idxmax() if not daily_tot_su.empty else None
    pk_arr_su_val  = int(daily_tot_su.loc[pk_arr_su_date]) if pk_arr_su_date is not None else 0
    print(f"\nPeak Arrival Day: {pk_arr_su_date} ({pk_arr_su_val:,} pax)")

    # Summer Peak Security Hour details (as before)
    print("\n--- SUMMER PEAK SECURITY HOUR ---")
    print(f"Window: {pk_sec_su['Window Start']} - {pk_sec_su['Window End']}")
    print(f"Date: {pk_sec_su['Date']}")
    print(f"Passengers (RH): {pk_sec_su['Pax RH']}")
    print(f"Staff (RH): {pk_sec_su['Staff RH']}")
    print(f"Total (RH): {pk_sec_su['Total RH']}")

    # Console summary — utilisation + overflow days
    print("\n--- PEAK SECURITY UTILISATION ---")
    print(f"2-week : {util_2w:0.1f}% of capacity (Total RH {pk_sec_2w['Total RH']:,} vs {SECURITY_CAX:,})")
    print(f"4-week : {util_4w:0.1f}% of capacity (Total RH {pk_sec_4w['Total RH']:,} vs {SECURITY_CAX:,})")
    print(f"Summer : {util_su:0.1f}% of capacity (Total RH {pk_sec_su['Total RH']:,} vs {SECURITY_CAX:,})")

    # Peak Total Arrivals Day vs Peak International Arrivals Day (Summer)
    daily_total_arrivals_su = daily_tot_su  # already computed
    if "International" in hr_su.columns:
        daily_intl_arrivals_su = hr_su.groupby("Date")["International"].sum()
    else:
        daily_intl_arrivals_su = pd.Series(dtype=float)
    
    print("\n--- IMMIGRATION OVERFLOW WINDOWS (compressed) ---")
    print(f"2-week  : total days where queue moves into overflow: {_days_with_overflow(ov_2w)}")
    print(f"4-week  : total days where queue moves into overflow: {_days_with_overflow(ov_4w)}")
    print(f"Summer  : total days where queue moves into overflow: {_days_with_overflow(ov_su)}")

    if not daily_total_arrivals_su.empty:
        peak_total_day = daily_total_arrivals_su.idxmax()
        total_pax_on_peak_total_day = int(daily_total_arrivals_su.loc[peak_total_day])
        intl_pax_on_peak_total_day = int(daily_intl_arrivals_su.loc[peak_total_day]) if not daily_intl_arrivals_su.empty and peak_total_day in daily_intl_arrivals_su.index else 0
        print(f"\nPeak Total Arrivals Day: {peak_total_day} -> Total: {total_pax_on_peak_total_day:,}, International: {intl_pax_on_peak_total_day:,}")

    if not daily_intl_arrivals_su.empty:
        peak_intl_day = daily_intl_arrivals_su.idxmax()
        total_pax_on_peak_intl_day = int(daily_total_arrivals_su.loc[peak_intl_day]) if peak_intl_day in daily_total_arrivals_su.index else 0
        intl_pax_on_peak_intl_day = int(daily_intl_arrivals_su.loc[peak_intl_day])
        print(f"Peak International Arrivals Day: {peak_intl_day} -> Total: {total_pax_on_peak_intl_day:,}, International: {intl_pax_on_peak_intl_day:,}")



if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Tactical Readiness — 2-week, 4-week, and Summer views.")
    ap.add_argument("--weeks-short", type=int, default=2, help="Short forward window in weeks (default 2).")
    ap.add_argument("--weeks-mid",   type=int, default=4, help="Mid forward window in weeks (default 4).")
    ap.add_argument("--summer-start", default=SUMMER_START, help="Summer start (YYYY-MM-DD).")
    ap.add_argument("--summer-end",   default=SUMMER_END,   help="Summer end (YYYY-MM-DD).")
    ap.add_argument("--outdir", default=None, help="Base output directory for plots.")
    ap.add_argument("--print-saves", action="store_true", help="Print a 'Saved:' line for every saved figure/table.")
    ap.add_argument("--csv", action="store_true", help="Export compact CSVs for Daily/Security/Immigration outputs.")
    args = ap.parse_args()

    main(args.weeks_short, args.weeks_mid, args.summer_start, args.summer_end, args.outdir,
         print_saves=args.print_saves, to_csv=args.csv)
