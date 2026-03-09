
# scripts/tactical_readiness.py
from __future__ import annotations

import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd

# utils
from modules.utils.query import query
from modules.utils.dates import to_datetime
from modules.utils.progress import step

# config
from modules.config import SUMMER_START, SUMMER_END, SECURITY_CAX

# domain (logic)
from modules.domain.tactical import (
    daily_summary,
    arrivals_per_hour,
    arrivals_per_15min,
    security_rolling_hour,
    peak_security_hour,
    immigration_queue_15m,
    immigration_queue_15m_all_days,
    immigration_overflow_windows,
    security_peak_utilisation,  # 🇬🇧 spelling
)

# viz (plots + table-render)
from modules.viz.tactical import (
    plot_daily_pax_summary,
    plot_weekly_abc,
    plot_peak_security,
    plot_peak_international_immigration,
    render_table_png,
)


# ----------------------- window spec & helpers -----------------------
@dataclass
class WindowSpec:
    name: str            # "2w", "4w", "summer", etc.
    start: str           # "YYYY-MM-DD"
    end: str             # "YYYY-MM-DD" (exclusive)
    label: str           # "(2-week)", "(4-week)", "(Summer)"
    include_daily: bool  # whether to draw daily A/D chart for this window


def forward_window(weeks: int) -> tuple[str, str]:
    start = datetime.today() + timedelta(days=1)
    end = start + timedelta(days=weeks * 7)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


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


def build_plot_dir(base_outdir: Optional[str], start: str, end: str) -> Optional[Path]:
    if not base_outdir:
        return None
    d = Path(base_outdir) / "tactical" / "plots" / f"{start}_to_{end}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def sp(dirpath: Optional[Path], filename: str) -> Optional[str]:
    return str(dirpath / filename) if dirpath else None


def export_csv(df: pd.DataFrame, dirpath: Optional[Path], filename: str, print_saves: bool = False):
    if dirpath is None:
        return
    out = dirpath / filename
    df.to_csv(out, index=False)
    if print_saves:
        print(f"    Saved: {out}")


def format_overflow_table(tbl: pd.DataFrame) -> pd.DataFrame:
    """Format overflow windows for table/CSV."""
    if tbl.empty:
        return tbl
    t = tbl.copy().sort_values(["Date", "Start"])
    t["Date"]  = pd.to_datetime(t["Date"]).dt.strftime("%d %b")
    t["Start"] = pd.to_datetime(t["Start"]).dt.strftime("%d %b %H:%M")
    t["End"]   = pd.to_datetime(t["End"]).dt.strftime("%d %b %H:%M")
    t["Duration (min)"] = t["Duration_Minutes"].astype(int)
    t["Max Overflow"]   = pd.to_numeric(t["Max_Overflow"], errors="coerce").round(0).astype("Int64")
    return t[["Date", "Start", "End", "Duration (min)", "Max Overflow"]]


# ----------------------- core routine per window -----------------------
def run_window(
    spec: WindowSpec,
    thresholds: Dict[str, float],
    outdir: Optional[str],
    print_saves: bool,
    to_csv: bool,
) -> Dict[str, Any]:
    """
    Run the full pipeline for one window and return metrics for summary.
    """
    # Load
    flights = load_flights(spec.start, spec.end)
    security = load_security(spec.start, spec.end)

    # Daily summary (use Summer thresholds for consistency across windows)
    summary, a_th, b_th = daily_summary(flights, a_threshold=thresholds["A"], b_threshold=thresholds["B"])

    # Hourly arrivals (sectors)
    hourly = arrivals_per_hour(flights)
    sector_cols = [c for c in hourly.columns if c not in ["Date", "Hour", "Hour_Label"]]

    # Security: rolling-hour + peaks + utilisation
    security_rh = security_rolling_hour(security)
    pk_sec = peak_security_hour(security_rh)
    utilisation = security_peak_utilisation(pk_sec, SECURITY_CAX)

    # Immigration: 15-min arrivals → peak-day queue (chart) and all-days queue (overflow table)
    pk_intl_day = hourly.groupby("Date")["International"].sum().idxmax() if "International" in hourly.columns and not hourly.empty else None
    arr15 = arrivals_per_15min(flights) if pk_intl_day is not None else pd.DataFrame()
    imm_peak = immigration_queue_15m(arr15, pk_intl_day) if pk_intl_day is not None else pd.DataFrame()

    imm_all = immigration_queue_15m_all_days(arr15) if not arr15.empty else pd.DataFrame()
    # Use graph-aligned breach definition: Overflow > Capacity
    ov = immigration_overflow_windows(imm_all, criterion="queue_gt_capacity") if not imm_all.empty else pd.DataFrame()

    # Output dirs
    pdir = build_plot_dir(outdir, spec.start, spec.end)
    ps = bool(outdir) and bool(print_saves)

    # Plots (only the ones you want now)
    if spec.include_daily:
        plot_daily_pax_summary(summary, title=f"Daily Arrivals / Departures {spec.label}", save_path=sp(pdir, "daily.png"))
        if ps: print(f"    Saved: {sp(pdir,'daily.png')}")
    plot_weekly_abc(summary, a_threshold=a_th, b_threshold=b_th, window_label=spec.label, save_path=sp(pdir, "weekly_abc.png"))
    if ps: print(f"    Saved: {sp(pdir,'weekly_abc.png')}")
    plot_peak_security(security_rh, pk_sec, capacity_line=SECURITY_CAX, title_prefix=f"Peak Security {spec.label}", save_path=sp(pdir, "peak_security.png"))
    if ps: print(f"    Saved: {sp(pdir,'peak_security.png')}")
    if not imm_peak.empty:
        plot_peak_international_immigration(imm_peak, pk_intl_day, window_label=spec.label, save_path=sp(pdir, "immigration.png"))
        if ps: print(f"    Saved: {sp(pdir,'immigration.png')}")

    # Overflow table PNG (slide-friendly) + CSV
    if not ov.empty and outdir:
        render_table_png(
            format_overflow_table(ov),
            title=f"Immigration Overflow Windows {spec.label}",
            save_path=sp(pdir, "immigration_overflow_windows.png"),
            max_rows=30,
            col_widths=[0.8, 1.2, 1.2, 0.9, 0.9],
        )
        if ps: print(f"    Saved: {sp(pdir,'immigration_overflow_windows.png')}")

    if to_csv:
        export_csv(summary, pdir, "daily_summary.csv", print_saves=ps)
        export_csv(security_rh, pdir, "security_rolling.csv", print_saves=ps)
        if not imm_peak.empty:
            export_csv(imm_peak, pdir, "immigration_peakday_15min.csv", print_saves=ps)
        if not ov.empty:
            export_csv(ov, pdir, "immigration_overflow_windows.csv", print_saves=ps)

    # Metrics for legacy-style summary
    daily_tot = hourly.groupby("Date")[sector_cols].sum().sum(axis=1) if sector_cols else pd.Series(dtype=float)
    pk_arrival_day = daily_tot.idxmax() if not daily_tot.empty else None
    pk_arrival_val = int(daily_tot.loc[pk_arrival_day]) if pk_arrival_day is not None else 0

    metrics = {
        "summary_df": summary,
        "hourly_df": hourly,
        "security_rh": security_rh,
        "pk_security": pk_sec,
        "utilisation": utilisation,
        "peak_arrival_day": pk_arrival_day,
        "peak_arrival_val": pk_arrival_val,
        "intl_peak_day": pk_intl_day,
        "overflow_windows": ov,
    }
    return metrics


# ----------------------- main -----------------------
def main(
    windows_weeks: List[int],
    include_summer: bool,
    summer_start: str,
    summer_end: str,
    outdir: Optional[str],
    print_saves: bool,
    to_csv: bool,
) -> None:

    print("\nTACTICAL READINESS — Orchestration")
    if windows_weeks:
        print(f"Forward windows (weeks): {', '.join(str(w) for w in windows_weeks)}")
    if include_summer:
        print(f"Summer: {summer_start} → {summer_end}")
    if outdir:
        print(f"Output dir: {Path(outdir).resolve()}")
    print()

    t0 = step(0.0, "Start")

    # --- Load Summer once to derive thresholds (A/B/C) ---
    print("[1/4] Baseline thresholds from Summer…")
    fl_su = load_flights(summer_start, summer_end)
    sec_su = load_security(summer_start, summer_end)
    t1 = step(t0, f"Summer flights/security: {len(fl_su):,} / {len(sec_su):,}")

    su_summary, su_A, su_B = daily_summary(fl_su)
    thresholds = {"A": su_A, "B": su_B}
    t2 = step(t1, f"Thresholds derived: A={int(su_A):,}, B={int(su_B):,}")

    # --- Build window specs ---
    specs: List[WindowSpec] = []
    for w in windows_weeks:
        s, e = forward_window(w)
        specs.append(WindowSpec(name=f"{w}w", start=s, end=e, label=f"({w}-week)", include_daily=True))
    if include_summer:
        specs.append(WindowSpec(name="summer", start=summer_start, end=summer_end, label="(Summer)", include_daily=False))

    # --- Run windows uniformly ---
    print("[2/4] Running windows…")
    results: Dict[str, Dict[str, Any]] = {}
    for i, spec in enumerate(specs, start=1):
        print(f"    · [{i}/{len(specs)}] {spec.name} {spec.start} → {spec.end}")
        res = run_window(spec, thresholds, outdir, print_saves, to_csv)
        results[spec.name] = res
    t3 = step(t2, "Windows complete")

    # --- Per-window prints (utilisation + overflow days) ---
    print("\n--- PEAK SECURITY UTILISATION ---")
    for spec in specs:
        u = results[spec.name]["utilisation"]
        pk = results[spec.name]["pk_security"]
        print(f"{spec.label[1:-1]:<6}: {u:0.1f}% of capacity (Total RH {pk['Total RH']:,} vs {SECURITY_CAX:,})")

    def _days_with_overflow(ov_df: pd.DataFrame) -> int:
        return 0 if ov_df.empty else ov_df["Date"].nunique()

    print("\n--- IMMIGRATION OVERFLOW WINDOWS (compressed) ---")
    for spec in specs:
        days = _days_with_overflow(results[spec.name]["overflow_windows"])
        print(f"{spec.label:<10} total days where queue moves into overflow: {days}")

    t4 = step(t3, "Window stats printed")

    # --- Legacy-style SUMMARY (2w, 4w, Summer) ---
    print("\n--- SUMMARY ---")
    spec_by_name = {s.name: s for s in specs}

    # 2-week
    if "2w" in results:
        r2 = results["2w"]
        print(f"2-week Peak Arrival Day: {r2['peak_arrival_day']} ({r2['peak_arrival_val']:,} pax)")
        pk2 = r2["pk_security"]
        print(f"2-week Peak Security Hour: {pk2['Window Start']}-{pk2['Window End']} on {pk2['Date']} ({pk2['Total RH']} total)")

    # 4-week (or the first non-summer forward window if weeks_weeks contains other numbers)
    fwd_alt = None
    if "4w" in results:
        fwd_alt = "4w"
    elif windows_weeks:
        # pick the *second* if present, else the first
        names = [f"{w}w" for w in windows_weeks]
        fwd_alt = names[1] if len(names) > 1 and names[1] in results else names[0] if names and names[0] in results else None

    if fwd_alt:
        r4 = results[fwd_alt]
        print(f"{spec_by_name[fwd_alt].label} Peak Arrival Day: {r4['peak_arrival_day']} ({r4['peak_arrival_val']:,} pax)")
        pk4 = r4["pk_security"]
        print(f"{spec_by_name[fwd_alt].label} Peak Security Hour: {pk4['Window Start']}-{pk4['Window End']} on {pk4['Date']} ({pk4['Total RH']} total)")

    # Summer season detail
    if "summer" in results:
        rs = results["summer"]
        print("\n--- SUMMER SEASON ---")
        print(f"Period: {summer_start} to {summer_end}")

        # A/B/C counts
        abc_counts = rs["summary_df"]["Ranking"].value_counts().reindex(["A", "B", "C"], fill_value=0)
        print("\nA/B/C Days:")
        print(f"  A Days: {abc_counts['A']}")
        print(f"  B Days: {abc_counts['B']}")
        print(f"  C Days: {abc_counts['C']}")

        print(f"\nPeak Arrival Day: {rs['peak_arrival_day']} ({rs['peak_arrival_val']:,} pax)")

        pkS = rs["pk_security"]
        print("\n--- SUMMER PEAK SECURITY HOUR ---")
        print(f"Window: {pkS['Window Start']} - {pkS['Window End']}")
        print(f"Date: {pkS['Date']}")
        print(f"Passengers (RH): {pkS['Pax RH']}")
        print(f"Staff (RH): {pkS['Staff RH']}")
        print(f"Total (RH): {pkS['Total RH']}")

        # Peak Total vs Peak International (Summer)
        hourly_su = results["summer"]["hourly_df"]
        sectors_su = [c for c in hourly_su.columns if c not in ["Date", "Hour", "Hour_Label"]]
        daily_total_arrivals = hourly_su.groupby("Date")[sectors_su].sum().sum(axis=1) if sectors_su else pd.Series(dtype=float)
        daily_intl_arrivals  = hourly_su.groupby("Date")["International"].sum() if "International" in hourly_su.columns else pd.Series(dtype=float)

        if not daily_total_arrivals.empty:
            peak_total_day = daily_total_arrivals.idxmax()
            total_pax_on_peak_total_day = int(daily_total_arrivals.loc[peak_total_day])
            intl_pax_on_peak_total_day  = int(daily_intl_arrivals.loc[peak_total_day]) if peak_total_day in daily_intl_arrivals.index else 0
            print(f"\nPeak Total Arrivals Day: {peak_total_day} -> Total: {total_pax_on_peak_total_day:,}, International: {intl_pax_on_peak_total_day:,}")

        if not daily_intl_arrivals.empty:
            peak_intl_day = daily_intl_arrivals.idxmax()
            total_pax_on_peak_intl_day = int(daily_total_arrivals.loc[peak_intl_day]) if peak_intl_day in daily_total_arrivals.index else 0
            intl_pax_on_peak_intl_day  = int(daily_intl_arrivals.loc[peak_intl_day])
            print(f"Peak International Arrivals Day: {peak_intl_day} -> Total: {total_pax_on_peak_intl_day:,}, International: {intl_pax_on_peak_intl_day:,}")

    step(t4, "Summary printed")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Tactical Readiness — DRY orchestration for multiple windows.")
    ap.add_argument("--weeks", default="2,4", help="Comma-separated forward windows in weeks, e.g., '1,2' or '2,4'.")
    ap.add_argument("--no-summer", action="store_true", help="Exclude Summer window.")
    ap.add_argument("--summer-start", default=SUMMER_START, help="Summer start (YYYY-MM-DD).")
    ap.add_argument("--summer-end",   default=SUMMER_END,   help="Summer end (YYYY-MM-DD).")
    ap.add_argument("--outdir", default=None, help="Base output directory for plots.")
    ap.add_argument("--print-saves", action="store_true", help="Print a 'Saved:' line for every saved figure/table.")
    ap.add_argument("--csv", action="store_true", help="Export CSVs for Daily/Security/Immigration outputs.")
    args = ap.parse_args()

    weeks = [int(w.strip()) for w in args.weeks.split(",") if w.strip().isdigit()]
    main(
        windows_weeks=weeks,
        include_summer=not args.no_summer,
        summer_start=args.summer_start,
        summer_end=args.summer_end,
        outdir=args.outdir,
        print_saves=args.print_saves,
        to_csv=args.csv,
    )
