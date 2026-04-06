
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
from modules.analytics.immigration import peak_immigration_day

# config
from modules.config import SUMMER_START, SUMMER_END, SECURITY_CAX

# domain (logic)

from modules.domain.tactical import (
    daily_summary,
    arrivals_per_hour,
    arrivals_per_slots,                 
    security_rolling_hour,
    peak_security_hour,
    immigration_queue_slots,            
    immigration_queue_slots_all_days,   
    immigration_overflow_windows,
    security_peak_utilisation,
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
    """
    Create a rolling forward window starting tomorrow
    """
    start = datetime.today() + timedelta(days=1)
    end = start + timedelta(days=weeks * 7)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

#----------------------Loaders - Flights and Security-----------------
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

#--------------------Utility helpers -----------------------
def build_plot_dir(base_outdir: Optional[str], start: str, end: str) -> Optional[Path]:
    """
    Return a directory path for saving plots.
    """
    if not base_outdir:
        return None
    d = Path(base_outdir) / "tactical" / "plots" / f"{start}_to_{end}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def sp(dirpath: Optional[Path], filename: str) -> Optional[str]:
    """
    Return fully-qualified save path or None
    """
    return str(dirpath / filename) if dirpath else None


def export_csv(df: pd.DataFrame, dirpath: Optional[Path], filename: str, print_saves: bool = False):
    """
    Export a CSV file into the window's output folder
    """
    if dirpath is None:
        return
    out = dirpath / filename
    df.to_csv(out, index=False)
    if print_saves:
        print(f"    Saved: {out}")


def format_overflow_table(tbl: pd.DataFrame) -> pd.DataFrame:
    """Format overflow windows for slide-friendly PNG rendering."""
    if tbl.empty:
        return tbl
    t = tbl.copy().sort_values(["Date", "Start"])
    t["Date"]  = pd.to_datetime(t["Date"]).dt.strftime("%d %b")
    t["Start"] = pd.to_datetime(t["Start"]).dt.strftime("%d %b %H:%M")
    t["End"]   = pd.to_datetime(t["End"]).dt.strftime("%d %b %H:%M")
    t["Duration (min)"] = t["Duration_Minutes"].astype(int)
    t["Max DownHall"]   = pd.to_numeric(t["Max_DownHall"], errors="coerce").round(0).astype("Int64")
    return t[["Date", "Start", "End", "Duration (min)", "Max DownHall"]]


# ----------------------- core routine per window -----------------------
def run_window(
    spec: WindowSpec,
    thresholds: Dict[str, float],
    outdir: Optional[str],
    print_saves: bool, 
    to_csv: bool,
    imm_slot: int
) -> Dict[str, Any]:
    """
    Run the full pipeline for one window and return metrics for summary.
        1. Flights and Security
        2. Daily Summary (A/B/C)
        3. Hourly Arrivals
        4. Security rolling hour
        5. Immigration - slot-based queue and overflow windows
        6. Plots and CSVs
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

    
    # ------------------------------
    # Immigration (slot-sized)
    # Dynamic timestamp column:
    #   time_col = "Time_5", "Time_10", "Time_15", etc.
    # ------------------------------

    # Build slot-sized arrivals grid (e.g., 5-min)
    slots = arrivals_per_slots(flights, slot_minutes=imm_slot)
    time_col = f"Time_{imm_slot}"

    # Peak International day
    pk_intl_day = peak_immigration_day(flights)      

    # Peak-day queue for plotting
    imm_peak = (
        immigration_queue_slots(slots, pk_intl_day, slot_minutes=imm_slot)
        if pk_intl_day is not None else pd.DataFrame()
    )

    # All-days queue for overflow windows
    imm_all = (
        immigration_queue_slots_all_days(slots, slot_minutes=imm_slot)
        if not slots.empty else pd.DataFrame()
    )

    #Overflow windows - slot-aware (uses dynamic time_col)
    ov = (
        immigration_overflow_windows(imm_all, time_col=time_col, criterion="queue_gt_capacity")
        if not imm_all.empty
        else pd.DataFrame()
    )

    # Plots
    pdir = build_plot_dir(outdir, spec.start, spec.end)
    ps = bool(outdir) and bool(print_saves)

    #Daily A/D stacked bar
    if spec.include_daily:
        plot_daily_pax_summary(summary, title=f"Daily Arrivals / Departures {spec.label}", save_path=sp(pdir, "daily.png"))
        if ps: print(f"    Saved: {sp(pdir,'daily.png')}")
    plot_weekly_abc(summary, a_threshold=a_th, b_threshold=b_th, window_label=spec.label, save_path=sp(pdir, "weekly_abc.png"))
    if ps: print(f"    Saved: {sp(pdir,'weekly_abc.png')}")
    plot_peak_security(security_rh, pk_sec, capacity_line=SECURITY_CAX, title_prefix=f"Peak Security {spec.label}", save_path=sp(pdir, "peak_security.png"))
    if ps: print(f"    Saved: {sp(pdir,'peak_security.png')}")
    if not imm_peak.empty:
        plot_peak_international_immigration(imm_peak, pk_intl_day, time_col=time_col, window_label=spec.label, save_path=sp(pdir, "immigration.png"))
        if ps: print(f"    Saved: {sp(pdir,'immigration.png')}")

    # Overflow PNG
    if not ov.empty and outdir:
        render_table_png(
            format_overflow_table(ov),
            title=f"Immigration Overflow Windows {spec.label}",
            save_path=sp(pdir, "immigration_overflow_windows.png"),
            max_rows=90,
            col_widths=[0.8, 1.2, 1.2, 0.9, 0.9],
        )
        if ps: print(f"    Saved: {sp(pdir,'immigration_overflow_windows.png')}")

    if to_csv:
        export_csv(summary, pdir, "daily_summary.csv", print_saves=ps)
        export_csv(security_rh, pdir, "security_rolling.csv", print_saves=ps)
        if not imm_peak.empty:
            export_csv(imm_peak, pdir, "immigration_peakday.csv", print_saves=ps)
        if not ov.empty:
            export_csv(ov, pdir, "immigration_overflow_windows.csv", print_saves=ps)

    # Metrics for legacy-style summary
    daily_tot = hourly.groupby("Date")[sector_cols].sum().sum(axis=1) if sector_cols else pd.Series(dtype=float)
    pk_arrival_day = daily_tot.idxmax() if not daily_tot.empty else None
    pk_arrival_val = int(daily_tot.loc[pk_arrival_day]) if pk_arrival_day is not None else 0

    return {
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



# ----------------------- main -----------------------
def main(
    windows_weeks: List[int],
    include_summer: bool,
    summer_start: str,
    summer_end: str,
    outdir: Optional[str],
    print_saves: bool,
    to_csv: bool,
    imm_slot: int,
    a_threshold: Optional[float] = None,
    b_threshold: Optional[float] = None,
) -> None:

    print("\nTACTICAL READINESS — Orchestration")
    if windows_weeks:
        print(f"Forward windows (weeks): {', '.join(str(w) for w in windows_weeks)}")
    if include_summer:
        print(f"Summer: {summer_start} → {summer_end}")
    print(f"Immigration slot size: {imm_slot} minutes")
    if outdir:
        print(f"Output dir: {Path(outdir).resolve()}")
    print()

    t0 = step(0.0, "Start")

    
    # -----------------------------------------------------------
    # 1/4 — SUMMER BASELINE THRESHOLDS
    # -----------------------------------------------------------

    print("[1/4] Baseline thresholds from Summer…")
    fl_su = load_flights(summer_start, summer_end)
    sec_su = load_security(summer_start, summer_end)
    t1 = step(t0, f"Summer flights/security: {len(fl_su):,} / {len(sec_su):,}")

    if a_threshold is not None and b_threshold is not None:
        su_summary, su_A, su_B = daily_summary(fl_su, a_threshold, b_threshold)
    else:
        su_summary, su_A, su_B = daily_summary(fl_su)
    thresholds = {"A": su_A, "B": su_B}
    t2 = step(t1, f"Thresholds derived: A={int(su_A):,}, B={int(su_B):,}")

    
    # -----------------------------------------------------------
    # 2/4 — WINDOW SPECS
    # -----------------------------------------------------------

    specs: List[WindowSpec] = []
    for w in windows_weeks:
        s, e = forward_window(w)
        specs.append(WindowSpec(name=f"{w}w", start=s, end=e, label=f"({w}-week)", include_daily=True))
    if include_summer:
        specs.append(WindowSpec(name="summer", start=summer_start, end=summer_end, label="(Summer)", include_daily=False))

    
    # -----------------------------------------------------------
    # 3/4 — RUN WINDOWS
    # ----------------------------------------------------------

    print("[2/4] Running windows…")
    results: Dict[str, Dict[str, Any]] = {}
    for i, spec in enumerate(specs, start=1):
        print(f"    · [{i}/{len(specs)}] {spec.name} {spec.start} → {spec.end}")
        results[spec.name] = run_window(spec=spec, thresholds=thresholds, outdir=outdir, print_saves=print_saves, to_csv=to_csv, imm_slot=imm_slot)
    t3 = step(t2, "Windows complete")

    
    # -----------------------------------------------------------
    # Security utilisation summary
    # -----------------------------------------------------------

    print("\n--- PEAK SECURITY UTILISATION ---")
    for spec in specs:
        u = results[spec.name]["utilisation"]
        pk = results[spec.name]["pk_security"]
        print(f"{spec.label[1:-1]:<6}: {u:0.1f}% of capacity (Total RH {pk['Total RH']:,} vs {SECURITY_CAX:,})")


    # -----------------------------------------------------------
    # Overflow windows summary
    # -----------------------------------------------------------
    
    print("\n--- IMMIGRATION OVERFLOW WINDOWS ---")

    def _days_with_overflow(ov_df: pd.DataFrame) -> int:
        return 0 if ov_df.empty else ov_df["Date"].nunique()

    for spec in specs:
        ov = results[spec.name]["overflow_windows"]

        days = _days_with_overflow(ov)
        print(f"{spec.label:<10} total days where queue moves into overflow: {days}")

        # Breach duration stats
        avg_breach_minutes = ov["Duration_Minutes"].mean() if not ov.empty else 0
        median_breach_minutes = ov["Duration_Minutes"].median() if not ov.empty else 0

        print(f"  • Avg Breach Duration: {avg_breach_minutes:.1f} mins")
        print(f"  • Median Breach Duration: {median_breach_minutes:.1f} mins")

    t4 = step(t3, "Window stats printed")


    
# -----------------------------------------------------------
    # 4/4 — CONSOLIDATED SUMMARY (NEW CLEAN VERSION)
    # -----------------------------------------------------------
    print("\n================ SUMMARY BY WINDOW ================\n")

    for spec in specs:
        r = results[spec.name]
        print(f"\n##################  {spec.label} WINDOW  ##################")

        # ---------------------------
        # A/B/C counts
        # ---------------------------
        summary_df = r["summary_df"]
        abc = summary_df["Ranking"].value_counts().reindex(["A","B","C"], fill_value=0)

        print("\nA/B/C Day Count:")
        print(f"  • A Days: {abc['A']}")
        print(f"  • B Days: {abc['B']}")
        print(f"  • C Days: {abc['C']}")

        # ---------------------------
        # Peak Security
        # ---------------------------
        pkS = r["pk_security"]
        print("\nPeak Security Hour:")
        print(f"  • Window: {pkS['Window Start']}–{pkS['Window End']}")
        print(f"  • Date:   {pkS['Date']}")
        print(f"  • Pax RH: {pkS['Pax RH']:,}")
        print(f"  • Total RH: {pkS['Total RH']:,}")

        # ---------------------------
        # Build daily totals for scheduled arrivals
        # ---------------------------
        hourly_df = r["hourly_df"]
        sector_cols = [c for c in hourly_df.columns
                       if c not in ["Date", "Hour", "Hour_Label"]]
        daily_total = hourly_df.groupby("Date")[sector_cols].sum().sum(axis=1)
        daily_intl  = hourly_df.groupby("Date")["International"].sum()

        # Build immigration-arrival totals
        f = load_flights(spec.start, spec.end)
        f = f[f["A/D"] == "A"]
        f["Imm_Arrival"] = f["Schedule"] + timedelta(minutes=20)
        f_intl = f[f["Sector"] == "International"]
        imm_intl_by_day = (
            f_intl.groupby(f_intl["Imm_Arrival"].dt.date)["Pax"].sum()
        )

        # ---------------------------
        # Peak Total Arrivals Day
        # ---------------------------
        if not daily_total.empty:
            pk_total = daily_total.idxmax()
            sched_total = int(daily_total.loc[pk_total])
            sched_intl  = int(daily_intl.get(pk_total, 0))
            imm_intl    = int(imm_intl_by_day.get(pk_total, 0))

            print("\nPeak Total Arrivals Day:")
            print(f"  • Day: {pk_total}")
            print(f"  • Total Scheduled Arrivals: {sched_total:,}")
            print(f"  • Scheduled International: {sched_intl:,}")
            print(f"  • Immigration-arrival International: {imm_intl:,}")

        # ---------------------------
        # Peak Scheduled-International Arrivals Day
        # ---------------------------
        if not daily_intl.empty:
            pk_sched_intl = daily_intl.idxmax()
            sched_intl2 = int(daily_intl.loc[pk_sched_intl])
            total_sched2 = int(daily_total.loc[pk_sched_intl])
            imm_intl2 = int(imm_intl_by_day.get(pk_sched_intl, 0))

            print("\nPeak International Arrivals Day (Scheduled):")
            print(f"  • Day: {pk_sched_intl}")
            print(f"  • Scheduled International: {sched_intl2:,}")
            print(f"  • Total Scheduled Arrivals: {total_sched2:,}")
            print(f"  • Immigration-arrival International: {imm_intl2:,}")

        # ---------------------------
        # Peak Immigration Arrivals Day (Schedule+20)
        # ---------------------------
        pk_imm = r["intl_peak_day"]
        if pk_imm is not None:
            pk_imm = pd.to_datetime(pk_imm).date()
            sched_intl3 = int(daily_intl.get(pk_imm, 0))
            total_sched3 = int(daily_total.get(pk_imm, 0))
            imm_intl3 = int(imm_intl_by_day.get(pk_imm, 0))

            print("\nPeak Immigration Arrivals Day (Schedule+20):")
            print(f"  • Day: {pk_imm}")
            print(f"  • Scheduled International: {sched_intl3:,}")
            print(f"  • Immigration-arrival International: {imm_intl3:,}")
            print(f"  • Total Scheduled Arrivals: {total_sched3:,}")

    step(t4, "Summary printed")



if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Tactical Readiness — Multi-window with slot-based immigration.")
    ap.add_argument("--weeks", default="2,4", help="Comma-separated forward windows in weeks, e.g., '1,2' or '2,4'.")
    ap.add_argument("--no-summer", action="store_true", help="Exclude Summer window.")
    ap.add_argument("--summer-start", default=SUMMER_START, help="Summer start (YYYY-MM-DD).")
    ap.add_argument("--summer-end",   default=SUMMER_END,   help="Summer end (YYYY-MM-DD).")
    ap.add_argument("--imm-slot", type=int, default=15, help="Immigration slot size in minutes (5, 10, 15…).")
    ap.add_argument("--outdir", default=None, help="Base output directory for plots.")
    ap.add_argument("--print-saves", action="store_true", help="Print a 'Saved:' line for every saved figure/table.")
    ap.add_argument("--csv", action="store_true", help="Export CSVs for Daily/Security/Immigration outputs.")
    ap.add_argument("--a-threshold", type=float, default=None, help="Custom threshold for A days (overrides Summer-derived). If provided, B-threshold must also be provided.")
    ap.add_argument("--b-threshold", type=float, default=None, help="Custom threshold for B days (overrides Summer-derived). If provided, A-threshold must also be provided.")

    args = ap.parse_args()

    
    if (args.a_threshold is None) ^ (args.b_threshold is None):
        ap.error("You must specify both --a-threshold and --b-threshold together.")


    weeks = [int(w.strip()) for w in args.weeks.split(",") if w.strip().isdigit()]
    main(
        windows_weeks=weeks,
        include_summer=not args.no_summer,
        summer_start=args.summer_start,
        summer_end=args.summer_end,
        outdir=args.outdir,
        print_saves=args.print_saves,
        to_csv=args.csv,
        imm_slot=args.imm_slot,
        a_threshold=args.a_threshold,
        b_threshold=args.b_threshold,
    )
