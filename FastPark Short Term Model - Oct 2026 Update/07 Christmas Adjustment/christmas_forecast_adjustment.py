"""
FastPark Christmas forecast adjustment
======================================

Post-processes the daily output of the production forecast
(05 Production Forecast/simplified_fastpark_shortterm.py) for the Christmas /
New Year window, without changing that script.

Why
---
The production forecast blends four components. Three of them (same weekday,
same weekday-and-month, trend x month) are holiday-blind, and the booking
curve's completion factor comes from the most recent completed days (ordinary
days). At 30-50 days out ~70% of the weight sits on the holiday-blind parts, so
quiet days such as Christmas Day come out far too high, and peaks such as
Boxing Day entries or the 2-3 Jan return exits come out too low.

What it does
------------
For each day in the window (Friday before Christmas to Sunday after New Year):

1. Matched days: each prior year's like-for-like day (major days on date,
   other days on the same weekday within 3 days, in the same part of the
   period, never onto a major day; otherwise the same calendar date).
2. Bookings-based estimate:
       bookings known now (from the forecast output)
       x average over prior years of (actual / bookings known at the same
         days-out on the matched day)
3. History-based estimate:
       this year's recent 4-week daily average
       x average over prior years of (actual on matched day / that year's
         4-week average over the same dates)
4. Christmas forecast = w x bookings-based + (1 - w) x history-based, where w
   is the production model's own learned booking weight for that flow and
   horizon (forecast_weights.csv), unless overridden below.
5. Final = CHRISTMAS_WEIGHT x Christmas forecast + (1 - CHRISTMAS_WEIGHT) x
   model forecast, for days in scope. Days outside the window are unchanged.

History is built with the production script's own functions, so entry/exit
definitions (CheckInEnded / ActualCheckedOutDate), the booking-to-operations
reconciliation, cancellation-aware booking curves and the 07:00 cutoff all
match the model. Prior years whose actual movements are incomplete are
dropped automatically (actuals < MIN_ACTUAL_COVERAGE of bookings).

Outputs (07 Christmas Adjustment/outputs/)
------------------------------------------
- christmas_daily_forecast_<run date>.csv : the forecast output with
  entry_forecast / exit_forecast replaced for the adjusted days, plus
  model_* and xmas_* columns explaining each change.
- christmas_daily_forecast_<run date>.xlsx: readable summary + chart.

Input
-----
The dated daily forecast the production run saves each day:
    01 Short-Term Models/Outputs/Daily Forecast Output/<YYYY-MM-DD>_daily_forecast.csv
By default today's file is used. If it doesn't exist (the daily run hasn't
finished) the script stops - it never falls back to an older forecast.

Run (after the daily production run)
------------------------------------
    python "07 Christmas Adjustment/christmas_forecast_adjustment.py"
    python "07 Christmas Adjustment/christmas_forecast_adjustment.py" --date 2026-11-16
    python "07 Christmas Adjustment/christmas_forecast_adjustment.py" --forecast path/to/file.csv
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from openpyxl import Workbook  # noqa: E402
from openpyxl.drawing.image import Image as XLImage  # noqa: E402
from openpyxl.formatting.rule import ColorScaleRule  # noqa: E402
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side  # noqa: E402
from openpyxl.utils import get_column_letter  # noqa: E402

HERE = Path(__file__).resolve().parent


# =====================================================================
# CONFIG - edit here
# =====================================================================
# Folder holding simplified_fastpark_shortterm.py. None = look for
# "05 Production Forecast" next to / above this script. Can also be set with
# the FASTPARK_PRODUCTION_DIR environment variable or --production-dir.
PRODUCTION_DIR = None

# Folder holding the dated daily forecasts the production run writes each day
# ("<YYYY-MM-DD>_daily_forecast.csv"). None = the production script's own
# setting: <output_base_path>/Daily Forecast Output, which is
# "01 Short-Term Models/Outputs/Daily Forecast Output".
DAILY_FORECAST_DIR = None

OUTPUT_DIR = HERE / "outputs"

# Earliest Christmas to use as history (Christmas year). Years with
# incomplete movement data are dropped automatically.
FIRST_SEASON = 2022
EXCLUDE_SEASONS = {2020, 2021}
MIN_ACTUAL_COVERAGE = 0.7

# Which days to adjust: "window" = every day Fri-before-Christmas to
# Sun-after-New-Year; "major" = only 24, 25, 26, 31 Dec, 1 and 2 Jan.
ADJUST_DAYS = "window"

# 1.0 = use the Christmas forecast outright on adjusted days.
# 0.5 = halfway between the model and the Christmas forecast.
CHRISTMAS_WEIGHT = 1.0

# None = use the model's learned booking weight per horizon.
# A number (0-1) = fixed weight on the bookings-based estimate.
BOOKING_WEIGHT_OVERRIDE = None

FLOWS = {
    # flow: (forecast col, bookings col, target field, actual field, daily col)
    "entry": ("entry_forecast", "entry_bookings", "entryDate",
              "actual_entry_ts", "entries"),
    "exit": ("exit_forecast", "exit_bookings", "exitDate",
             "actual_exit_ts", "exits"),
}


# =====================================================================
# Calendar helpers
# =====================================================================
MAJOR_DAYS = {
    (12, 24): "Christmas Eve",
    (12, 25): "Christmas Day",
    (12, 26): "Boxing Day",
    (12, 31): "Hogmanay",
    (1, 1): "New Year's Day",
    (1, 2): "2 Jan bank holiday",
}

SHORT_NAMES = {
    (12, 24): "Xmas Eve", (12, 25): "Xmas Day", (12, 26): "Boxing Day",
    (12, 31): "Hogmanay", (1, 1): "NY Day", (1, 2): "Bank hol",
}


def season_label(season):
    return f"{season}/{str(season + 1)[-2:]}"


def season_of(ts):
    """Christmas year a date belongs to (Jul-Dec = that year, Jan-Jun = prior)."""
    return ts.year if ts.month >= 7 else ts.year - 1


def christmas_window(season):
    """Friday before Christmas to the Sunday after New Year's Day."""
    xmas = pd.Timestamp(season, 12, 25)
    days_back = (xmas.weekday() - 4) % 7 or 7
    ny = pd.Timestamp(season + 1, 1, 1)
    days_fwd = (6 - ny.weekday()) % 7 or 7
    return pd.date_range(
        xmas - pd.Timedelta(days=days_back),
        ny + pd.Timedelta(days=days_fwd),
        freq="D",
    )


def holiday_name(ts):
    return MAJOR_DAYS.get((ts.month, ts.day), "")


def is_major(ts):
    return (ts.month, ts.day) in MAJOR_DAYS


def period_phase(ts):
    if is_major(ts):
        return "major"
    if ts.month == 12 and ts.day < 24:
        return "pre-Christmas"
    if ts.month == 12 and 27 <= ts.day <= 30:
        return "between"
    if ts.month == 1 and ts.day >= 3:
        return "post-New Year"
    return "other"


def to_season_date(ts, season):
    """Same calendar date, in another season."""
    year = season if ts.month >= 7 else season + 1
    return pd.Timestamp(year, ts.month, ts.day)


def matched_date(ts, season):
    """
    Like-for-like day in another season. Major days are matched on date;
    other days on the same weekday within 3 days, in the same part of the
    period and never onto a major day; otherwise the same calendar date.
    """
    same = to_season_date(ts, season)
    if is_major(ts):
        return same
    phase = period_phase(ts)
    for k in sorted(range(-3, 4), key=lambda x: (abs(x), x)):
        cand = same + pd.Timedelta(days=k)
        if (
            cand.weekday() == ts.weekday()
            and not is_major(cand)
            and period_phase(cand) == phase
        ):
            return cand
    return same


def day_text(ts, with_year=False, with_holiday=True):
    txt = ts.strftime("%a %d %b %Y" if with_year else "%a %d %b")
    hol = holiday_name(ts)
    if with_holiday and hol:
        txt += f" ({hol})"
    return txt


def safe_ratio(a, b):
    try:
        if b is None or pd.isna(b) or b == 0 or pd.isna(a):
            return np.nan
    except TypeError:
        return np.nan
    return a / b


# =====================================================================
# Chart helpers
# =====================================================================
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
GRID = "#e4e3df"
BAND = "#f0efec"
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]


def new_fig(w, h):
    return plt.figure(figsize=(w, h), facecolor=SURFACE)


def style_axes(ax):
    ax.set_facecolor(SURFACE)
    for side in ["top", "right"]:
        ax.spines[side].set_visible(False)
    for side in ["left", "bottom"]:
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=8, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def shade_holidays(ax, window):
    for i, d in enumerate(window):
        if is_major(d):
            ax.axvspan(i - 0.5, i + 0.5, color=BAND, zorder=0, linewidth=0)


def day_ticks(ax, window):
    labels = [
        f"{d:%a}\n{d:%d %b}" + ("\n" + SHORT_NAMES[(d.month, d.day)]
                                if is_major(d) else "")
        for d in window
    ]
    ax.set_xticks(range(len(window)))
    ax.set_xticklabels(labels, fontsize=7)
    ax.set_xlim(-0.5, len(window) - 0.5)


def finish(fig, path, title, subtitle):
    fig.suptitle(title, x=0.01, ha="left", fontsize=13, color=INK,
                 fontweight="bold", y=0.995)
    fig.text(0.01, 0.955, subtitle, ha="left", fontsize=9, color=INK2)
    fig.savefig(path, dpi=110, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    return path


# =====================================================================
# Excel helpers
# =====================================================================
HEADER_FILL = PatternFill("solid", fgColor="1C5CAB")
GROUP_FILL = PatternFill("solid", fgColor="CDE2FB")
TOTAL_FILL = PatternFill("solid", fgColor="F0EFEC")
HEADER_FONT = Font(bold=True, color="FFFFFF", size=10)
TITLE_FONT = Font(bold=True, size=14, color="0B0B0B")
SUB_FONT = Font(bold=True, size=11, color="1C5CAB")
NOTE_FONT = Font(italic=True, size=9, color="52514E")
BORDER = Border(bottom=Side(style="thin", color="D9D8D3"))

FMT_INT = "#,##0"
FMT_PCT = "0%"
FMT_DIFF = "+0%;-0%;0%"
FMT_DAYS = "0"


def guess_format(col, override):
    for key, fmt in override.items():
        if key in col:
            return fmt
    return FMT_INT


def write_title(ws, row, text, note=None):
    ws.cell(row=row, column=1, value=text).font = TITLE_FONT
    if note:
        ws.cell(row=row + 1, column=1, value=note).font = NOTE_FONT
        return row + 3
    return row + 2


def write_table(ws, df, row, title=None, note=None, formats=None,
                scale=None, scale_cols=None, total_row=False):
    """
    Write df at `row`. Column names with '|' get a group header row
    ('Entries|Bookings now' -> group 'Entries', header 'Bookings now').
    scale='div' colours the columns matching scale_cols red (-) / blue (+).
    """
    formats = formats or {}
    if title:
        ws.cell(row=row, column=1, value=title).font = SUB_FONT
        row += 1
    if note:
        ws.cell(row=row, column=1, value=note).font = NOTE_FONT
        row += 1

    cols = list(df.columns)
    if any("|" in str(c) for c in cols):
        prev, start_c = None, None
        for j, c in enumerate(cols, start=1):
            grp = str(c).split("|")[0] if "|" in str(c) else ""
            if grp != prev:
                if prev:
                    ws.merge_cells(start_row=row, start_column=start_c,
                                   end_row=row, end_column=j - 1)
                start_c, prev = j, grp
            cell = ws.cell(row=row, column=j, value=grp or None)
            if grp:
                cell.fill = GROUP_FILL
                cell.font = Font(bold=True, size=10, color="0B0B0B")
                cell.alignment = Alignment(horizontal="center")
        if prev:
            ws.merge_cells(start_row=row, start_column=start_c,
                           end_row=row, end_column=len(cols))
        row += 1

    header_row = row
    for j, c in enumerate(cols, start=1):
        cell = ws.cell(row=row, column=j, value=str(c).split("|")[-1])
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center",
                                   wrap_text=True)
    ws.row_dimensions[row].height = 32
    row += 1

    first_data = row
    for _, rec in df.iterrows():
        for j, c in enumerate(cols, start=1):
            v = rec[c]
            if isinstance(v, (float, np.floating)) and np.isnan(v):
                v = None
            elif isinstance(v, np.generic):
                v = v.item()
            cell = ws.cell(row=row, column=j, value=v)
            cell.border = BORDER
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                cell.number_format = guess_format(str(c), formats)
                cell.alignment = Alignment(horizontal="right")
        row += 1
    last_data = row - 1

    if total_row and len(df):
        ws.cell(row=row, column=1, value="Total").font = Font(bold=True)
        for j, c in enumerate(cols, start=1):
            ws.cell(row=row, column=j).fill = TOTAL_FILL
            fmt = guess_format(str(c), formats)
            if (j > 1 and pd.api.types.is_numeric_dtype(df[c])
                    and fmt == FMT_INT):
                L = get_column_letter(j)
                cell = ws.cell(row=row, column=j,
                               value=f"=SUM({L}{first_data}:{L}{last_data})")
                cell.number_format = FMT_INT
                cell.font = Font(bold=True)
        row += 1

    if scale == "div" and len(df):
        for j, c in enumerate(cols, start=1):
            if scale_cols and any(k in str(c) for k in scale_cols):
                L = get_column_letter(j)
                ws.conditional_formatting.add(
                    f"{L}{first_data}:{L}{last_data}",
                    ColorScaleRule(
                        start_type="num", start_value=-0.5,
                        start_color="E34948",
                        mid_type="num", mid_value=0, mid_color="F0EFEC",
                        end_type="num", end_value=0.5, end_color="2A78D6"))

    return row + 2, header_row


def autosize(ws, min_w=8, max_w=34):
    widths = {}
    for r in ws.iter_rows():
        for cell in r:
            if cell.value is None or cell.font == TITLE_FONT:
                continue
            if cell.font.italic:
                continue
            ln = 8 if isinstance(cell.value, float) else len(str(cell.value))
            if cell.fill == HEADER_FILL:
                ln = min(ln, 16)
            widths[cell.column_letter] = max(
                widths.get(cell.column_letter, 0), ln)
    for col, w in widths.items():
        ws.column_dimensions[col].width = max(min_w, min(max_w, w + 2))


# =====================================================================
# Production model (imported read-only)
# =====================================================================
fsm = None  # set by load_production_module()


def resolve_production_dir(explicit=None):
    candidates = [explicit, os.environ.get("FASTPARK_PRODUCTION_DIR"),
                  PRODUCTION_DIR]
    for c in candidates:
        if c:
            path = Path(c)
            if (path / "simplified_fastpark_shortterm.py").exists():
                return path
            raise FileNotFoundError(
                f"simplified_fastpark_shortterm.py not found in {path}")
    for parent in [HERE, *HERE.parents]:
        for guess in (parent / "05 Production Forecast",
                      parent / "FastPark Short Term Model - Oct 2026 Update"
                      / "05 Production Forecast"):
            if (guess / "simplified_fastpark_shortterm.py").exists():
                return guess
    raise FileNotFoundError(
        "Could not find simplified_fastpark_shortterm.py. Set PRODUCTION_DIR "
        "at the top of this script, or pass --production-dir.")


def load_production_module(production_dir):
    global fsm
    if str(production_dir) not in sys.path:
        sys.path.insert(0, str(production_dir))
    import simplified_fastpark_shortterm as module
    fsm = module
    return module


# =====================================================================
# Inputs
# =====================================================================
def daily_forecast_dir():
    if DAILY_FORECAST_DIR:
        return Path(DAILY_FORECAST_DIR)
    return Path(fsm.PRODUCTION_CONFIG["output_base_path"]) / "Daily Forecast Output"


def find_forecast_file(forecast_date, explicit=None):
    """
    The dated daily forecast for `forecast_date`, or an explicit path.
    Never falls back to another day's file.
    """
    if explicit:
        path = Path(explicit)
        if not path.exists():
            raise FileNotFoundError(path)
        return path
    folder = daily_forecast_dir()
    path = folder / f"{forecast_date:%Y-%m-%d}_daily_forecast.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"No production forecast for {forecast_date:%d %b %Y}:\n  {path}\n"
            "Has the daily production run finished? To adjust another day's "
            "forecast use --date YYYY-MM-DD, or point at a file with --forecast."
        )
    return path


def load_forecast(path):
    """Read the production daily output; return (df, run_date)."""
    return prepare_forecast(pd.read_csv(path))


def prepare_forecast(df):
    """Normalise a daily forecast table (file or in-memory); return (df, run_date)."""
    df = df.copy()
    if "target_date" not in df.columns:
        # legacy "<date>_daily_forecast.csv" layout
        first = df.columns[0]
        df = df.rename(columns={
            first: "target_date",
            "booking_entries": "entry_bookings",
            "booking_exits": "exit_bookings",
        })
    df["target_date"] = pd.to_datetime(df["target_date"]).dt.normalize()
    if "horizon_days" in df.columns and (df["horizon_days"] == 0).any():
        run_date = df.loc[df["horizon_days"] == 0, "target_date"].iloc[0]
    else:
        run_date = df["target_date"].min()
        df["horizon_days"] = (df["target_date"] - run_date).dt.days
    required = {"entry_forecast", "exit_forecast",
                "entry_bookings", "exit_bookings"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Forecast file missing columns: {sorted(missing)}")
    return df.sort_values("target_date").reset_index(drop=True), run_date


def load_booking_weights():
    """Model's learned weight on the booking component, by flow and horizon."""
    w = pd.read_csv(fsm.PRODUCTION_CONFIG["forecast_weights_path"])
    out = {}
    for flow in FLOWS:
        sub = w[w["demand_type"].str.lower() == flow]
        out[flow] = dict(zip(
            sub["horizon_days"].astype(int),
            pd.to_numeric(sub[f"{flow}_booking_shrunk"], errors="coerce"),
        ))
    return out


def booking_weight(weights, flow, h):
    if BOOKING_WEIGHT_OVERRIDE is not None:
        return float(BOOKING_WEIGHT_OVERRIDE)
    table = weights.get(flow, {})
    if not table:
        return 0.5
    h = int(np.clip(h, min(table), max(table)))
    val = table.get(h, np.nan)
    return 0.5 if pd.isna(val) else float(val)


def load_history(run_date, seasons, leads):
    """
    Bookings, movements, daily actuals and booking curves for each season's
    Sep-Jan, using the production script's own loaders and logic.
    """
    config = dict(fsm.PRODUCTION_CONFIG)
    config["current_run_date"] = run_date
    engine = fsm.get_engine(dsn=config["db_dsn"],
                            username=config["db_username"])
    bk_parts, op_parts = [], []
    for s in seasons:
        start = pd.Timestamp(s, 9, 1)
        end = min(pd.Timestamp(s + 1, 2, 1), run_date)
        if end <= start:
            continue
        print(f"Loading {season_label(s)} history "
              f"({start:%d %b %Y} to {end:%d %b %Y}) ...")
        bk_parts.append(fsm.get_fastpark_bookings(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            statuses=[config["valid_booking_status"],
                      config["cancelled_status"], "F"],
            asset_name=config["asset_name"],
            engine=engine,
        ))
        op_parts.append(fsm.get_fastpark_entry_exits(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            engine=engine,
        ))
    bk_parts = [p for p in bk_parts if len(p)]
    op_parts = [p for p in op_parts if len(p)]
    bookings = (pd.concat(bk_parts, ignore_index=True)
                .drop_duplicates(subset=["bookingUuid"]))
    operations = pd.concat(op_parts, ignore_index=True).drop_duplicates()

    bookings_clean = fsm.clean_bookings(bookings, config)
    operations_clean = fsm.clean_operations(operations, config)
    master = fsm.reconcile_bookings_to_operations(bookings_clean,
                                                  operations_clean)
    daily = fsm.create_daily_fastpark_actuals(master)
    daily = daily[daily["date"] < run_date].set_index("date")

    curves = {}
    for flow, (_, _, target_field, actual_field, _) in FLOWS.items():
        print(f"Building {flow} booking curves for {len(leads)} horizons ...")
        c = fsm.build_historical_booking_curve(
            bookings=bookings_clean,
            master=master,
            target_field=target_field,
            actual_field=actual_field,
            cutoff_days=leads,
        )
        c["target_date"] = pd.to_datetime(c["target_date"]).dt.normalize()
        curves[flow] = c.set_index(["target_date", "lead_days"])
    return daily, curves


# =====================================================================
# Calculation
# =====================================================================
def season_coverage(daily, curves, prior):
    """actual / bookings known on the day (lead 0) across each window."""
    coverage, valid = {}, {}
    for s in prior:
        w = christmas_window(s)
        for flow, (_, _, _, _, col) in FLOWS.items():
            act = daily[col].reindex(w).sum(min_count=1)
            try:
                known = curves[flow].xs(0, level="lead_days")[
                    "bookings_known"].reindex(w).sum()
            except KeyError:
                known = np.nan
            cov = safe_ratio(act, known)
            coverage[(s, flow)] = cov
            valid[(s, flow)] = pd.notna(cov) and cov >= MIN_ACTUAL_COVERAGE
    return coverage, valid


def baseline_avg(daily, col, start, end, season):
    s = to_season_date(start, season)
    e = to_season_date(end, season)
    vals = daily.loc[(daily.index >= s) & (daily.index <= e), col]
    return vals.mean() if len(vals) >= 20 else np.nan


def in_scope(d, window):
    if d not in window:
        return False
    return ADJUST_DAYS == "window" or is_major(d)


def build_adjustment(fc, run_date, daily, curves, weights, prior):
    current = season_of(run_date)
    window = christmas_window(current)
    coverage, valid = season_coverage(daily, curves, prior)
    use = {f: [s for s in prior if valid[(s, f)]] for f in FLOWS}

    base_end = min(run_date - pd.Timedelta(days=1),
                   pd.Timestamp(current, 12, 14))
    base_start = base_end - pd.Timedelta(days=27)

    out = fc.copy()
    for flow, (fcol, bcol, _, _, dcol) in FLOWS.items():
        out[f"model_{fcol}"] = out[fcol]
        for c in ["bookings_based", "history_based", "booking_weight",
                  "christmas_forecast", "ly_actual_matched"]:
            out[f"xmas_{flow}_{c}"] = np.nan
        out[f"xmas_{flow}_years_used"] = ""
    out["xmas_holiday"] = out["target_date"].map(holiday_name)
    out["xmas_adjusted"] = False

    base_now = {f: baseline_avg(daily, FLOWS[f][4], base_start, base_end,
                                current) for f in FLOWS}
    base_prior = {(s, f): baseline_avg(daily, FLOWS[f][4], base_start,
                                       base_end, s)
                  for s in prior for f in FLOWS}

    max_h = int(out["horizon_days"].max())
    for i, row in out.iterrows():
        d = row["target_date"]
        if not in_scope(d, window):
            continue
        h = int(row["horizon_days"])
        adjusted_any = False
        for flow, (fcol, bcol, _, _, dcol) in FLOWS.items():
            known = row[bcol]
            # The production script extracts bookings with entryDate <
            # run date + lookahead, so the final horizon has no entry
            # bookings. Treat a zero there as unknown, not as zero demand.
            if h == max_h and (pd.isna(known) or known == 0):
                known = np.nan
            ratios, idx = [], []
            for s in use[flow]:
                m = matched_date(d, s)
                key = (m, h)
                if key in curves[flow].index:
                    rec = curves[flow].loc[key]
                    if rec["bookings_known"] > 0:
                        ratios.append(rec["actual_demand"]
                                      / rec["bookings_known"])
                a = daily[dcol].get(m, np.nan)
                b = base_prior[(s, flow)]
                r = safe_ratio(a, b)
                if pd.notna(r):
                    idx.append(r)
            bk_est = (known * np.mean(ratios)
                      if ratios and pd.notna(known) else np.nan)
            hist_est = (base_now[flow] * np.mean(idx)
                        if idx and pd.notna(base_now[flow]) else np.nan)
            w = booking_weight(weights, flow, h)
            if pd.notna(bk_est) and pd.notna(hist_est):
                xmas = w * bk_est + (1 - w) * hist_est
            elif pd.notna(bk_est):
                xmas = bk_est
            elif pd.notna(hist_est):
                xmas = hist_est
            else:
                xmas = np.nan

            ly = current - 1
            ly_act = daily[dcol].get(matched_date(d, ly), np.nan)

            out.at[i, f"xmas_{flow}_bookings_based"] = bk_est
            out.at[i, f"xmas_{flow}_history_based"] = hist_est
            out.at[i, f"xmas_{flow}_booking_weight"] = w
            out.at[i, f"xmas_{flow}_christmas_forecast"] = xmas
            out.at[i, f"xmas_{flow}_ly_actual_matched"] = ly_act
            out.at[i, f"xmas_{flow}_years_used"] = ", ".join(
                season_label(s) for s in use[flow])
            if pd.notna(xmas):
                model = row[fcol]
                blended = (xmas if pd.isna(model) else
                           CHRISTMAS_WEIGHT * xmas
                           + (1 - CHRISTMAS_WEIGHT) * model)
                out.at[i, fcol] = max(0.0, blended)
                adjusted_any = True
        out.at[i, "xmas_adjusted"] = adjusted_any

    meta = {
        "coverage": coverage, "valid": valid, "use": use,
        "baseline_text": f"{base_start:%d %b} to {base_end:%d %b}",
        "base_now": base_now, "window": window, "current": current,
    }
    return out, meta


# =====================================================================
# Outputs
# =====================================================================
def chart(out, meta, run_date, path):
    window = meta["window"]
    sub = out[out["target_date"].isin(window)].set_index("target_date")
    sub = sub.reindex(window)
    x = np.arange(len(window))
    fig = new_fig(13, 8.5)
    axes = fig.subplots(2, 1, sharex=True)
    for ax, flow in zip(axes, FLOWS):
        fcol = FLOWS[flow][0]
        style_axes(ax)
        shade_holidays(ax, window)
        ax.plot(x, sub[f"xmas_{flow}_ly_actual_matched"], color="#8a8984",
                linewidth=1.8, linestyle="--",
                label="Last year actual (matched day)")
        ax.plot(x, sub[f"model_{fcol}"], color=CATEGORICAL[1],
                linewidth=2.0, marker="o", markersize=4,
                markeredgecolor=SURFACE, label="Production model")
        ax.plot(x, sub[fcol], color=CATEGORICAL[0], linewidth=2.6,
                marker="o", markersize=4, markeredgecolor=SURFACE,
                label="Christmas-adjusted")
        ax.set_ylabel("Entries per day" if flow == "entry" else "Exits per day",
                      color=INK2, fontsize=9)
        ax.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    day_ticks(axes[-1], window)
    axes[0].legend(ncol=3, frameon=False, fontsize=8, loc="lower left",
                   bbox_to_anchor=(0, 1.0))
    return finish(
        fig, path,
        f"Christmas-adjusted forecast (model run {run_date:%d %b %Y})",
        "Shaded = major days. Christmas-adjusted = model's booking weight x "
        "bookings-based + rest x history-based, from prior years' matched "
        "days.")


def summary_table(out, meta, flow):
    fcol, bcol = FLOWS[flow][0], FLOWS[flow][1]
    sub = out[out["target_date"].isin(meta["window"])].copy()
    label = "Entries" if flow == "entry" else "Exits"
    df = pd.DataFrame({
        "Date": sub["target_date"].map(
            lambda d: day_text(d, with_holiday=False)),
        "Holiday": sub["xmas_holiday"],
        "Days out": sub["horizon_days"],
        f"{label}|Bookings now": sub[bcol],
        f"{label}|Model forecast": sub[f"model_{fcol}"],
        f"{label}|Bookings-based": sub[f"xmas_{flow}_bookings_based"],
        f"{label}|History-based": sub[f"xmas_{flow}_history_based"],
        f"{label}|Booking weight": sub[f"xmas_{flow}_booking_weight"],
        f"{label}|Final forecast": sub[fcol],
        f"{label}|Change vs model": sub[fcol] / sub[f"model_{fcol}"] - 1,
        f"{label}|LY actual (matched)": sub[f"xmas_{flow}_ly_actual_matched"],
    })
    return df


def write_excel(path, out, meta, run_date, src, chart_path):
    wb = Workbook()
    wb.remove(wb.active)
    ws = wb.create_sheet("Summary")
    ws.sheet_view.showGridLines = False
    r = write_title(
        ws, 1,
        f"FastPark Christmas-adjusted forecast - model run {run_date:%d %b %Y}",
        f"Source: {src}. Adjusting: "
        f"{'every day in the window' if ADJUST_DAYS == 'window' else 'major days only'}"
        f", Christmas weight {CHRISTMAS_WEIGHT:.0%}. "
        f"Entries history: {', '.join(season_label(s) for s in meta['use']['entry']) or 'none'}; "
        f"exits history: {', '.join(season_label(s) for s in meta['use']['exit']) or 'none'}.")
    fmts = {"Change": FMT_DIFF, "weight": FMT_PCT,
            "Days out": FMT_DAYS}
    for flow in FLOWS:
        r, _ = write_table(ws, summary_table(out, meta, flow), r,
                              title="Entries" if flow == "entry" else "Exits",
                              formats=fmts, scale="div",
                              scale_cols=["Change"], total_row=True)
    autosize(ws)
    ws.column_dimensions["A"].width = 14
    img = XLImage(str(chart_path))
    f = 900 / img.width
    img.width, img.height = int(img.width * f), int(img.height * f)
    ws.add_image(img, f"A{r + 1}")

    ws = wb.create_sheet("Method")
    ws.sheet_view.showGridLines = False
    r = write_title(ws, 1, "How the adjustment works")
    rows = [
        ("Bookings-based", "Bookings known now (from the model output) x "
                           "average of (actual / bookings known at the same "
                           "days-out) on prior years' matched days. Uses the "
                           "model's own cancellation-aware booking curves."),
        ("History-based", "This year's daily average over "
                          + meta["baseline_text"] + " x average of (actual on "
                          "the matched day / that year's average over the "
                          "same dates)."),
        ("Booking weight", "The production model's learned weight on its "
                           "booking component at that horizon "
                           "(forecast_weights.csv)"
                           + ("" if BOOKING_WEIGHT_OVERRIDE is None else
                              f" - overridden to {BOOKING_WEIGHT_OVERRIDE}")
                           + ". Final = weight x bookings-based + (1 - weight)"
                             " x history-based."),
        ("Matched day", "24, 25, 26, 31 Dec, 1 and 2 Jan matched on date; "
                        "other days on the same weekday within 3 days, in "
                        "the same part of the period."),
        ("History years", "Years whose actual movements are below "
                          f"{MIN_ACTUAL_COVERAGE:.0%} of bookings are dropped "
                          "(incomplete movement records)."),
    ]
    r, _ = write_table(ws, pd.DataFrame(rows, columns=["Item", "Detail"]), r)
    cov = pd.DataFrame([
        {"Season": season_label(s),
         "Entry actuals / bookings": meta["coverage"][(s, "entry")],
         "Entries used?": "Yes" if meta["valid"][(s, "entry")] else "No",
         "Exit actuals / bookings": meta["coverage"][(s, "exit")],
         "Exits used?": "Yes" if meta["valid"][(s, "exit")] else "No"}
        for s in sorted({k[0] for k in meta["coverage"]})
    ])
    r, _ = write_table(ws, cov, r, title="History data check",
                          formats={"/ bookings": FMT_PCT})
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 100
    for col in "CDE":
        ws.column_dimensions[col].width = 16

    wb.save(path)


# =====================================================================
# Main
# =====================================================================
def adjust_daily_forecast(daily_df, run_date=None, output_dir=OUTPUT_DIR,
                          production_module=None,
                          source_label="production run", save=True):
    """
    Christmas-adjust an in-memory daily forecast table.

    Called by the production script (toggle_christmas_adjustment) straight
    after its daily forecast, and by run() below for a saved file.

    daily_df          : production daily forecast (target_date, horizon_days,
                        entry/exit_forecast, entry/exit_bookings, ...).
    run_date          : forecast run date (default: horizon 0 / first date).
    production_module : the already-loaded production module, so it isn't
                        imported a second time. None = find and import it.
    save              : write the workings (csv / xlsx / png) to output_dir.

    Returns (adjusted_df, workings_df, csv_path):
      adjusted_df  - daily_df with the same columns, entry_forecast and
                     exit_forecast replaced on the adjusted days;
      workings_df  - every column used in the adjustment (None if nothing
                     to adjust);
      csv_path     - path of the saved workings CSV (None if not saved).
    """
    global fsm
    if production_module is not None:
        fsm = production_module
    elif fsm is None:
        load_production_module(resolve_production_dir())

    fc, inferred = prepare_forecast(daily_df)
    run_date = (pd.Timestamp(run_date).normalize() if run_date is not None
                else inferred)

    current = season_of(run_date)
    window = christmas_window(current)
    in_fc = fc["target_date"].isin(window)
    if not in_fc.any():
        print("Christmas adjustment: no Christmas window days in this "
              "forecast - nothing to adjust.")
        return daily_df.copy(), None, None
    if in_fc.sum() < len(window):
        print(f"  Forecast covers {in_fc.sum()} of {len(window)} window days.")

    leads = sorted({0} | set(fc.loc[in_fc, "horizon_days"].astype(int)))
    prior = [s for s in range(FIRST_SEASON, current)
             if s not in EXCLUDE_SEASONS]
    daily, curves = load_history(run_date, prior + [current], leads)
    weights = load_booking_weights()

    out, meta = build_adjustment(fc, run_date, daily, curves, weights, prior)
    for s in prior:
        for f in FLOWS:
            if not meta["valid"][(s, f)]:
                cov = meta["coverage"][(s, f)]
                print(f"  {season_label(s)} {f}s not used: actuals cover "
                      f"{0 if pd.isna(cov) else cov:.0%} of bookings")

    csv_path = xlsx_path = None
    if save:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        stem = f"christmas_daily_forecast_{run_date:%Y-%m-%d}"
        csv_path = output_dir / f"{stem}.csv"
        out_csv = out.copy()
        out_csv["target_date"] = out_csv["target_date"].dt.strftime("%Y-%m-%d")
        out_csv.to_csv(csv_path, index=False)
        chart_path = chart(out, meta, run_date, output_dir / f"{stem}.png")
        xlsx_path = output_dir / f"{stem}.xlsx"
        write_excel(xlsx_path, out, meta, run_date, source_label, chart_path)

    show = out[out["xmas_adjusted"]][[
        "target_date", "xmas_holiday", "horizon_days",
        "model_entry_forecast", "entry_forecast",
        "model_exit_forecast", "exit_forecast",
    ]].copy()
    show["target_date"] = show["target_date"].dt.strftime("%a %d %b")
    show.columns = ["Date", "Holiday", "Out", "Entry model", "Entry xmas",
                    "Exit model", "Exit xmas"]
    print()
    with pd.option_context("display.width", 140,
                           "display.float_format", "{:,.0f}".format):
        print(show.to_string(index=False))
    if save:
        print(f"\nChristmas workings saved: {csv_path}\n"
              f"                          {xlsx_path}")

    # Hand back the caller's own table with only the forecasts replaced.
    adjusted = daily_df.copy()
    keys = pd.to_datetime(adjusted["target_date"]).dt.normalize()
    by_date = out.set_index("target_date")
    for fcol in ("entry_forecast", "exit_forecast"):
        adjusted[fcol] = keys.map(by_date[fcol]).fillna(adjusted[fcol]).values
    return adjusted, out, csv_path


def run(forecast_path=None, output_dir=OUTPUT_DIR, production_dir=None,
        forecast_date=None):
    """Adjust a saved dated daily forecast file (standalone use)."""
    production_dir = resolve_production_dir(production_dir)
    load_production_module(production_dir)
    print(f"Production model:    {production_dir}")
    forecast_date = pd.Timestamp(
        forecast_date or datetime.now().date()).normalize()
    src = find_forecast_file(forecast_date, forecast_path)
    fc, run_date = load_forecast(src)
    print(f"Production forecast: {src}")
    print(f"Model run date:      {run_date:%a %d %b %Y}")
    if run_date != forecast_date:
        msg = (f"Forecast file starts on {run_date:%d %b %Y}, not "
               f"{forecast_date:%d %b %Y}.")
        if forecast_path:
            print(f"  NOTE: {msg}")
        else:
            raise ValueError(msg + " Check the file before adjusting it.")
    _, _, csv_path = adjust_daily_forecast(
        fc, run_date, output_dir=output_dir, source_label=src.name)
    return csv_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Christmas adjustment of the FastPark production forecast")
    parser.add_argument("--date", default=None,
                        help="Forecast run date to adjust, YYYY-MM-DD "
                             "(default today)")
    parser.add_argument("--forecast", default=None,
                        help="Path to a specific daily forecast CSV instead")
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--production-dir", default=None,
                        help="Folder containing simplified_fastpark_shortterm.py")
    args = parser.parse_args()
    run(forecast_path=args.forecast, output_dir=args.out_dir,
        production_dir=args.production_dir, forecast_date=args.date)
