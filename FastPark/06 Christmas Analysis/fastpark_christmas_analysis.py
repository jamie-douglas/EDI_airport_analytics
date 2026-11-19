"""
FastPark Christmas & New Year analysis.

Purpose
-------
Compare the Christmas / New Year period (the Friday before Christmas to the
Sunday after New Year's Day) across every season we hold data for, and use
the history to sense-check what this year is likely to do.

For every day in the window it looks at:

1. Actual entries and exits (with the day of the week), for every season.
2. Booking curves: how many of each day's bookings were already on hand at
   each horizon (D0 to D42+) and when those bookings were made, and how that
   has shifted year on year.
3. Each day against its monthly average (and a recent 4-week average), as a
   % above / below, and what that implies for this year.
4. Where this year's bookings are now against the same point in prior years,
   and two projections for this year's actuals:
     - Booking pace:   bookings on hand now x historic (actual / bookings on
                       hand at the same days-out).
     - Average index:  this year's recent 4-week daily average x historic
                       (actual / recent 4-week average).

Outputs (outputs/fastpark_christmas_analysis/)
----------------------------------------------
- FastPark_Christmas_Analysis_<run date>.xlsx: one workbook. "Read Me" and
  "Summary" first, charts embedded on the "Charts" sheet, detail after.
- PNG copies of each chart.

Comparing days across years
---------------------------
Three alignments are used, because Christmas moves around the week:

- Each year's own window: the Friday before Christmas + n days. Weekdays
  line up (Fri with Fri) but holidays do not.
- Same calendar date: 25 Dec with 25 Dec. Holidays line up, weekdays do not.
- Matched day (used for the projections): the major days (24, 25, 26, 31 Dec,
  1 and 2 Jan) are matched on date, so they count as equal across years. Every
  other day is matched to the same weekday within 3 days, in the same part of
  the period (before Christmas / between Christmas and Hogmanay / after New
  Year), and never onto a major day. If there is no such day, the same
  calendar date is used.

Data
----
- Bookings:  AirportX.v_Bookings, assetName = 'FastPark', status = 'B'.
- Movements: FastPark.v_EntryAndExits, distinct BookingReference per day,
             entries by CheckInStarted, exits by ActualCheckedOutDate.

Run
---
    python scripts/fastpark_christmas_analysis.py
    python scripts/fastpark_christmas_analysis.py --as-of 2026-11-17
"""

import argparse
import pathlib
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

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))


# =====================================================================
# CONFIG - edit here
# =====================================================================
# "As at" date. Bookings created up to the day before are counted as on hand.
# None = today.
AS_OF_DATE = None

# Earliest Christmas to try (Christmas year, e.g. 2021 = Christmas 2021 / NY
# 2022). Seasons with no data are skipped automatically.
FIRST_SEASON = 2019

# Seasons shown in the tables but left out of averages and projections
# (Covid-affected).
EXCLUDE_FROM_AVERAGE = {2020, 2021}

# Actual movements must be at least this share of final bookings for a
# season to count (catches years where movement records are incomplete).
MIN_ACTUAL_COVERAGE = 0.7

# Horizons shown in the booking curve tables and plots.
KEY_HORIZONS = [42, 35, 28, 21, 14, 7, 3, 1, 0]
PLOT_MAX_HORIZON = 42

# Optional: if you have a forecast daily average for this December / January,
# put it here. The workbook then adds a projection based on each day's
# historic % above / below its own monthly average.
THIS_YEAR_MONTHLY_AVG = {
    "Dec": {"Entries": None, "Exits": None},
    "Jan": {"Entries": None, "Exits": None},
}

OUTPUT_DIR = Path("outputs/fastpark_christmas_analysis")


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

TYPES = ["Entries", "Exits"]


def season_label(season):
    return f"{season}/{str(season + 1)[-2:]}"


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
    """Like-for-like day in another season (see module docstring)."""
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


def season_of(ts):
    return ts.year if ts.month >= 7 else ts.year - 1


# =====================================================================
# Data loaders (kept separate so they can be swapped / tested)
# =====================================================================
def load_daily_movements(start, end):
    """Daily distinct entries and exits in [start, end)."""
    from modules.utils.db import get_engine
    from modules.utils.sql import read_sql

    engine = get_engine()
    series = {}
    for label, col in [
        ("Entries", "CheckInStarted"),
        ("Exits", "ActualCheckedOutDate"),
    ]:
        sql = f"""
            SELECT CAST({col} AS date) AS [day],
                   COUNT(DISTINCT BookingReference) AS n
            FROM FastPark.v_EntryAndExits
            WHERE {col} >= :start AND {col} < :end
              AND BookingReference IS NOT NULL
            GROUP BY CAST({col} AS date)
        """
        df = read_sql(
            engine,
            sql,
            params={
                "start": start.strftime("%Y-%m-%d"),
                "end": end.strftime("%Y-%m-%d"),
            },
        )
        df["day"] = pd.to_datetime(df["day"])
        series[label] = df.set_index("day")["n"].astype(float)
    return pd.DataFrame(series).sort_index()


def load_bookings(start, end, anchor):
    """
    Live FastPark bookings whose entryDate / exitDate is in [start, end).
    Returns bookingUuid, creation_day, day (the anchor date, normalised).
    """
    from modules.utils.query import query

    df = query(
        table="AirportX.v_Bookings",
        columns=["bookingUuid", "createdAt", "entryDate", "exitDate"],
        where=["assetName = 'FastPark'", "status = 'B'"],
        date_column=anchor,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
    )
    df = pd.DataFrame(df)
    if df.empty:
        return pd.DataFrame(columns=["bookingUuid", "creation_day", "day"])
    df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce")
    df[anchor] = pd.to_datetime(df[anchor], errors="coerce")
    df = (
        df.dropna(subset=["bookingUuid", "createdAt", anchor])
        .sort_values(["bookingUuid", "createdAt"])
        .drop_duplicates("bookingUuid", keep="first")
    )
    return pd.DataFrame({
        "bookingUuid": df["bookingUuid"].values,
        "creation_day": df["createdAt"].dt.normalize().values,
        "day": df[anchor].dt.normalize().values,
    })


# =====================================================================
# Calculations
# =====================================================================
def booking_curve(bk, days, max_h):
    """
    On-hand bookings for each day at each horizon h (created <= day - h).
    Returns DataFrame index=day, columns=0..max_h, plus 'Final'.
    """
    out = pd.DataFrame(index=days)
    if bk.empty:
        for h in range(max_h + 1):
            out[h] = 0.0
        out["Final"] = 0.0
        return out
    bk = bk.copy()
    bk["lead"] = (bk["day"] - bk["creation_day"]).dt.days
    grouped = bk.groupby("day")
    out["Final"] = grouped.size().reindex(days).fillna(0).astype(float)
    for h in range(max_h + 1):
        out[h] = (
            (bk["lead"] >= h)
            .groupby(bk["day"])
            .sum()
            .reindex(days)
            .fillna(0)
            .astype(float)
        )
    return out


def median_lead(bk, days):
    if bk.empty:
        return pd.Series(np.nan, index=days)
    lead = (bk["day"] - bk["creation_day"]).dt.days
    return lead.groupby(bk["day"]).median().reindex(days)


def safe_ratio(a, b):
    try:
        if b is None or pd.isna(b) or b == 0 or pd.isna(a):
            return np.nan
    except TypeError:
        return np.nan
    return a / b


def mean_or_nan(values):
    vals = [v for v in values if pd.notna(v)]
    return float(np.mean(vals)) if vals else np.nan


def collect_data(as_of, seasons, current, loaders):
    load_moves, load_bk = loaders
    cutoff = as_of - pd.Timedelta(days=1)

    cur_window = christmas_window(current)
    h_needed = (cur_window[-1] - cutoff).days
    max_h = max(PLOT_MAX_HORIZON, max(KEY_HORIZONS), h_needed) + 1

    # ---- daily movements, one query for the whole span
    mv_start = pd.Timestamp(seasons[0], 10, 1)
    mv_end = min(pd.Timestamp(current + 1, 2, 1), as_of)
    print(f"Loading daily movements {mv_start:%d %b %Y} to {mv_end:%d %b %Y} ...")
    moves = load_moves(mv_start, mv_end)
    moves = moves[moves.index <= cutoff]

    # ---- bookings, per season
    curves, leads, bookings_raw = {}, {}, {}
    for s in seasons:
        bstart = pd.Timestamp(s, 12, 10)
        bend = pd.Timestamp(s + 1, 1, 16)
        days = pd.date_range(bstart, bend - pd.Timedelta(days=1), freq="D")
        for t, anchor in [("Entries", "entryDate"), ("Exits", "exitDate")]:
            print(f"Loading {season_label(s)} bookings by {anchor} ...")
            bk = load_bk(bstart, bend, anchor)
            if s == current:
                bk = bk[bk["creation_day"] <= cutoff]
            bookings_raw[(s, t)] = bk
            curves[(s, t)] = booking_curve(bk, days, max_h)
            leads[(s, t)] = median_lead(bk, days)

    return {
        "moves": moves,
        "curves": curves,
        "leads": leads,
        "bookings_raw": bookings_raw,
        "max_h": max_h,
        "cutoff": cutoff,
    }


def check_actuals_coverage(data, seasons, current):
    """
    Movement records are incomplete in early years (e.g. CheckInStarted is
    largely empty before May 2024). For each season and type, compare actual
    movements in the window with final bookings. Below MIN_ACTUAL_COVERAGE
    the actuals are blanked (Oct-Feb of that season) so they don't distort
    comparisons. Booking curves are unaffected.
    """
    moves = data["moves"]
    coverage, valid = {}, {}
    for s in seasons:
        w = christmas_window(s)
        for t in TYPES:
            if s == current:
                coverage[(s, t)] = np.nan
                valid[(s, t)] = True
                continue
            act = moves[t].reindex(w).sum(min_count=1)
            bk = data["curves"][(s, t)]["Final"].reindex(w).sum()
            cov = safe_ratio(act, bk)
            coverage[(s, t)] = cov
            ok = pd.notna(cov) and cov >= MIN_ACTUAL_COVERAGE
            valid[(s, t)] = ok
            if not ok:
                mask = ((moves.index >= pd.Timestamp(s, 10, 1))
                        & (moves.index < pd.Timestamp(s + 1, 2, 1)))
                moves.loc[mask, t] = np.nan
                print(f"  {season_label(s)} {t.lower()}: actuals cover "
                      f"{0 if pd.isna(cov) else cov:.0%} of bookings - "
                      f"treated as missing")
    data["coverage"] = coverage
    data["valid"] = valid


def actual(moves, ts, t):
    if ts in moves.index:
        return moves.at[ts, t]
    return np.nan


def monthly_averages(moves, seasons, current, cutoff):
    """Daily averages for Nov / Dec / Jan and a recent 4-week baseline."""
    base_end_cur = min(cutoff, pd.Timestamp(current, 12, 14))
    base_start_cur = base_end_cur - pd.Timedelta(days=27)
    rows = []
    for s in seasons:
        row = {"Season": season_label(s), "_season": s}
        b_start = to_season_date(base_start_cur, s)
        b_end = to_season_date(base_end_cur, s)
        for t in TYPES:
            for label, start, end in [
                ("Nov", pd.Timestamp(s, 11, 1), pd.Timestamp(s, 11, 30)),
                ("Dec", pd.Timestamp(s, 12, 1), pd.Timestamp(s, 12, 31)),
                ("Jan", pd.Timestamp(s + 1, 1, 1), pd.Timestamp(s + 1, 1, 31)),
                ("Recent 4 wks", b_start, b_end),
            ]:
                sl = moves.loc[(moves.index >= start) & (moves.index <= end), t]
                expected_days = (min(end, cutoff) - start).days + 1
                complete = expected_days >= (end - start).days + 1
                val = sl.mean() if len(sl) else np.nan
                if label != "Recent 4 wks" and not complete:
                    val = np.nan  # partial month - don't present as a monthly avg
                row[f"{label} {t}"] = val
        rows.append(row)
    df = pd.DataFrame(rows)
    baseline_text = (
        f"{base_start_cur:%d %b} to {base_end_cur:%d %b} "
        f"(same dates each year, so 4 of each weekday)"
    )
    return df, baseline_text


def build_analysis(data, as_of, seasons, current):
    moves = data["moves"]
    curves = data["curves"]
    cutoff = data["cutoff"]
    prior = [s for s in seasons if s < current]
    avg_seasons = [s for s in prior if s not in EXCLUDE_FROM_AVERAGE]
    valid = data.get("valid", {})
    # seasons with usable actuals, per type - used for anything needing actuals
    proj_seasons = {
        t: [s for s in avg_seasons if valid.get((s, t), True)] for t in TYPES
    }
    ly = current - 1 if (current - 1) in prior else None

    cur_window = christmas_window(current)
    monthly, baseline_text = monthly_averages(moves, seasons, current, cutoff)
    mavg = monthly.set_index("_season")

    # ---------- per current day x season: matched info
    match_rows = []
    for pos, d in enumerate(cur_window):
        h_now = max((d - cutoff).days, 0)
        for s in seasons:
            m = d if s == current else matched_date(d, s)
            r = {
                "pos": pos, "day": d, "season": s, "matched": m,
                "h_now": h_now,
            }
            for t in TYPES:
                cv = curves[(s, t)]
                a = actual(moves, m, t)
                r[f"actual_{t}"] = a
                if m in cv.index:
                    r[f"final_{t}"] = cv.at[m, "Final"]
                    r[f"onhand_{t}"] = cv.at[m, h_now]
                else:
                    r[f"final_{t}"] = np.nan
                    r[f"onhand_{t}"] = np.nan
                r[f"uplift_{t}"] = safe_ratio(a, r[f"onhand_{t}"])
                r[f"pct_final_{t}"] = safe_ratio(
                    r[f"onhand_{t}"], r[f"final_{t}"]
                )
                month_key = "Dec" if m.month == 12 else "Jan"
                r[f"vs_month_{t}"] = (
                    safe_ratio(a, mavg.at[s, f"{month_key} {t}"]) - 1
                )
                r[f"vs_recent_{t}"] = safe_ratio(
                    a, mavg.at[s, f"Recent 4 wks {t}"]
                )
            match_rows.append(r)
    match = pd.DataFrame(match_rows)

    # ---------- projections for the current season
    proj_rows = []
    for pos, d in enumerate(cur_window):
        sub = match[match["pos"] == pos]
        cur = sub[sub["season"] == current].iloc[0]
        lyr = sub[sub["season"] == ly].iloc[0] if ly is not None else None
        r = {
            "Date": day_text(d, with_holiday=False),
            "Holiday": holiday_name(d),
            "Days out": (d - cutoff).days,
        }
        for t in TYPES:
            hist = sub[sub["season"].isin(proj_seasons[t])]
            now = cur[f"onhand_{t}"]
            ups = hist[f"uplift_{t}"].dropna()
            idx = hist[f"vs_recent_{t}"].dropna()
            mon = hist[f"vs_month_{t}"].dropna()
            base_now = mavg.at[current, f"Recent 4 wks {t}"]
            r[f"{t}|Bookings now"] = now
            r[f"{t}|LY at same point"] = (
                lyr[f"onhand_{t}"] if lyr is not None else np.nan
            )
            r[f"{t}|vs LY"] = safe_ratio(now, r[f"{t}|LY at same point"]) - 1
            avg_same = hist[f"onhand_{t}"].mean()
            r[f"{t}|Avg at same point"] = avg_same
            r[f"{t}|vs avg"] = safe_ratio(now, avg_same) - 1
            r[f"{t}|LY actual"] = (
                lyr[f"actual_{t}"] if lyr is not None else np.nan
            )
            r[f"{t}|Projected (booking pace)"] = (
                now * ups.mean() if len(ups) else np.nan
            )
            r[f"{t}|Pace range"] = (
                f"{now * ups.min():,.0f} - {now * ups.max():,.0f}"
                if len(ups) else ""
            )
            r[f"{t}|Projected (4-wk avg index)"] = (
                base_now * idx.mean() if len(idx) and pd.notna(base_now)
                else np.nan
            )
            month_key = "Dec" if d.month == 12 else "Jan"
            override = THIS_YEAR_MONTHLY_AVG.get(month_key, {}).get(t)
            if override is not None:
                r[f"{t}|Projected (your monthly avg)"] = (
                    override * (1 + mon.mean()) if len(mon) else np.nan
                )
            r[f"{t}|Actual so far"] = cur[f"actual_{t}"]
        proj_rows.append(r)
    proj = pd.DataFrame(proj_rows)

    return {
        "match": match,
        "proj": proj,
        "monthly": monthly,
        "baseline_text": baseline_text,
        "prior": prior,
        "avg_seasons": avg_seasons,
        "proj_seasons": proj_seasons,
        "ly": ly,
        "cur_window": cur_window,
    }


# =====================================================================
# Table builders (presentation)
# =====================================================================
def table_actuals_own_window(moves, seasons):
    rows = []
    for pos in range(17):
        r = {"Day #": pos + 1}
        for s in seasons:
            d = christmas_window(s)[0] + pd.Timedelta(days=pos)
            in_window = d <= christmas_window(s)[-1]
            txt = day_text(d)
            if not in_window:
                txt += " *"
            r[f"{season_label(s)}|Date"] = txt
            r[f"{season_label(s)}|Entries"] = actual(moves, d, "Entries")
            r[f"{season_label(s)}|Exits"] = actual(moves, d, "Exits")
        rows.append(r)
    return pd.DataFrame(rows)


def table_actuals_same_date(moves, seasons, cur_window):
    rows = []
    for d in cur_window:
        r = {"Date": d.strftime("%d %b"), "Holiday": holiday_name(d)}
        for s in seasons:
            sd = to_season_date(d, s)
            r[f"{season_label(s)}|Day"] = sd.strftime("%a")
            r[f"{season_label(s)}|Entries"] = actual(moves, sd, "Entries")
            r[f"{season_label(s)}|Exits"] = actual(moves, sd, "Exits")
        rows.append(r)
    return pd.DataFrame(rows)


def table_matched(match, seasons, current):
    rows = []
    for pos, sub in match.groupby("pos"):
        d = sub["day"].iloc[0]
        r = {"This year": day_text(d)}
        for s in seasons:
            if s == current:
                continue
            x = sub[sub["season"] == s].iloc[0]
            r[f"{season_label(s)}|Matched to"] = day_text(x["matched"])
            r[f"{season_label(s)}|Entries"] = x["actual_Entries"]
            r[f"{season_label(s)}|Exits"] = x["actual_Exits"]
        rows.append(r)
    return pd.DataFrame(rows)


def table_by_season(match, seasons, value_col, current, avg_seasons,
                    add_avg=True):
    rows = []
    for pos, sub in match.groupby("pos"):
        d = sub["day"].iloc[0]
        r = {"This year": day_text(d)}
        for s in seasons:
            if s == current:
                continue
            r[season_label(s)] = sub[sub["season"] == s][value_col].iloc[0]
        if add_avg:
            r["Average*"] = mean_or_nan(
                [r[season_label(s)] for s in avg_seasons]
            )
        rows.append(r)
    return pd.DataFrame(rows)


def table_curve_block(match, curves, leads, s, t, current):
    sub = match[match["season"] == s]
    cv = curves[(s, t)]
    rows = []
    for _, x in sub.iterrows():
        m = x["matched"]
        r = {
            "This year": day_text(x["day"]),
            "Matched day": day_text(m, with_year=True),
            "Final bookings": cv.at[m, "Final"] if m in cv.index else np.nan,
            "Actual": x[f"actual_{t}"],
            "Median days booked ahead": leads[(s, t)].get(m, np.nan),
        }
        final = r["Final bookings"]
        for h in KEY_HORIZONS:
            val = cv.at[m, h] if m in cv.index else np.nan
            if s == current and h < x["h_now"]:
                val = np.nan
            r[f"D{h}"] = safe_ratio(val, final) if s != current else val
        rows.append(r)
    return pd.DataFrame(rows)


def table_curve_average(match, curves, avg_seasons, t):
    rows = []
    for pos, sub in match.groupby("pos"):
        d = sub["day"].iloc[0]
        r = {"This year": day_text(d)}
        for h in KEY_HORIZONS:
            vals = []
            for s in avg_seasons:
                m = sub[sub["season"] == s]["matched"].iloc[0]
                cv = curves[(s, t)]
                if m in cv.index:
                    vals.append(safe_ratio(cv.at[m, h], cv.at[m, "Final"]))
            r[f"D{h}"] = mean_or_nan(vals)
        rows.append(r)
    return pd.DataFrame(rows)


def table_lead_by_season(match, leads, seasons, current, t):
    rows = []
    for pos, sub in match.groupby("pos"):
        d = sub["day"].iloc[0]
        r = {"This year": day_text(d)}
        for s in seasons:
            if s == current:
                continue
            m = sub[sub["season"] == s]["matched"].iloc[0]
            r[season_label(s)] = leads[(s, t)].get(m, np.nan)
        rows.append(r)
    return pd.DataFrame(rows)


def table_share_booked_early(match, bookings_raw, seasons, current, t,
                             days_ahead=28):
    rows = []
    for pos, sub in match.groupby("pos"):
        d = sub["day"].iloc[0]
        r = {"This year": day_text(d)}
        for s in seasons:
            if s == current:
                continue
            m = sub[sub["season"] == s]["matched"].iloc[0]
            bk = bookings_raw[(s, t)]
            b = bk[bk["day"] == m]
            if len(b):
                lead = (b["day"] - b["creation_day"]).dt.days
                r[season_label(s)] = (lead >= days_ahead).mean()
            else:
                r[season_label(s)] = np.nan
        rows.append(r)
    return pd.DataFrame(rows)


def table_long_curves(data, seasons, current):
    rows = []
    for (s, t), cv in data["curves"].items():
        hcols = [c for c in cv.columns if c != "Final"]
        for d, row in cv.iterrows():
            final = row["Final"]
            for h in hcols:
                if s == current and (d - data["cutoff"]).days > h:
                    continue
                nxt = row[h + 1] if (h + 1) in hcols else np.nan
                rows.append({
                    "Season": season_label(s),
                    "Type": t,
                    "Date": d.strftime("%Y-%m-%d"),
                    "Weekday": d.strftime("%a"),
                    "Holiday": holiday_name(d),
                    "Days before": h,
                    "Bookings on hand": row[h],
                    "Created on this day-before": (
                        row[h] - nxt if pd.notna(nxt) else np.nan
                    ),
                    "Final bookings": final,
                    "% of final on hand": safe_ratio(row[h], final),
                    "Actual": actual(data["moves"], d, t),
                })
    return pd.DataFrame(rows)


def table_long_daily(data, seasons):
    rows = []
    for s in seasons:
        cvE = data["curves"][(s, "Entries")]
        cvX = data["curves"][(s, "Exits")]
        for d in cvE.index:
            rows.append({
                "Season": season_label(s),
                "Date": d.strftime("%Y-%m-%d"),
                "Weekday": d.strftime("%a"),
                "Holiday": holiday_name(d),
                "In that year's window": d in christmas_window(s),
                "Actual entries": actual(data["moves"], d, "Entries"),
                "Actual exits": actual(data["moves"], d, "Exits"),
                "Entry bookings (final)": cvE.at[d, "Final"],
                "Exit bookings (final)": cvX.at[d, "Final"],
            })
    return pd.DataFrame(rows)


# =====================================================================
# Charts
# =====================================================================
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
GRID = "#e4e3df"
BAND = "#f0efec"
CATEGORICAL = [
    "#2a78d6", "#eb6834", "#1baf7a", "#eda100",
    "#e87ba4", "#008300", "#6250d6", "#e34948",
]


def season_colour(s, current):
    """Fixed colour per season: newest = slot 1, and so on back in time."""
    return CATEGORICAL[(current - s) % len(CATEGORICAL)]


def style_axes(ax):
    ax.set_facecolor(SURFACE)
    for side in ["top", "right"]:
        ax.spines[side].set_visible(False)
    for side in ["left", "bottom"]:
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=8, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def shade_holidays(ax, cur_window):
    for i, d in enumerate(cur_window):
        if is_major(d):
            ax.axvspan(i - 0.5, i + 0.5, color=BAND, zorder=0, linewidth=0)


def day_ticks(ax, cur_window):
    labels = [
        f"{d:%a}\n{d:%d %b}" + ("\n" + SHORT_NAMES[(d.month, d.day)]
                                if is_major(d) else "")
        for d in cur_window
    ]
    ax.set_xticks(range(len(cur_window)))
    ax.set_xticklabels(labels, fontsize=7)
    ax.set_xlim(-0.5, len(cur_window) - 0.5)


def new_fig(w, h):
    fig = plt.figure(figsize=(w, h), facecolor=SURFACE)
    return fig


def finish(fig, path, title, subtitle):
    fig.suptitle(title, x=0.01, ha="left", fontsize=13, color=INK,
                 fontweight="bold", y=0.995)
    fig.text(0.01, 0.955, subtitle, ha="left", fontsize=9, color=INK2)
    fig.savefig(path, dpi=110, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    return path


def line_by_season(ax, x, y, s, current, label):
    is_cur = s == current
    ax.plot(
        x, y,
        color=season_colour(s, current),
        linewidth=2.6 if is_cur else 1.8,
        marker="o", markersize=4 if is_cur else 3,
        markeredgecolor=SURFACE, markeredgewidth=0.8,
        label=label, zorder=3 if is_cur else 2,
    )


def chart_actuals(an, seasons, current, path):
    match, cw = an["match"], an["cur_window"]
    fig = new_fig(13, 8.5)
    axes = fig.subplots(2, 1, sharex=True)
    for ax, t in zip(axes, TYPES):
        style_axes(ax)
        shade_holidays(ax, cw)
        for s in seasons:
            sub = match[match["season"] == s].sort_values("pos")
            y = sub[f"actual_{t}"].values
            if np.all(np.isnan(y)):
                continue
            line_by_season(ax, sub["pos"], y, s, current, season_label(s))
        ax.set_ylabel(f"Actual {t.lower()} per day", color=INK2, fontsize=9)
        ax.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    day_ticks(axes[-1], cw)
    axes[0].legend(ncol=len(seasons), frameon=False, fontsize=8,
                   loc="lower left", bbox_to_anchor=(0, 1.0))
    return finish(
        fig, path, "Actual entries and exits, Christmas & New Year",
        "Prior years plotted on their matched day (same date for the major "
        "days, same weekday otherwise). Shaded = major days.")


def chart_vs_month(an, seasons, current, path):
    match, cw = an["match"], an["cur_window"]
    fig = new_fig(13, 8.5)
    axes = fig.subplots(2, 1, sharex=True)
    for ax, t in zip(axes, TYPES):
        style_axes(ax)
        shade_holidays(ax, cw)
        ax.axhline(0, color=INK2, linewidth=1)
        for s in seasons:
            if s == current:
                continue
            sub = match[match["season"] == s].sort_values("pos")
            y = sub[f"vs_month_{t}"].values
            if np.all(np.isnan(y)):
                continue
            line_by_season(ax, sub["pos"], y, s, current, season_label(s))
        ax.set_ylabel(f"{t}: % vs monthly daily avg", color=INK2, fontsize=9)
        ax.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(
                lambda v, _: "0%" if abs(v) < 1e-9 else f"{v:+.0%}"))
    day_ticks(axes[-1], cw)
    axes[0].legend(ncol=len(seasons), frameon=False, fontsize=8,
                   loc="lower left", bbox_to_anchor=(0, 1.0))
    return finish(
        fig, path, "Each day against its monthly average",
        "0% = the average day in that December (or January) that year. "
        "Above the line = busier than an average day.")


def chart_curves(an, data, seasons, current, t, path):
    match, cw = an["match"], an["cur_window"]
    n = len(cw)
    ncols = 6
    nrows = int(np.ceil(n / ncols))
    fig = new_fig(16, 3.0 * nrows + 0.8)
    axes = fig.subplots(nrows, ncols, sharex=True, sharey=True).ravel()
    hs = list(range(PLOT_MAX_HORIZON, -1, -1))
    for i, d in enumerate(cw):
        ax = axes[i]
        style_axes(ax)
        sub = match[match["pos"] == i]
        for s in seasons:
            if s == current:
                continue
            m = sub[sub["season"] == s]["matched"].iloc[0]
            cv = data["curves"][(s, t)]
            if m not in cv.index or cv.at[m, "Final"] == 0:
                continue
            y = [cv.at[m, h] / cv.at[m, "Final"] for h in hs]
            ax.plot(hs, y, color=season_colour(s, current), linewidth=1.6,
                    label=season_label(s))
        h_now = (d - data["cutoff"]).days
        if 0 <= h_now <= PLOT_MAX_HORIZON:
            ax.axvline(h_now, color=INK2, linewidth=1, linestyle="--")
        ax.set_title(day_text(d), fontsize=8.5, color=INK, loc="left")
        ax.set_xlim(PLOT_MAX_HORIZON, 0)
        ax.set_ylim(0, 1.02)
        ax.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0%}"))
    for j in range(n, len(axes)):
        axes[j].set_visible(False)
    for ax in axes[-ncols:]:
        ax.set_xlabel("Days before", fontsize=8, color=INK2)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, ncol=len(labels), frameon=False, fontsize=9,
               loc="upper left", bbox_to_anchor=(0.01, 0.94))
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    word = "entry" if t == "Entries" else "exit"
    return finish(
        fig, path, f"Booking curves: {word} bookings on hand as % of final",
        f"One panel per day this year; prior years on their matched day. "
        f"Dashed line = days-out today. Steeper late = booked later.")


def chart_lead_heatmap(an, data, seasons, current, path):
    match, cw = an["match"], an["cur_window"]
    prior = [s for s in seasons if s != current]
    fig = new_fig(13, 6.5)
    axes = fig.subplots(1, 2, sharey=True)
    mats = {}
    for t in TYPES:
        mat = np.full((len(cw), len(prior)), np.nan)
        for i in range(len(cw)):
            sub = match[match["pos"] == i]
            for j, s in enumerate(prior):
                m = sub[sub["season"] == s]["matched"].iloc[0]
                mat[i, j] = data["leads"][(s, t)].get(m, np.nan)
        mats[t] = mat
    vmax = np.nanmax([np.nanmax(m) for m in mats.values()]) if prior else 1
    vmin = np.nanmin([np.nanmin(m) for m in mats.values()]) if prior else 0
    vmin = min(vmin, vmax - 1)
    cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
        "blue", ["#cde2fb", "#3987e5", "#0d366b"])
    for ax, t in zip(axes, TYPES):
        mat = mats[t]
        ax.imshow(mat, aspect="auto", cmap=cmap, vmin=vmin, vmax=vmax)
        for i in range(mat.shape[0]):
            for j in range(mat.shape[1]):
                v = mat[i, j]
                if np.isnan(v):
                    continue
                ax.text(j, i, f"{v:.0f}", ha="center", va="center",
                        fontsize=8,
                        color="white" if v > vmin + (vmax - vmin) * 0.5
                        else INK)
        ax.set_xticks(range(len(prior)))
        ax.set_xticklabels([season_label(s) for s in prior], fontsize=8)
        ax.set_yticks(range(len(cw)))
        ax.set_yticklabels([day_text(d) for d in cw], fontsize=8)
        ax.set_title(f"{t}", fontsize=10, color=INK, loc="left")
        ax.tick_params(length=0, colors=INK2)
        for sp in ax.spines.values():
            sp.set_visible(False)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    return finish(
        fig, path, "Median days booked ahead",
        "Half of each day's bookings were made at least this many days "
        "before. Darker = booked further ahead.")


def chart_pace(an, seasons, current, path):
    match, cw = an["match"], an["cur_window"]
    fig = new_fig(13, 8.5)
    axes = fig.subplots(2, 1, sharex=True)
    for ax, t in zip(axes, TYPES):
        style_axes(ax)
        shade_holidays(ax, cw)
        for s in seasons:
            sub = match[match["season"] == s].sort_values("pos")
            y = sub[f"onhand_{t}"].values
            if np.all(np.isnan(y)):
                continue
            line_by_season(ax, sub["pos"], y, s, current, season_label(s))
        ax.set_ylabel(f"{'Entry' if t == 'Entries' else 'Exit'} bookings on hand",
                      color=INK2, fontsize=9)
        ax.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    day_ticks(axes[-1], cw)
    axes[0].legend(ncol=len(seasons), frameon=False, fontsize=8,
                   loc="lower left", bbox_to_anchor=(0, 1.0))
    return finish(
        fig, path, "Booking pace: bookings on hand at the same days-out",
        f"This year = bookings made up to {an['cutoff_text']}. Prior years "
        f"= bookings on hand the same number of days before their matched day.")


def chart_projection(an, current, path):
    proj, cw = an["proj"], an["cur_window"]
    ly = an["ly"]
    fig = new_fig(13, 8.5)
    axes = fig.subplots(2, 1, sharex=True)
    x = np.arange(len(cw))
    for ax, t in zip(axes, TYPES):
        style_axes(ax)
        shade_holidays(ax, cw)
        if ly is not None:
            ax.plot(x, proj[f"{t}|LY actual"], color="#8a8984",
                    linewidth=1.8, linestyle="--",
                    label=f"{season_label(ly)} actual (matched day)")
        ax.plot(x, proj[f"{t}|Projected (booking pace)"],
                color=CATEGORICAL[0], linewidth=2.6, marker="o",
                markersize=4, markeredgecolor=SURFACE,
                label="Projected: booking pace")
        ax.plot(x, proj[f"{t}|Projected (4-wk avg index)"],
                color=CATEGORICAL[1], linewidth=2.0, marker="o",
                markersize=4, markeredgecolor=SURFACE,
                label="Projected: 4-week average index")
        col = f"{t}|Projected (your monthly avg)"
        if col in proj:
            ax.plot(x, proj[col], color=CATEGORICAL[2], linewidth=2.0,
                    label="Projected: your monthly average")
        ax.set_ylabel(f"{t} per day", color=INK2, fontsize=9)
        ax.yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    day_ticks(axes[-1], cw)
    axes[0].legend(ncol=4, frameon=False, fontsize=8,
                   loc="lower left", bbox_to_anchor=(0, 1.0))
    return finish(
        fig, path, f"{season_label(current)}: what history suggests",
        "Booking pace = bookings now x historic actual / bookings at the same "
        "days-out. Index = recent 4-week avg x historic day / 4-week avg.")


# =====================================================================
# Excel writer
# =====================================================================
HEADER_FILL = PatternFill("solid", fgColor="1C5CAB")
GROUP_FILL = PatternFill("solid", fgColor="CDE2FB")
TOTAL_FILL = PatternFill("solid", fgColor="F0EFEC")
HEADER_FONT = Font(bold=True, color="FFFFFF", size=10)
TITLE_FONT = Font(bold=True, size=14, color="0B0B0B")
SUB_FONT = Font(bold=True, size=11, color="1C5CAB")
NOTE_FONT = Font(italic=True, size=9, color="52514E")
THIN = Side(style="thin", color="D9D8D3")
BORDER = Border(bottom=THIN)

FMT_INT = "#,##0"
FMT_PCT = "0%"
FMT_DIFF = "+0%;-0%;0%"
FMT_X = '0.00"x"'
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
    scale: 'seq' | 'div' | 'div_x'. scale_cols: substrings of columns to
    colour (default all numeric columns).
    """
    formats = formats or {}
    if title:
        ws.cell(row=row, column=1, value=title).font = SUB_FONT
        row += 1
    if note:
        ws.cell(row=row, column=1, value=note).font = NOTE_FONT
        row += 1

    cols = list(df.columns)
    has_groups = any("|" in str(c) for c in cols)
    if has_groups:
        prev = None
        start_c = None
        for j, c in enumerate(cols, start=1):
            grp = str(c).split("|")[0] if "|" in str(c) else ""
            if grp != prev:
                if prev:
                    ws.merge_cells(start_row=row, start_column=start_c,
                                   end_row=row, end_column=j - 1)
                start_c = j
                prev = grp
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
        label = str(c).split("|")[-1]
        cell = ws.cell(row=row, column=j, value=label)
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
            numeric = pd.api.types.is_numeric_dtype(df[c])
            if j > 1 and numeric and fmt == FMT_INT and "Days" not in str(c):
                L = get_column_letter(j)
                cell = ws.cell(row=row, column=j,
                               value=f"=SUM({L}{first_data}:{L}{last_data})")
                cell.number_format = FMT_INT
                cell.font = Font(bold=True)
        row += 1

    if scale and len(df):
        target = [
            j for j, c in enumerate(cols, start=1)
            if pd.api.types.is_numeric_dtype(df[c])
            and (scale_cols is None or any(k in str(c) for k in scale_cols))
        ]
        # contiguous runs share one colour scale
        runs, run = [], []
        for j in target:
            if run and j != run[-1] + 1:
                runs.append(run)
                run = []
            run.append(j)
        if run:
            runs.append(run)
        for run in runs:
            rng = (f"{get_column_letter(run[0])}{first_data}:"
                   f"{get_column_letter(run[-1])}{last_data}")
            if scale == "seq":
                rule = ColorScaleRule(start_type="min", start_color="FFFFFF",
                                      end_type="max", end_color="5598E7")
            elif scale == "div":
                rule = ColorScaleRule(
                    start_type="num", start_value=-0.5, start_color="E34948",
                    mid_type="num", mid_value=0, mid_color="F0EFEC",
                    end_type="num", end_value=0.5, end_color="2A78D6")
            else:  # div_x, centred on 1.0
                rule = ColorScaleRule(
                    start_type="num", start_value=0.5, start_color="E34948",
                    mid_type="num", mid_value=1, mid_color="F0EFEC",
                    end_type="num", end_value=1.5, end_color="2A78D6")
            ws.conditional_formatting.add(rng, rule)

    return row + 2, header_row


def autosize(ws, min_w=8, max_w=34):
    widths = {}
    for row in ws.iter_rows():
        for cell in row:
            if cell.value is None or cell.font == TITLE_FONT:
                continue
            if cell.font.italic:  # notes can overflow
                continue
            v = cell.value
            if isinstance(v, float):
                ln = 8
            else:
                ln = len(str(v))
            if cell.fill == HEADER_FILL:
                ln = min(ln, 16)
            widths[cell.column_letter] = max(widths.get(cell.column_letter, 0),
                                             ln)
    for col, w in widths.items():
        ws.column_dimensions[col].width = max(min_w, min(max_w, w + 2))


def build_workbook(path, an, data, seasons, current, as_of, charts):
    wb = Workbook()
    wb.remove(wb.active)
    cur_lbl = season_label(current)
    cutoff_txt = data["cutoff"].strftime("%d %b %Y")
    avg_lbl = ", ".join(season_label(s) for s in an["avg_seasons"]) or "none"
    proj_lbl = {
        t: ", ".join(season_label(s) for s in an["proj_seasons"][t]) or "none"
        for t in TYPES
    }
    proj_txt = (f"entries use {proj_lbl['Entries']}; "
                f"exits use {proj_lbl['Exits']}")
    match = an["match"]
    prior = an["prior"]

    # ---------------- Read Me
    ws = wb.create_sheet("Read Me")
    ws.sheet_view.showGridLines = False
    r = write_title(ws, 1, "FastPark Christmas & New Year analysis",
                    f"Run {datetime.now():%d %b %Y %H:%M}. Bookings counted "
                    f"up to and including {cutoff_txt}.")
    windows = pd.DataFrame([
        {
            "Season": season_label(s),
            "Window": (f"{day_text(christmas_window(s)[0], True, False)} to "
                       f"{day_text(christmas_window(s)[-1], True, False)}"),
            "Days": len(christmas_window(s)),
            "Christmas Day": pd.Timestamp(s, 12, 25).strftime("%A"),
            "In averages?": (
                "This year" if s == current
                else ("No (Covid)" if s in EXCLUDE_FROM_AVERAGE else "Yes")
            ),
            "Entry actuals / bookings": data["coverage"].get((s, "Entries")),
            "Entry actuals usable?": (
                "" if s == current
                else ("Yes" if data["valid"].get((s, "Entries")) else "No")),
            "Exit actuals / bookings": data["coverage"].get((s, "Exits")),
            "Exit actuals usable?": (
                "" if s == current
                else ("Yes" if data["valid"].get((s, "Exits")) else "No")),
        }
        for s in seasons
    ])
    r, _ = write_table(
        ws, windows, r, title="Seasons included",
        note=(f"Actuals are only used where actual movements are at least "
              f"{MIN_ACTUAL_COVERAGE:.0%} of final bookings. Movement records "
              f"are incomplete in early years (check-in times mostly missing "
              f"before May 2024), so those years are blank for actuals but "
              f"still used for booking curves."),
        formats={"/ bookings": FMT_PCT, "Days": FMT_DAYS})
    guide = [
        ("Summary", "Start here. Every day this year: bookings now against "
                    "the same point in previous years, and projected entries "
                    "and exits."),
        ("Charts", "All charts in one place."),
        ("Actuals - each year", "Actual entries/exits for each year's own "
                                "window (Friday before Christmas + n days). "
                                "Weekdays line up across years."),
        ("Actuals - same date", "Same calendar date across years. Holidays "
                                "line up; the weekday is shown for each."),
        ("Matched days", "The like-for-like day used for every comparison "
                         "and projection (see below)."),
        ("vs Monthly average", "Each day as % above/below that year's "
                               "monthly daily average, plus the recent "
                               "4-week index used in projections."),
        ("Booking curves - Entries / Exits",
         "When bookings for each day were made: median days booked ahead, "
         "% booked 28+ days out, and % of final bookings on hand at "
         "D42 ... D0, year by year."),
        ("Booking pace", "Bookings on hand now against each prior year at "
                         "the same days-out."),
        ("Uplift factors", "Actual / bookings on hand at today's days-out, "
                           "per year. This drives the booking pace "
                           "projection."),
        ("Data - ...", "Flat tables for your own pivots."),
    ]
    r, _ = write_table(ws, pd.DataFrame(guide, columns=["Sheet", "What it shows"]),
                       r, title="Sheets")
    method = [
        ("Window", "Friday before Christmas to the Sunday after New Year's "
                   "Day."),
        ("Matched day", "Major days (24, 25, 26, 31 Dec, 1 and 2 Jan) are "
                        "matched on date, so they are treated as equal "
                        "across years. Other days are matched to the same "
                        "weekday within 3 days, in the same part of the "
                        "period, and never onto a major day; if none exists "
                        "the same date is used."),
        ("Days out / D-n", "Bookings on hand at D-n = bookings created on or "
                           "before the date n days before the day. This "
                           "year's 'now' = created up to " + cutoff_txt + "."),
        ("Booking pace projection",
         "Bookings now x average of (actual / bookings on hand at the same "
         "days-out); " + proj_txt + ". The range is the lowest to highest "
         "year."),
        ("4-week avg index projection",
         "This year's daily average over " + an["baseline_text"] + " x "
         "average of (actual on the day / the same 4-week average); "
         + proj_txt + "."),
        ("Monthly average", "Average daily entries (or exits) across the "
                            "full calendar month of that year."),
        ("Averages", "Booking curve shapes: average of " + avg_lbl + ". "
                     "Anything using actuals (Average* columns, pace, uplift, "
                     "projections): " + proj_txt + ". Covid seasons are shown "
                     "but never averaged."),
        ("Caveat: cancellations", "Only live bookings (status B) are "
                                  "available. Prior years only include "
                                  "bookings that were never cancelled; this "
                                  "year's still include some that will be "
                                  "cancelled. Booking pace may therefore read "
                                  "slightly high for this year."),
        ("Caveat: movements", "Entries/exits = distinct booking references "
                              "per day (as in the other FastPark scripts); "
                              "movements without a booking reference are "
                              "not counted."),
    ]
    r, _ = write_table(ws, pd.DataFrame(method, columns=["Item", "Definition"]),
                       r, title="Method")
    ws.column_dimensions["A"].width = 32
    ws.column_dimensions["B"].width = 110
    for col in "CDE":
        ws.column_dimensions[col].width = 16
    for row in ws.iter_rows(min_row=1):
        for cell in row:
            if cell.column == 2 and isinstance(cell.value, str):
                cell.alignment = Alignment(wrap_text=True, vertical="top")

    # ---------------- Summary
    ws = wb.create_sheet("Summary")
    ws.sheet_view.showGridLines = False
    r = write_title(
        ws, 1,
        f"FastPark {cur_lbl} Christmas & New Year - outlook as at "
        f"{as_of:%d %b %Y}",
        f"Bookings made up to {cutoff_txt}. Prior years compared on their "
        f"matched day. Averages and projections: {proj_txt} (years with "
        f"complete movement data).")
    fmts = {"vs ": FMT_DIFF, "Days out": FMT_DAYS}
    base_cols = ["Date", "Holiday", "Days out"]
    for t in TYPES:
        cols = [c for c in an["proj"].columns if c.startswith(t + "|")]
        df = an["proj"][base_cols + cols]
        if df[f"{t}|Actual so far"].isna().all():
            df = df.drop(columns=[f"{t}|Actual so far"])
        r, hdr = write_table(
            ws, df, r, title=t, formats=fmts, scale="div",
            scale_cols=["vs "], total_row=True)
    autosize(ws)
    ws.column_dimensions["A"].width = 14

    # ---------------- Charts
    ws = wb.create_sheet("Charts")
    ws.sheet_view.showGridLines = False
    r = write_title(ws, 1, "Charts", "PNG copies are saved next to the "
                                     "workbook.")
    for p in charts:
        img = XLImage(str(p))
        scale_f = 900 / img.width
        img.width = int(img.width * scale_f)
        img.height = int(img.height * scale_f)
        ws.add_image(img, f"A{r}")
        r += int(img.height / 20) + 3

    # ---------------- Actuals - each year
    ws = wb.create_sheet("Actuals - each year")
    ws.sheet_view.showGridLines = False
    r = write_title(ws, 1, "Actual entries and exits - each year's own window",
                    "Day 1 = the Friday before Christmas. * = after that "
                    "year's Sunday-after-New-Year (shown so all years have 17 "
                    "days).")
    r, _ = write_table(ws, table_actuals_own_window(data["moves"], seasons), r,
                       total_row=True)
    ws.freeze_panes = "B6"
    autosize(ws)

    # ---------------- Actuals - same date
    ws = wb.create_sheet("Actuals - same date")
    ws.sheet_view.showGridLines = False
    r = write_title(ws, 1, "Actual entries and exits - same calendar date",
                    "Holidays line up across years; check the weekday "
                    "column.")
    r, _ = write_table(ws, table_actuals_same_date(data["moves"], seasons,
                                                   an["cur_window"]), r,
                       total_row=True)
    ws.freeze_panes = "C6"
    autosize(ws)

    # ---------------- Matched days
    ws = wb.create_sheet("Matched days")
    ws.sheet_view.showGridLines = False
    r = write_title(ws, 1, "Matched days - the like-for-like comparison",
                    "Major days matched on date; other days on weekday "
                    "(within 3 days, same part of the period).")
    r, _ = write_table(ws, table_matched(match, seasons, current), r,
                       total_row=True)
    ws.freeze_panes = "B6"
    autosize(ws)

    # ---------------- vs Monthly average
    ws = wb.create_sheet("vs Monthly average")
    ws.sheet_view.showGridLines = False
    r = write_title(ws, 1, "Each day against the monthly average",
                    "Blue = busier than an average day that month, red = "
                    "quieter. Prior years on their matched day.")
    mon = an["monthly"].drop(columns=["_season"]).copy()
    mon.columns = ["Season"] + [
        f"{c.split(' ')[-1]}|{' '.join(c.split(' ')[:-1])} daily avg"
        for c in mon.columns[1:]
    ]
    mon = mon[["Season"]
              + [c for c in mon.columns if c.startswith("Entries|")]
              + [c for c in mon.columns if c.startswith("Exits|")]]
    r, _ = write_table(ws, mon, r, title="Daily averages by month",
                       note="Recent 4 wks = " + an["baseline_text"]
                       + ". Blank = month not complete yet.")
    for t in TYPES:
        df = table_by_season(match, seasons, f"vs_month_{t}", current,
                             an["proj_seasons"][t])
        month_override = {
            k: v.get(t) for k, v in THIS_YEAR_MONTHLY_AVG.items()
        }
        if any(v is not None for v in month_override.values()):
            df[f"Implied {cur_lbl} (your monthly avg)"] = [
                (month_override["Dec" if d.month == 12 else "Jan"]
                 * (1 + a))
                if month_override["Dec" if d.month == 12 else "Jan"]
                is not None and pd.notna(a) else np.nan
                for d, a in zip(an["cur_window"], df["Average*"])
            ]
        r, _ = write_table(
            ws, df, r,
            title=f"{t}: % above / below that month's daily average",
            note="e.g. +40% = 40% busier than the average December day "
                 "that year. Blank = movement data incomplete that year.",
            formats={"Implied": FMT_INT, "": FMT_DIFF}, scale="div",
            scale_cols=[season_label(s) for s in prior] + ["Average*"])
    for t in TYPES:
        df = table_by_season(match, seasons, f"vs_recent_{t}", current,
                             an["proj_seasons"][t])
        base_now = an["monthly"].set_index("_season").at[
            current, f"Recent 4 wks {t}"]
        df[f"Implied {cur_lbl}"] = df["Average*"] * base_now
        r, _ = write_table(
            ws, df, r,
            title=f"{t}: multiple of the recent 4-week daily average",
            note=f"This year's recent 4-week average = {base_now:,.0f} per "
                 f"day. Implied = that x Average*.",
            formats={"Implied": FMT_INT, "": FMT_X}, scale="div_x",
            scale_cols=[season_label(s) for s in prior] + ["Average*"])
    autosize(ws)
    ws.column_dimensions["A"].width = 30

    # ---------------- Booking curves
    for t in TYPES:
        word = "entry" if t == "Entries" else "exit"
        ws = wb.create_sheet(f"Booking curves - {t}")
        ws.sheet_view.showGridLines = False
        r = write_title(
            ws, 1, f"When {word} bookings are made",
            "Prior years on their matched day. D28 = % of the day's final "
            "bookings already made 28 days before.")
        r, _ = write_table(
            ws, table_lead_by_season(match, data["leads"], seasons, current, t),
            r, title="Median days booked ahead",
            note="Half the bookings for the day were made at least this many "
                 "days before. Higher = booked earlier.",
            formats={"": FMT_DAYS}, scale="seq")
        r, _ = write_table(
            ws, table_share_booked_early(match, data["bookings_raw"], seasons,
                                         current, t),
            r, title="% of bookings made 28+ days before",
            formats={"": FMT_PCT}, scale="seq")
        avg = table_curve_average(match, data["curves"], an["avg_seasons"], t)
        r, _ = write_table(
            ws, avg, r, title=f"Average ({avg_lbl}): % of final on hand",
            formats={"D": FMT_PCT}, scale="seq",
            scale_cols=[f"D{h}" for h in KEY_HORIZONS])
        for s in sorted(seasons, reverse=True):
            blk = table_curve_block(match, data["curves"], data["leads"], s, t,
                                    current)
            if s == current:
                blk = blk.drop(columns=["Matched day", "Final bookings",
                                        "Actual", "Median days booked ahead"])
                r, _ = write_table(
                    ws, blk, r,
                    title=f"{season_label(s)} (this year): bookings on hand "
                          f"so far (count)",
                    note="Blank = that horizon hasn't been reached yet.",
                    formats={"": FMT_INT})
            else:
                r, _ = write_table(
                    ws, blk, r, title=f"{season_label(s)}: % of final on hand",
                    formats={"D": FMT_PCT, "Median": FMT_DAYS},
                    scale="seq", scale_cols=[f"D{h}" for h in KEY_HORIZONS])
        autosize(ws)
        ws.column_dimensions["A"].width = 30
        ws.column_dimensions["B"].width = 30

    # ---------------- Booking pace
    ws = wb.create_sheet("Booking pace")
    ws.sheet_view.showGridLines = False
    r = write_title(ws, 1, "Booking pace - bookings on hand at the same "
                           "days-out",
                    f"This year = made up to {cutoff_txt}. Prior years = on "
                    f"hand the same number of days before their matched day.")
    for t in TYPES:
        df = table_by_season(match, seasons, f"onhand_{t}", current,
                             an["proj_seasons"][t])
        cur = match[match["season"] == current].sort_values("pos")
        df.insert(1, "Days out", cur["h_now"].values)
        df.insert(2, f"{cur_lbl} now", cur[f"onhand_{t}"].values)
        df["vs Average*"] = df[f"{cur_lbl} now"] / df["Average*"] - 1
        r, _ = write_table(ws, df, r, title=f"{t[:-1] if t == 'Entries' else 'Exit'} "
                                            f"bookings on hand",
                           formats={"Days out": FMT_DAYS, "vs ": FMT_DIFF},
                           scale="div", scale_cols=["vs "], total_row=True)
        df2 = table_by_season(match, seasons, f"pct_final_{t}", current,
                              an["avg_seasons"])
        r, _ = write_table(ws, df2, r,
                           title="...which was this % of those years' final "
                                 "bookings",
                           formats={"": FMT_PCT}, scale="seq")
    autosize(ws)
    ws.column_dimensions["A"].width = 30

    # ---------------- Uplift factors
    ws = wb.create_sheet("Uplift factors")
    ws.sheet_view.showGridLines = False
    r = write_title(ws, 1, "Uplift factors - actual / bookings on hand at "
                           "today's days-out",
                    "1.50x = actuals ended 50% above the bookings on hand at "
                    "this point. Used for the booking pace projection.")
    for t in TYPES:
        df = table_by_season(match, seasons, f"uplift_{t}", current,
                             an["proj_seasons"][t])
        cur = match[match["season"] == current].sort_values("pos")
        df.insert(1, "Days out", cur["h_now"].values)
        r, _ = write_table(ws, df, r, title=t,
                           formats={"Days out": FMT_DAYS, "": FMT_X},
                           scale="seq",
                           scale_cols=[season_label(s) for s in prior]
                           + ["Average*"])
    autosize(ws)
    ws.column_dimensions["A"].width = 30

    # ---------------- Data
    ws = wb.create_sheet("Data - daily")
    r, _ = write_table(ws, table_long_daily(data, seasons), 1)
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    autosize(ws)

    ws = wb.create_sheet("Data - booking curves")
    r, _ = write_table(ws, table_long_curves(data, seasons, current), 1,
                       formats={"%": FMT_PCT, "Days before": FMT_DAYS})
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    autosize(ws)

    for name in wb.sheetnames:
        wb[name].sheet_properties.tabColor = (
            "1C5CAB" if name in ("Read Me", "Summary", "Charts")
            else ("8A8984" if name.startswith("Data") else "86B6EF")
        )
    wb.active = wb.sheetnames.index("Summary")
    wb.save(path)


# =====================================================================
# Main
# =====================================================================
def run(as_of=None, loaders=None, output_dir=OUTPUT_DIR):
    as_of = pd.Timestamp(as_of or AS_OF_DATE or datetime.now().date())
    as_of = as_of.normalize()
    current = season_of(as_of)
    loaders = loaders or (load_daily_movements, load_bookings)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_seasons = list(range(FIRST_SEASON, current + 1))
    data = collect_data(as_of, all_seasons, current, loaders)

    # keep seasons that actually have bookings or movements
    def has_data(s):
        bk = sum(data["curves"][(s, t)]["Final"].sum() for t in TYPES)
        w = christmas_window(s)
        mv = data["moves"].reindex(w).notna().any(axis=None)
        return bk > 0 or (mv and s != current)

    seasons = [s for s in all_seasons if s == current or has_data(s)]
    dropped = [season_label(s) for s in all_seasons if s not in seasons]
    if dropped:
        print(f"No data for: {', '.join(dropped)} (skipped)")

    check_actuals_coverage(data, seasons, current)

    an = build_analysis(data, as_of, seasons, current)
    an["cutoff_text"] = data["cutoff"].strftime("%d %b %Y")

    stamp = datetime.now().strftime("%Y-%m-%d")
    charts = [
        chart_projection(an, current, output_dir / "01_projection.png"),
        chart_pace(an, seasons, current, output_dir / "02_booking_pace.png"),
        chart_actuals(an, seasons, current, output_dir / "03_actuals.png"),
        chart_vs_month(an, seasons, current,
                       output_dir / "04_vs_monthly_average.png"),
        chart_curves(an, data, seasons, current, "Entries",
                     output_dir / "05_booking_curves_entries.png"),
        chart_curves(an, data, seasons, current, "Exits",
                     output_dir / "06_booking_curves_exits.png"),
        chart_lead_heatmap(an, data, seasons, current,
                           output_dir / "07_median_days_booked_ahead.png"),
    ]

    xlsx = output_dir / f"FastPark_Christmas_Analysis_{stamp}.xlsx"
    build_workbook(xlsx, an, data, seasons, current, as_of, charts)

    # console summary
    print("\n" + "=" * 70)
    print(f"{season_label(current)} outlook (bookings to {an['cutoff_text']})")
    print("=" * 70)
    show = an["proj"][[
        "Date", "Holiday", "Days out",
        "Entries|Bookings now", "Entries|vs LY",
        "Entries|Projected (booking pace)",
        "Exits|Bookings now", "Exits|Projected (booking pace)",
    ]].copy()
    show.columns = ["Date", "Holiday", "Out", "Ent bk", "vs LY",
                    "Ent proj", "Exit bk", "Exit proj"]
    with pd.option_context("display.width", 140, "display.max_columns", 20,
                           "display.float_format", "{:,.2f}".format):
        print(show.to_string(index=False))
    print(f"\nSaved workbook: {xlsx}")
    print(f"Charts in:      {output_dir}")
    return xlsx                                                                                                                                                                                                                                             


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--as-of", default=None,
                        help="As-at date YYYY-MM-DD (default today)")
    args = parser.parse_args()
    run(as_of=args.as_of)
