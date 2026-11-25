import pathlib
import sys

import numpy as np
import pandas as pd

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))  # repo root
from modules.paths import FASTPARK_MOVEMENTS_DIR

# ============================================================
# VERSION 3 - MOVES PER WORKING HOUR (5YP PLANNING ASSUMPTION)
# ============================================================
#
# One metric: how many cars a driver moves per hour while working.
#
#   Moves per working hour = adjusted moves / working hours
#
# Adjusted moves:
#   All movements except same-location moves (from == to).
#
# Working hours:
#   Each driver's movements are sorted in time order. Consecutive
#   movements are joined into one "work block" unless the gap between
#   one move ending and the next starting is more than BREAK_GAP_MINUTES.
#   A longer gap is treated as lunch / a break / standby and is excluded.
#   Working hours = sum of (last move end - first move start) per block.
#
#   Short gaps (walking back, keys, getting to the next car) stay in
#   working hours because they are part of moving a car.
#
# Data window:
#   Movement times were logged as single timestamps (no end time) up to
#   Sep 2025, with a mixed month in Oct 2025. From Nov 2025 they are
#   start-end ranges. Gap-based working hours are not comparable across
#   that change, so only data from DATA_START is used. Any month at the
#   end of the data that is not complete is also excluded.
#
#   A minority of rows after Nov 2025 still have a single timestamp.
#   Their end time is imputed as start + the median recorded move duration.
#
# Seasons:
#   Summer = Apr-Oct, Winter = Nov-Mar (e.g. Winter 25/26 = Nov 25 - Mar 26).
#
# Every table is built from totals (moves / hours), so monthly, seasonal,
# hourly and overall figures all tie together. Do not average the rate
# columns in Excel - divide the moves and hours columns instead.
#
# Output:
#   outputs/SR_5YP_Movements.xlsx
# ============================================================


# =========================
# CONFIG
# =========================

INPUT_FILES = {
    "2025": str(FASTPARK_MOVEMENTS_DIR / "movements_2025.csv"),
    "2026": str(FASTPARK_MOVEMENTS_DIR / "movements_YTD_26.csv"),
}

OUTPUT_EXCEL = str(
    pathlib.Path(__file__).resolve().parent / "outputs" / "SR_5YP_Movements.xlsx"
)

BREAK_GAP_MINUTES = 75          # gaps longer than this are not working time
DATA_START = "2025-11-01"       # first month with start-end move times

SUMMER_MONTHS = [4, 5, 6, 7, 8, 9, 10]
HOUR_LABELS = [f"{h:02d}:00" for h in range(24)]


# =========================
# HELPERS
# =========================

def extract_location(x):
    """'Terminal / Zone A' -> 'Terminal'"""
    if pd.isna(x):
        return x
    return str(x).split("/")[0].strip()


def season_label(ts):
    """Apr-Oct -> 'Summer 2026'; Nov-Mar -> 'Winter 25/26'."""
    if ts.month in SUMMER_MONTHS:
        return f"Summer {ts.year}"
    start_year = ts.year if ts.month >= 11 else ts.year - 1
    return f"Winter {start_year % 100:02d}/{(start_year + 1) % 100:02d}"


def season_sort_key(label):
    """Order seasons chronologically (Winter 25/26 starts Nov 2025)."""
    kind, yr = label.split(" ")
    if kind == "Summer":
        return (int(yr), 0)
    return (2000 + int(yr.split("/")[0]), 1)


def load_movements(filepath):
    df = pd.read_csv(filepath, low_memory=False)
    df.columns = df.columns.str.strip().str.lower()

    missing = [c for c in ["date", "time", "driver", "from", "to"] if c not in df.columns]
    if missing:
        raise ValueError(f"{filepath} is missing required columns: {missing}")

    df["same_location"] = df["from"].apply(extract_location) == df["to"].apply(extract_location)

    # Time is either "9:55 - 10:02 (7 mins)" or a single "9:59"
    time_text = df["time"].astype(str).str.strip()
    rng = time_text.str.extract(r"(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})")
    single = time_text.str.extract(r"^(\d{1,2}:\d{2})$")[0]

    df["has_end_time"] = rng[1].notna()
    start_time = rng[0].fillna(single)
    end_time = rng[1].fillna(single)

    date_text = df["date"].astype(str)
    df["start_dt"] = pd.to_datetime(date_text + " " + start_time, dayfirst=True, errors="coerce")
    df["end_dt"] = pd.to_datetime(date_text + " " + end_time, dayfirst=True, errors="coerce")

    # Overnight ranges, e.g. 23:55 - 00:03
    df.loc[df["end_dt"] < df["start_dt"], "end_dt"] += pd.Timedelta(days=1)

    return df[["driver", "start_dt", "end_dt", "has_end_time", "same_location"]]


def allocate_to_clock_hours(blocks):
    """
    Splits each work block's hours across the clock hours it covers.
    e.g. 05:40-07:10 -> 05:00 = 0.33h, 06:00 = 1.0h, 07:00 = 0.17h
    Returns one row per block-hour piece with the season attached.
    """
    pieces = []
    for b in blocks.itertuples(index=False):
        t = b.block_start
        while t < b.block_end:
            nxt = min(t.floor("h") + pd.Timedelta(hours=1), b.block_end)
            pieces.append((b.season, t.hour, (nxt - t).total_seconds() / 3600))
            t = nxt
    return pd.DataFrame(pieces, columns=["season", "hour", "working_hours"])


def add_rate(df):
    df["moves_per_working_hour"] = (df["adjusted_moves"] / df["working_hours"]).round(2)
    df["working_hours"] = df["working_hours"].round(1)
    return df


def format_sheet(writer, sheet_name, df):
    ws = writer.sheets[sheet_name]
    ws.freeze_panes = "A2"
    for col_idx, col_name in enumerate(df.columns, start=1):
        longest = max(len(str(col_name)), df[col_name].astype(str).map(len).max() if len(df) else 0)
        ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(max(longest + 2, 10), 60)


# =========================
# 1. LOAD
# =========================

df = pd.concat([load_movements(f) for f in INPUT_FILES.values()], ignore_index=True)

unparsed = df["start_dt"].isna().sum()
print(f"Rows loaded: {len(df):,}  (unparsed times dropped: {unparsed:,})")
df = df[df["start_dt"].notna()].copy()


# =========================
# 2. DATA WINDOW
# =========================

last_date = df["start_dt"].max().normalize()
last_month = last_date.to_period("M")
if last_date < last_month.end_time.normalize():
    last_month = last_month - 1     # drop incomplete final month

window_start = pd.Period(DATA_START, "M")
df["month"] = df["start_dt"].dt.to_period("M")
df = df[(df["month"] >= window_start) & (df["month"] <= last_month)].copy()

print(f"Data window: {window_start} to {last_month}  (latest data: {last_date.date()})")


# =========================
# 3. IMPUTE MISSING END TIMES
# =========================

recorded = df.loc[df["has_end_time"], "end_dt"] - df.loc[df["has_end_time"], "start_dt"]
median_duration = recorded.median()
df.loc[~df["has_end_time"], "end_dt"] = df.loc[~df["has_end_time"], "start_dt"] + median_duration

print(
    f"Rows with no end time: {(~df['has_end_time']).mean():.1%} "
    f"- imputed median duration {median_duration.total_seconds() / 60:.0f} mins"
)


# =========================
# 4. BUILD WORK BLOCKS
# =========================

df = df.sort_values(["driver", "start_dt", "end_dt"]).reset_index(drop=True)

prev_end = df.groupby("driver")["end_dt"].shift()
df["gap_minutes"] = (df["start_dt"] - prev_end).dt.total_seconds() / 60

df["new_block"] = df["gap_minutes"].isna() | (df["gap_minutes"] > BREAK_GAP_MINUTES)
df["block_id"] = df.groupby("driver")["new_block"].cumsum()
df["adjusted_move"] = ~df["same_location"]

blocks = df.groupby(["driver", "block_id"]).agg(
    block_start=("start_dt", "min"),
    block_end=("end_dt", "max"),
    all_moves=("adjusted_move", "size"),
    adjusted_moves=("adjusted_move", "sum"),
).reset_index()

blocks["working_hours"] = (blocks["block_end"] - blocks["block_start"]).dt.total_seconds() / 3600
blocks["month"] = blocks["block_start"].dt.to_period("M")
blocks["season"] = blocks["block_start"].apply(season_label)

# Moves are attributed to the month/season of their work block, so moves
# and hours always land in the same period.

seasons_in_order = sorted(blocks["season"].unique(), key=season_sort_key)


# =========================
# 5. OVERALL
# =========================

overall = pd.DataFrame([{
    "period": f"{window_start.strftime('%b %Y')} - {last_month.strftime('%b %Y')}",
    "adjusted_moves": blocks["adjusted_moves"].sum(),
    "working_hours": blocks["working_hours"].sum(),
    "all_moves_incl_same_location": blocks["all_moves"].sum(),
}])
overall = add_rate(overall)[[
    "period", "moves_per_working_hour", "adjusted_moves", "working_hours",
    "all_moves_incl_same_location",
]]


# =========================
# 6. SEASONAL
# =========================

seasonal = blocks.groupby("season").agg(
    first_month=("month", "min"),
    last_month=("month", "max"),
    adjusted_moves=("adjusted_moves", "sum"),
    working_hours=("working_hours", "sum"),
).reindex(seasons_in_order).reset_index()

seasonal["months"] = (
    seasonal["first_month"].dt.strftime("%b %y") + " - " + seasonal["last_month"].dt.strftime("%b %y")
)
seasonal["complete_season"] = seasonal.apply(
    lambda r: (r["last_month"] - r["first_month"]).n + 1
    == (len(SUMMER_MONTHS) if r["season"].startswith("Summer") else 12 - len(SUMMER_MONTHS)),
    axis=1,
)
seasonal = add_rate(seasonal)
seasonal["vs_previous_season"] = (
    seasonal["moves_per_working_hour"].pct_change().map(lambda x: "" if pd.isna(x) else f"{x:+.1%}")
)
seasonal = seasonal[[
    "season", "months", "complete_season", "moves_per_working_hour",
    "adjusted_moves", "working_hours", "vs_previous_season",
]]


# =========================
# 7. MONTHLY
# =========================

monthly = blocks.groupby(["month", "season"]).agg(
    adjusted_moves=("adjusted_moves", "sum"),
    working_hours=("working_hours", "sum"),
).reset_index().sort_values("month")

monthly = add_rate(monthly)
monthly["month"] = monthly["month"].dt.strftime("%b %Y")
monthly = monthly[["month", "season", "moves_per_working_hour", "adjusted_moves", "working_hours"]]


# =========================
# 8. HOURLY (overall and by season)
# =========================

hour_pieces = allocate_to_clock_hours(blocks)

# Attach each move's block season so hourly moves match block attribution
block_season = blocks.set_index(["driver", "block_id"])["season"]
adj = df.loc[df["adjusted_move"], ["driver", "block_id", "start_dt"]].copy()
adj["season"] = block_season.reindex(pd.MultiIndex.from_frame(adj[["driver", "block_id"]])).values
adj["hour"] = adj["start_dt"].dt.hour

hourly_moves = adj.groupby(["season", "hour"]).size().rename("adjusted_moves")
hourly_hours = hour_pieces.groupby(["season", "hour"])["working_hours"].sum()
hourly_long = pd.concat([hourly_moves, hourly_hours], axis=1).fillna(0).reset_index()

hourly_all = hourly_long.groupby("hour")[["adjusted_moves", "working_hours"]].sum()
hourly = pd.DataFrame(index=range(24))
hourly.index.name = "hour"
hourly["Overall rate"] = (hourly_all["adjusted_moves"] / hourly_all["working_hours"]).round(2)
for s in seasons_in_order:
    part = hourly_long[hourly_long["season"] == s].set_index("hour").reindex(range(24))
    hourly[f"{s} rate"] = (part["adjusted_moves"] / part["working_hours"]).round(2)
hourly["Overall moves"] = hourly_all["adjusted_moves"].astype(int)
hourly["Overall working hours"] = hourly_all["working_hours"].round(1)
for s in seasons_in_order:
    part = hourly_long[hourly_long["season"] == s].set_index("hour").reindex(range(24))
    hourly[f"{s} working hours"] = part["working_hours"].round(1)
hourly = hourly.reset_index()
hourly["hour"] = hourly["hour"].map(lambda h: HOUR_LABELS[h])


# =========================
# 9. CHECKS (everything ties back to overall)
# =========================

total_moves = overall["adjusted_moves"].iloc[0]
assert monthly["adjusted_moves"].sum() == total_moves
assert seasonal["adjusted_moves"].sum() == total_moves
assert hourly["Overall moves"].sum() == total_moves
assert np.isclose(hour_pieces["working_hours"].sum(), blocks["working_hours"].sum())


# =========================
# 10. NOTES
# =========================

notes = pd.DataFrame({
    "item": [
        "Metric",
        "Adjusted moves",
        "Working hours",
        "Break threshold",
        "Data window",
        "Missing end times",
        "Seasons",
        "Hourly",
        "Totals",
    ],
    "detail": [
        "Moves per working hour = adjusted moves / working hours.",
        "All movements excluding same-location moves (from location == to location).",
        "Each driver's moves are joined into work blocks; a block runs from its first move start to its last move end. "
        "Short gaps between moves (walking back, keys) count as working time.",
        f"A gap of more than {BREAK_GAP_MINUTES} minutes between one move ending and the next starting is treated as "
        "lunch / break / standby and excluded from working hours.",
        f"{window_start.strftime('%b %Y')} - {last_month.strftime('%b %Y')}. Earlier data only has single timestamps "
        "per move, so gaps are not comparable; any incomplete final month is excluded.",
        f"{(~df['has_end_time']).mean():.1%} of rows in the window have no end time; end = start + median recorded "
        f"duration ({median_duration.total_seconds() / 60:.0f} mins).",
        "Summer = Apr-Oct, Winter = Nov-Mar. complete_season = FALSE means the season is only partly in the data.",
        "Working hours are split across the clock hours each block covers; moves are counted by start hour. "
        "Overnight hours have far fewer working hours, so treat them with more caution.",
        "All rates are total moves / total hours. To combine periods in Excel, sum moves and hours and divide - "
        "do not average the rate column.",
    ],
})


# =========================
# 11. WRITE EXCEL
# =========================

sheets = {
    "Notes": notes,
    "Overall": overall,
    "Seasonal": seasonal,
    "Monthly": monthly,
    "Hourly": hourly,
}

with pd.ExcelWriter(OUTPUT_EXCEL, engine="openpyxl") as writer:
    for name, sheet_df in sheets.items():
        sheet_df.to_excel(writer, sheet_name=name, index=False)
        format_sheet(writer, name, sheet_df)

print(f"\nSaved: {OUTPUT_EXCEL}\n")
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
print("=== OVERALL ===");  print(overall.to_string(index=False))
print("\n=== SEASONAL ==="); print(seasonal.to_string(index=False))
print("\n=== MONTHLY ===");  print(monthly.to_string(index=False))
print("\n=== HOURLY ===");   print(hourly.iloc[:, :1 + 1 + len(seasons_in_order)].to_string(index=False))
