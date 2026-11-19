
import sys
import pathlib
from pathlib import Path

# Add parent directory to path so custom modules can be imported
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from modules.utils.query import query
import pandas as pd


# ==========================================================
# CALENDAR SHIFT HELPERS
# ==========================================================
def month_start_end(year: int, month: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(year=year, month=month, day=1)

    if month == 12:
        end_exclusive = pd.Timestamp(year=year + 1, month=1, day=1)
    else:
        end_exclusive = pd.Timestamp(year=year, month=month + 1, day=1)

    return start, end_exclusive


def aligned_month_shift(source_year: int, target_year: int, month: int) -> dict:
    source_start, source_end_exclusive = month_start_end(source_year, month)
    target_start, target_end_exclusive = month_start_end(target_year, month)

    target_first_weekday = target_start.weekday()

    for i in range(7):
        candidate = source_start + pd.Timedelta(days=i)
        if candidate.weekday() == target_first_weekday:
            source_anchor = candidate
            break

    shift_delta = target_start - source_anchor

    required_source_end_exclusive = target_end_exclusive - shift_delta
    extra_days_needed = max(0, (required_source_end_exclusive - source_end_exclusive).days)

    return {
        "source_start": source_start,
        "source_end_exclusive": source_end_exclusive,
        "target_start": target_start,
        "target_end_exclusive": target_end_exclusive,
        "shift_delta": shift_delta,
        "extra_days_needed": extra_days_needed,
    }


def shift_datetime_columns(df, datetime_cols, shift_delta, keep_target_month=None, anchor_col=None):
    out = df.copy()

    for col in datetime_cols:
        if col in df.columns:
            out[f"{col}_source"] = pd.to_datetime(df[col], errors="coerce")
            out[col] = out[f"{col}_source"] + shift_delta

    if keep_target_month is not None:
        target_start, target_end_exclusive = keep_target_month
        out = out[
            out[anchor_col].notna()
            & (out[anchor_col] >= target_start)
            & (out[anchor_col] < target_end_exclusive)
        ].copy()

    return out


def target_day_to_source_day(target_day, source_year):
    target_day = pd.Timestamp(target_day)

    info = aligned_month_shift(source_year, target_day.year, target_day.month)
    shift_delta = info["shift_delta"]

    source_t0 = target_day - shift_delta
    source_t1 = source_t0 + pd.Timedelta(days=1)

    return source_t0, source_t1, info


# ==========================================================
# LOADERS
# ==========================================================
def load_fastpark(start, end):
    return query(
        table="FastPark.v_EntryAndExits",
        columns=[
            "BookingReference AS [Booking ID]",
            "CheckInEnded",
            "ExpectedArrivalDate",
            "ExpectedReturnDate",
            "ActualCheckedOutDate",
        ],
        date_column="CheckInEnded",
        start=start,
        end=end,
    )


def deduplicate_bookings(df):
    df = df.copy()
    df["_creation_dt"] = pd.to_datetime(df["Creation Date"], errors="coerce")
    df["_duration_num"] = pd.to_numeric(df["Duration"], errors="coerce")

    df = (
        df.sort_values(["Booking ID", "_creation_dt", "_duration_num"], ascending=[True, False, False])
        .drop_duplicates(subset=["Booking ID"], keep="first")
        .drop(columns=["_creation_dt", "_duration_num"])
    )
    return df


def deduplicate_actuals(df):
    df = df.copy()
    df["ActualCheckedOutDate"] = pd.to_datetime(df["ActualCheckedOutDate"], errors="coerce")
    df = df.sort_values(["Booking ID", "ActualCheckedOutDate"], ascending=[True, False])
    return df.drop_duplicates(subset=["Booking ID"], keep="first")


def load_fastpark_bookings(start, end, deduplicate=False):
    df = query(
        table="AirportX.v_Bookings",
        columns=[
            "bookingUuid",
            "bookingId AS [Booking ID]",
            "createdAt AS [Creation Date]",
            "entryDate",
            "exitDate",
            "Duration",
        ],
        where=["assetName = 'FastPark'"],
        date_column="entryDate",
        start=start,
        end=end,
    )

    if deduplicate:
        df = deduplicate_bookings(df)

    return df


# ==========================================================
# LANE LOADER (UNCHANGED)
# ==========================================================
def load_lane_movements(path: Path) -> pd.DataFrame:

    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Lane movements file not found: {path}")

    if path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(path, engine="openpyxl")
    else:
        df = pd.read_csv(path)

    required = {"Lane", "Vehicle Reg", "Direction", "Moved At"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Lane movements missing columns: {missing}")

    df = df.copy()

    df["Moved At"] = pd.to_datetime(df["Moved At"], dayfirst=True, errors="coerce")

    df["Direction"] = df["Direction"].astype(str).str.strip().str.upper()
    df["Direction"] = df["Direction"].replace({
        "IN ": "IN",
        "OUT ": "OUT",
        "INBOUND": "IN",
        "OUTBOUND": "OUT"
    })

    df = df.dropna(subset=["Moved At"])
    df["Block"] = df["Lane"].astype(str).str[0]

    return df


# ==========================================================
# FLIGHTS
# ==========================================================
def load_flights(start, end):
    df = query(
        table="EAL.FlightPerformance",
        columns=[
            "FlightID",
            "ScheduledDateTime_Local AS [Scheduled DateTime]",
            "ArrDeptureCode as [Arrival_Departure]",
            "Sector",
            "Passengers",
        ],
        where=["IsPassengerFlight = 1"],
        date_column="ScheduledDateTime_Local",
        start=start,
        end=end,
    )

    df["Scheduled DateTime"] = pd.to_datetime(df["Scheduled DateTime"], errors="coerce")
    return df


def load_flights_as_target_month(source_year, target_year, month):
    info = aligned_month_shift(source_year, target_year, month)

    df = load_flights(
        str(info["source_start"]),
        str(info["source_end_exclusive"] + pd.Timedelta(days=info["extra_days_needed"]))
    )

    df["Scheduled DateTime_source"] = df["Scheduled DateTime"]
    df["Scheduled DateTime"] = df["Scheduled DateTime_source"] + info["shift_delta"]
    df["aligned_date"] = df["Scheduled DateTime"].dt.date

    return df


# ==========================================================
# OCCUPANCY FUNCTIONS (UNCHANGED)
# ==========================================================
def _initial_occupancy_from_first_out(events, group_cols, time_col):
    e = events.sort_values(time_col).copy()

    if group_cols:
        first_dir = (
            e.groupby(group_cols + ["Vehicle Reg"], as_index=False)
             .first()[group_cols + ["Vehicle Reg", "Direction"]]
        )

        return (
            first_dir.assign(is_start=lambda x: x["Direction"].eq("OUT").astype(int))
                     .groupby(group_cols)["is_start"]
                     .sum()
        )
    else:
        first_dir = e.groupby("Vehicle Reg", as_index=False).first()
        return int((first_dir["Direction"] == "OUT").sum())


def build_hourly_occupancy(events, group_cols=None, time_col="Moved At"):
    e = events.copy()
    e[time_col] = pd.to_datetime(e[time_col], errors="coerce")
    e = e.dropna(subset=[time_col]).copy()

    e["Hour Start"] = e[time_col].dt.floor("h")
    e = e.sort_values(time_col)

    gb_cols = (group_cols or []) + ["Hour Start", "Direction"]

    counts = (
        e.groupby(gb_cols)
         .size()
         .unstack(fill_value=0)
         .rename(columns={"IN": "Cars In", "OUT": "Cars Out"})
         .reset_index()
    )

    for col in ["Cars In", "Cars Out"]:
        if col not in counts.columns:
            counts[col] = 0

    counts["Cars In"] = counts["Cars In"].astype(int)
    counts["Cars Out"] = counts["Cars Out"].astype(int)

    counts["Net"] = counts["Cars In"] - counts["Cars Out"]

    init_occ = _initial_occupancy_from_first_out(e, group_cols, time_col)

    if group_cols:
        counts = counts.sort_values(group_cols + ["Hour Start"]).copy()

        counts["Starting Occupancy"] = (
            counts.groupby(group_cols)["Net"].cumsum().shift(fill_value=0)
        )

        addition = counts[group_cols].apply(
            lambda r: init_occ.loc[tuple(r)] if len(group_cols) > 1 else init_occ.loc[r.iloc[0]],
            axis=1
        )

        counts["Starting Occupancy"] = counts["Starting Occupancy"] + addition
    else:
        counts = counts.sort_values(["Hour Start"]).copy()
        counts["Starting Occupancy"] = int(init_occ) + counts["Net"].cumsum().shift(fill_value=0)

    counts["Ending Occupancy"] = counts["Starting Occupancy"] + counts["Net"]
    counts["aligned_date"] = counts["Hour Start"].dt.date

    return counts



# ==========================================================
# PSEUDO-YEAR LOADERS
# ==========================================================
def load_fastpark_bookings_as_target_month(
    source_year: int,
    target_year: int,
    month: int,
    deduplicate: bool = True,
) -> pd.DataFrame:

    info = aligned_month_shift(source_year, target_year, month)

    load_start = info["source_start"]
    load_end = info["source_end_exclusive"] + pd.Timedelta(days=info["extra_days_needed"])

    df = query(
        table="AirportX.v_Bookings",
        columns=[
            "bookingUuid",
            "bookingId AS [Booking ID]",
            "createdAt AS [Creation Date]",
            "entryDate",
            "exitDate",
            "Duration",
        ],
        where=["assetName = 'FastPark'"],
        date_column="entryDate",
        start=str(load_start),
        end=str(load_end),
    )

    if deduplicate:
        df = deduplicate_bookings(df)

    df = shift_datetime_columns(
        df=df,
        datetime_cols=["Creation Date", "entryDate", "exitDate"],
        shift_delta=info["shift_delta"],
        keep_target_month=(info["target_start"], info["target_end_exclusive"]),
        anchor_col="entryDate",
    )

    return df


def load_fastpark_actuals_as_target_month(
    source_year: int,
    target_year: int,
    month: int,
    drop_null_checkout: bool = False,
    deduplicate: bool = True,
) -> pd.DataFrame:

    info = aligned_month_shift(source_year, target_year, month)

    load_start = info["source_start"]
    load_end = info["source_end_exclusive"] + pd.Timedelta(days=info["extra_days_needed"])

    df = query(
        table="FastPark.v_EntryAndExits",
        columns=[
            "BookingReference AS [Booking ID]",
            "CheckInEnded",
            "ExpectedArrivalDate",
            "ExpectedReturnDate",
            "ActualCheckedOutDate",
        ],
        date_column="CheckInEnded",
        start=str(load_start),
        end=str(load_end),
    )

    if deduplicate:
        df = deduplicate_actuals(df)

    if drop_null_checkout:
        df = df.dropna(subset=["ActualCheckedOutDate"]).copy()

    df = shift_datetime_columns(
        df=df,
        datetime_cols=[
            "CheckInEnded",
            "ExpectedArrivalDate",
            "ExpectedReturnDate",
            "ActualCheckedOutDate",
        ],
        shift_delta=info["shift_delta"],
        keep_target_month=(info["target_start"], info["target_end_exclusive"]),
        anchor_col="CheckInEnded",
    )

    return df


def load_lane_movements_as_target_month(
    path: Path,
    source_year: int,
    target_year: int,
    month: int,
) -> pd.DataFrame:

    info = aligned_month_shift(source_year, target_year, month)

    df = load_lane_movements(path)

    source_keep_end = info["source_end_exclusive"] + pd.Timedelta(days=info["extra_days_needed"])

    df = df[
        df["Moved At"].notna()
        & (df["Moved At"] >= info["source_start"])
        & (df["Moved At"] < source_keep_end)
    ].copy()

    df = shift_datetime_columns(
        df=df,
        datetime_cols=["Moved At"],
        shift_delta=info["shift_delta"],
        keep_target_month=(info["target_start"], info["target_end_exclusive"]),
        anchor_col="Moved At",
    )

    return df


# ==========================================================
# MAIN SCRIPT
# ==========================================================

sep_2025_bookings_df = load_fastpark_bookings_as_target_month(2024, 2025, 9)
sep_2025_actuals_df = load_fastpark_actuals_as_target_month(2024, 2025, 9, drop_null_checkout=True)


aug_to_oct_2025_bookings_df = pd.concat([
    load_fastpark_bookings_as_target_month(2024, 2025, 8),
    load_fastpark_bookings_as_target_month(2024, 2025, 9),
    load_fastpark_bookings_as_target_month(2024, 2025, 10)
], ignore_index=True)

aug_to_oct_2026_bookings_df = pd.concat([
    load_fastpark_bookings_as_target_month(2025, 2026, 8),
    load_fastpark_bookings_as_target_month(2025, 2026, 9),
    load_fastpark_bookings_as_target_month(2025, 2026, 10)
], ignore_index=True)


sep_2026_bookings_df = load_fastpark_bookings_as_target_month(2025, 2026, 9)
sep_2026_actuals_df = load_fastpark_actuals_as_target_month(2025, 2026, 9, drop_null_checkout=True)

sep_2025_fp_merged = pd.merge(sep_2025_actuals_df, sep_2025_bookings_df, on="Booking ID", how="outer")
sep_2026_fp_merged = pd.merge(sep_2026_actuals_df, sep_2026_bookings_df, on="Booking ID", how="outer")


# ✅ FLIGHTS ADDED
flights_2025_df = load_flights_as_target_month(2024, 2025, 9)
flights_2026_df = pd.concat([
    load_flights_as_target_month(2025, 2026, 8),
    load_flights_as_target_month(2025, 2026, 9)
], ignore_index=True)


# ==========================================================
# LANE MODEL (REAL DATA)
# ==========================================================
lane_movements_path = Path(__file__).resolve().parents[1] / "inputs" / "lane_movements_week.xlsx"

lane_events_df = load_lane_movements(lane_movements_path)

block_hourly_df = build_hourly_occupancy(
    lane_events_df,
    group_cols=None,
    time_col="Moved At"
)

lane_hourly_df = build_hourly_occupancy(
    lane_events_df,
    group_cols=["Lane"],
    time_col="Moved At"
)

# ✅ FIXED NAMES
block_hourly_df["aligned_date"] = block_hourly_df["Hour Start"].dt.date
lane_hourly_df["aligned_date"] = lane_hourly_df["Hour Start"].dt.date



# ==========================================================
# DAILY SYSTEM OCCUPANCY (HOURLY VERSION)
# ==========================================================
# We define the TARGET day we care about.
# The aligned source day is then derived automatically based on weekday alignment.

target_day = pd.Timestamp("2025-09-27 00:00:00")

# Example:
# Build pseudo current system for 2025-09-27 using aligned Sep 2024 day
source_t0, source_t1, shift_info = target_day_to_source_day(target_day, source_year=2024)

# ✅ FIX: override to REAL day (no pseudo alignment)
source_t0 = target_day
source_t1 = target_day + pd.Timedelta(days=1)

# These are the target-day timestamps that will appear in final output
t0 = target_day
t1 = t0 + pd.Timedelta(days=1)


# ----------------------------------------------------------
# STEP 1: Starting occupancy at midnight
# ----------------------------------------------------------

starting_raw_df = query(
    table="FastPark.v_EntryAndExits",
    columns=[
        "BookingReference AS [Booking ID]",
        "CheckInEnded",
        "ActualCheckedOutDate"
    ],
    where=[
        f"CheckInEnded <= '{source_t0}'"
    ]
)

starting_raw_df = deduplicate_actuals(starting_raw_df)

starting_df = starting_raw_df[
    starting_raw_df["ActualCheckedOutDate"].isna() |
    (pd.to_datetime(starting_raw_df["ActualCheckedOutDate"], errors="coerce") > source_t0)
].copy()

starting_occupancy = len(starting_df)

print("Target day:", t0.date())
print("Aligned source day used:", source_t0.date())
print("Starting raw rows after dedupe:", len(starting_raw_df))
print("Starting active at midnight:", starting_occupancy)


# ----------------------------------------------------------
# STEP 2: Cars entering during the day
# ----------------------------------------------------------
cars_in_df = query(
    table="FastPark.v_EntryAndExits",
    columns=[
        "BookingReference AS [Booking ID]",
        "CheckInEnded",
        "ActualCheckedOutDate"
    ],
    date_column="CheckInEnded",
    start=str(source_t0),
    end=str(source_t1)
)

cars_in_df = deduplicate_actuals(cars_in_df)

# ✅ FIX: DO NOT APPLY SHIFT
cars_in_df["CheckInEnded_source"] = pd.to_datetime(cars_in_df["CheckInEnded"], errors="coerce")
cars_in_df["CheckInEnded"] = cars_in_df["CheckInEnded_source"]

cars_in_df["Hour of Day"] = cars_in_df["CheckInEnded"].dt.floor("h")

cars_in_hourly = (
    cars_in_df.groupby("Hour of Day")
    .size()
    .reset_index(name="Cars In")
)

print(
    cars_in_df[[
        "CheckInEnded_source",
        "CheckInEnded"
    ]].head(5)
)


# ----------------------------------------------------------
# STEP 3: Cars leaving during the day
# ----------------------------------------------------------
cars_out_df = query(
    table="FastPark.v_EntryAndExits",
    columns=[
        "BookingReference AS [Booking ID]",
        "CheckInEnded",
        "ActualCheckedOutDate"
    ],
    date_column="ActualCheckedOutDate",
    start=str(source_t0),
    end=str(source_t1)
)

cars_out_df = deduplicate_actuals(cars_out_df)

# ✅ FIX: DO NOT APPLY SHIFT
cars_out_df["ActualCheckedOutDate_source"] = pd.to_datetime(cars_out_df["ActualCheckedOutDate"], errors="coerce")
cars_out_df["ActualCheckedOutDate"] = cars_out_df["ActualCheckedOutDate_source"]

cars_out_df["Hour of Day"] = cars_out_df["ActualCheckedOutDate"].dt.floor("h")

cars_out_hourly = (
    cars_out_df.groupby("Hour of Day")
    .size()
    .reset_index(name="Cars Out")
)


# ----------------------------------------------------------
# STEP 4: Create full 24-hour scaffold
# ----------------------------------------------------------
all_hours = pd.DataFrame({
    "Hour of Day": pd.date_range(start=t0, end=t1 - pd.Timedelta(hours=1), freq="h")
})


# ----------------------------------------------------------
# STEP 5: Merge hourly in/out
# ----------------------------------------------------------
current_system = (
    all_hours
    .merge(cars_in_hourly, on="Hour of Day", how="left")
    .merge(cars_out_hourly, on="Hour of Day", how="left")
    .fillna(0)
)

current_system["Cars In"] = current_system["Cars In"].astype(int)
current_system["Cars Out"] = current_system["Cars Out"].astype(int)


# ----------------------------------------------------------
# STEP 6: Build occupancy
# ----------------------------------------------------------
current_system["Net"] = current_system["Cars In"] - current_system["Cars Out"]

current_system["Starting Occupancy"] = (
    starting_occupancy + current_system["Net"].cumsum().shift(fill_value=0)
)

current_system["Ending Occupancy"] = (
    current_system["Starting Occupancy"] + current_system["Net"]
)

current_system["aligned_date"] = current_system["Hour of Day"].dt.date
current_system["source_day"] = source_t0.date()


# ----------------------------------------------------------
# STEP 7: Final columns
# ----------------------------------------------------------
current_system = current_system[[
    "Hour of Day",
    "aligned_date",
    "source_day",
    "Starting Occupancy",
    "Cars In",
    "Cars Out",
    "Ending Occupancy"
]]

print("Starting occupancy used in hourly table:", starting_occupancy)
print(current_system.head(5))



# ==========================================================
# EXPORT
# ==========================================================
def drop_source_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df[[c for c in df.columns if not c.endswith("_source")]].copy()


sep_2025_fp_merged_clean = drop_source_columns(sep_2025_fp_merged)
sep_2026_fp_merged_clean = drop_source_columns(sep_2026_fp_merged)

sep_2025_bookings_clean = drop_source_columns(sep_2025_bookings_df)
sep_2026_bookings_clean = drop_source_columns(sep_2026_bookings_df)

block_hourly_clean = drop_source_columns(block_hourly_df)
lane_hourly_clean = drop_source_columns(lane_hourly_df)

current_system_clean = drop_source_columns(current_system)


clean_output_path = Path(__file__).resolve().parents[1] / "outputs" / "fastpark_msc_vol3.xlsx"

with pd.ExcelWriter(clean_output_path, engine="openpyxl", mode="w") as writer:
    sep_2025_fp_merged_clean.to_excel(writer, sheet_name="Pseudo Sep2025 actuals+book", index=False)
    sep_2026_fp_merged_clean.to_excel(writer, sheet_name="Pseudo Sep2026 actuals+book", index=False)
    sep_2025_bookings_clean.to_excel(writer, sheet_name="Pseudo Sep2025 bookings", index=False)
    sep_2026_bookings_clean.to_excel(writer, sheet_name="Pseudo Sep2026 bookings", index=False)
    block_hourly_clean.to_excel(writer, sheet_name="Pseudo Sep2025 block occ", index=False)
    lane_hourly_clean.to_excel(writer, sheet_name="Pseudo Sep2025 lane occ", index=False)
    current_system_clean.to_excel(writer, sheet_name="Pseudo current_system", index=False)

    # ✅ flights
    drop_source_columns(flights_2025_df).to_excel(writer, sheet_name="Pseudo Sep2025 flights", index=False)
    drop_source_columns(flights_2026_df).to_excel(writer, sheet_name="Pseudo Sep2026 flights", index=False)

debug_output_path = Path(__file__).resolve().parents[1] / "outputs" / "fastpark_msc_vol3.xlsx"

with pd.ExcelWriter(debug_output_path, engine="openpyxl", mode="w") as writer:
    sep_2025_fp_merged.to_excel(writer, sheet_name="Pseudo Sep2025 actuals+book", index=False)
    sep_2026_fp_merged.to_excel(writer, sheet_name="Pseudo Sep2026 actuals+book", index=False)
    sep_2025_bookings_df.to_excel(writer, sheet_name="Pseudo Sep2025 bookings", index=False)
    sep_2026_bookings_df.to_excel(writer, sheet_name="Pseudo Sep2026 bookings", index=False)
    aug_to_oct_2025_bookings_df.to_excel(writer, sheet_name="Pseudo Aug-Oct 2025 bookings", index=False)
    aug_to_oct_2026_bookings_df.to_excel(writer, sheet_name="Pseudo Aug-Oct 2026 bookings", index=False)
    block_hourly_df.to_excel(writer, sheet_name="Pseudo Sep2025 block occ", index=False)
    lane_hourly_df.to_excel(writer, sheet_name="Pseudo Sep2025 lane occ", index=False)
    current_system.to_excel(writer, sheet_name="Pseudo current_system", index=False)

    flights_2025_df.to_excel(writer, sheet_name="Pseudo Sep2025 flights", index=False)
    flights_2026_df.to_excel(writer, sheet_name="Pseudo Sep2026 flights", index=False)


print(f"Saved CLEAN Excel: {clean_output_path}")
print(f"Saved DEBUG Excel: {debug_output_path}")
