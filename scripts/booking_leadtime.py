
import sys
import pathlib
from pathlib import Path

# Add parent directory to path so custom modules can be imported
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from modules.utils.query import query
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def load_fastpark_bookings(start, end, deduplicate=False):
    """
    Load FastPark bookings from AirportX.v_Bookings.
    Pull by entryDate window.
    """
    df = query(
        table="AirportX.v_Bookings",
        columns=[
            "bookingUuid",
            "bookingId AS [Booking ID]",
            "createdAt AS [Creation Date]",
            "entryDate",
            "exitDate",
            "Duration",
            "status",
        ],
        where=[
            "assetName = 'FastPark'",
            "status = 'B'"
        ],
        date_column="entryDate",
        start=start,
        end=end,
    )

    df = pd.DataFrame(df).copy()

    # Convert date columns
    for col in ["Creation Date", "entryDate", "exitDate"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Convert duration
    df["Duration"] = pd.to_numeric(df["Duration"], errors="coerce")

    # Optional deduplication
    if deduplicate and "bookingUuid" in df.columns:
        df = (
            df.sort_values(["bookingUuid", "Creation Date"])
              .drop_duplicates(subset=["bookingUuid"], keep="first")
              .reset_index(drop=True)
        )

    return df


def prepare_fastpark_lead_time(df):
    """
    Enrich booking data with lead-time metrics and useful buckets.
    """
    df = df.copy()

    # Remove rows with missing critical fields
    df = df.dropna(subset=["Creation Date", "entryDate", "exitDate"])

    # Exact lead times
    df["lead_to_entry_days_exact"] = (
        (df["entryDate"] - df["Creation Date"]).dt.total_seconds() / 86400
    )
    df["lead_to_exit_days_exact"] = (
        (df["exitDate"] - df["Creation Date"]).dt.total_seconds() / 86400
    )

    # Hours are useful for last-minute operational analysis
    df["lead_to_entry_hours"] = (
        (df["entryDate"] - df["Creation Date"]).dt.total_seconds() / 3600
    )
    df["lead_to_exit_hours"] = (
        (df["exitDate"] - df["Creation Date"]).dt.total_seconds() / 3600
    )

    # Integer lead times
    df["lead_to_entry_days_floor"] = np.floor(df["lead_to_entry_days_exact"]).astype("Int64")
    df["lead_to_exit_days_floor"] = np.floor(df["lead_to_exit_days_exact"]).astype("Int64")

    # Calendar-date based lead time
    df["lead_to_entry_calendar_days"] = (
        df["entryDate"].dt.normalize() - df["Creation Date"].dt.normalize()
    ).dt.days
    df["lead_to_exit_calendar_days"] = (
        df["exitDate"].dt.normalize() - df["Creation Date"].dt.normalize()
    ).dt.days

    # Invalid timings
    df["invalid_entry_lead"] = df["lead_to_entry_hours"] < 0
    df["invalid_exit_lead"] = df["lead_to_exit_hours"] < 0

    # Duration cleanup
    inferred_duration_days = (
        (df["exitDate"].dt.normalize() - df["entryDate"].dt.normalize()).dt.days
    )

    df["Duration_clean"] = df["Duration"]
    df.loc[df["Duration_clean"].isna(), "Duration_clean"] = inferred_duration_days[df["Duration_clean"].isna()]
    df["Duration_clean"] = pd.to_numeric(df["Duration_clean"], errors="coerce")

    # Daily date columns
    df["creation_day"] = df["Creation Date"].dt.normalize()
    df["entry_day"] = df["entryDate"].dt.normalize()
    df["exit_day"] = df["exitDate"].dt.normalize()

    # Duration bucket
    def duration_bucket(x):
        if pd.isna(x):
            return "Unknown"
        elif x == 1:
            return "1 day"
        elif x == 2:
            return "2 days"
        elif 3 <= x <= 6:
            return "3-6 days"
        elif 7 <= x <= 9:
            return "7-9 days"
        elif 10 <= x <= 13:
            return "10-13 days"
        elif 14 <= x <= 20:
            return "14-20 days"
        elif 21 <= x <= 29:
            return "21-29 days"
        elif 30 <= x <= 59:
            return "30-59 days"
        elif 60 <= x <= 89:
            return "60-89 days"
        else:
            return "90+ days"

    df["duration_bucket"] = df["Duration_clean"].apply(duration_bucket)

    # Lead-time buckets focused on operations
    lead_bins = [-np.inf, 0, 1, 2, 3, 7, 14, 30, 60, 90, np.inf]
    lead_labels = [
        "Same day or late",
        "1 day",
        "2 days",
        "3 days",
        "4-7 days",
        "8-14 days",
        "15-30 days",
        "31-60 days",
        "61-90 days",
        "90+ days",
    ]

    df["entry_lead_bucket"] = pd.cut(
        df["lead_to_entry_calendar_days"],
        bins=lead_bins,
        labels=lead_labels,
        right=True,
        include_lowest=True,
    )

    df["exit_lead_bucket"] = pd.cut(
        df["lead_to_exit_calendar_days"],
        bins=lead_bins,
        labels=lead_labels,
        right=True,
        include_lowest=True,
    )

    # Last-minute flags for entry
    df["booked_within_24h_of_entry"] = df["lead_to_entry_hours"] <= 24
    df["booked_within_48h_of_entry"] = df["lead_to_entry_hours"] <= 48
    df["booked_within_72h_of_entry"] = df["lead_to_entry_hours"] <= 72
    df["booked_within_7d_of_entry"] = df["lead_to_entry_days_exact"] <= 7

    # Last-minute flags for exit
    df["booked_within_24h_of_exit"] = df["lead_to_exit_hours"] <= 24
    df["booked_within_48h_of_exit"] = df["lead_to_exit_hours"] <= 48
    df["booked_within_72h_of_exit"] = df["lead_to_exit_hours"] <= 72
    df["booked_within_7d_of_exit"] = df["lead_to_exit_days_exact"] <= 7

    return df.reset_index(drop=True)


def summarize_fastpark_lead_time(df):
    """
    Produce headline summary tables.
    """
    usable = df.loc[~df["invalid_entry_lead"] & ~df["invalid_exit_lead"]].copy()

    overall_summary = pd.DataFrame({
        "metric": [
            "total_bookings",
            "median_lead_to_entry_days",
            "mean_lead_to_entry_days",
            "median_lead_to_exit_days",
            "mean_lead_to_exit_days",
            "pct_booked_within_24h_of_entry",
            "pct_booked_within_48h_of_entry",
            "pct_booked_within_72h_of_entry",
            "pct_booked_within_7d_of_entry",
            "pct_booked_within_24h_of_exit",
            "pct_booked_within_48h_of_exit",
            "pct_booked_within_72h_of_exit",
            "pct_booked_within_7d_of_exit",
        ],
        "value": [
            len(usable),
            usable["lead_to_entry_days_exact"].median(),
            usable["lead_to_entry_days_exact"].mean(),
            usable["lead_to_exit_days_exact"].median(),
            usable["lead_to_exit_days_exact"].mean(),
            usable["booked_within_24h_of_entry"].mean() * 100,
            usable["booked_within_48h_of_entry"].mean() * 100,
            usable["booked_within_72h_of_entry"].mean() * 100,
            usable["booked_within_7d_of_entry"].mean() * 100,
            usable["booked_within_24h_of_exit"].mean() * 100,
            usable["booked_within_48h_of_exit"].mean() * 100,
            usable["booked_within_72h_of_exit"].mean() * 100,
            usable["booked_within_7d_of_exit"].mean() * 100,
        ]
    })

    duration_distribution = (
        usable["duration_bucket"]
        .value_counts(dropna=False)
        .rename_axis("duration_bucket")
        .reset_index(name="bookings")
    )
    duration_distribution["pct"] = duration_distribution["bookings"] / duration_distribution["bookings"].sum() * 100

    by_duration = (
        usable.groupby("duration_bucket", dropna=False)
        .agg(
            bookings=("bookingUuid", "count"),
            median_lead_to_entry_days=("lead_to_entry_days_exact", "median"),
            mean_lead_to_entry_days=("lead_to_entry_days_exact", "mean"),
            median_lead_to_exit_days=("lead_to_exit_days_exact", "median"),
            mean_lead_to_exit_days=("lead_to_exit_days_exact", "mean"),
            pct_within_24h_entry=("booked_within_24h_of_entry", lambda s: s.mean() * 100),
            pct_within_48h_entry=("booked_within_48h_of_entry", lambda s: s.mean() * 100),
            pct_within_72h_entry=("booked_within_72h_of_entry", lambda s: s.mean() * 100),
            pct_within_7d_entry=("booked_within_7d_of_entry", lambda s: s.mean() * 100),
            pct_within_24h_exit=("booked_within_24h_of_exit", lambda s: s.mean() * 100),
            pct_within_48h_exit=("booked_within_48h_of_exit", lambda s: s.mean() * 100),
            pct_within_72h_exit=("booked_within_72h_of_exit", lambda s: s.mean() * 100),
            pct_within_7d_exit=("booked_within_7d_of_exit", lambda s: s.mean() * 100),
        )
        .reset_index()
        .sort_values("bookings", ascending=False)
    )

    entry_lead_distribution = (
        usable["entry_lead_bucket"]
        .value_counts(dropna=False, sort=False)
        .rename_axis("entry_lead_bucket")
        .reset_index(name="bookings")
    )
    entry_lead_distribution["pct"] = entry_lead_distribution["bookings"] / entry_lead_distribution["bookings"].sum() * 100

    exit_lead_distribution = (
        usable["exit_lead_bucket"]
        .value_counts(dropna=False, sort=False)
        .rename_axis("exit_lead_bucket")
        .reset_index(name="bookings")
    )
    exit_lead_distribution["pct"] = exit_lead_distribution["bookings"] / exit_lead_distribution["bookings"].sum() * 100

    duration_x_entry_lead = pd.crosstab(
        usable["duration_bucket"],
        usable["entry_lead_bucket"],
        margins=True
    )

    duration_x_exit_lead = pd.crosstab(
        usable["duration_bucket"],
        usable["exit_lead_bucket"],
        margins=True
    )

    short_stays = usable[usable["Duration_clean"].isin([1, 2])].copy()

    one_two_day_summary = (
        short_stays.groupby("Duration_clean", dropna=False)
        .agg(
            bookings=("bookingUuid", "count"),
            median_lead_to_entry_days=("lead_to_entry_days_exact", "median"),
            mean_lead_to_entry_days=("lead_to_entry_days_exact", "mean"),
            median_lead_to_exit_days=("lead_to_exit_days_exact", "median"),
            mean_lead_to_exit_days=("lead_to_exit_days_exact", "mean"),
            pct_within_24h_entry=("booked_within_24h_of_entry", lambda s: s.mean() * 100),
            pct_within_48h_entry=("booked_within_48h_of_entry", lambda s: s.mean() * 100),
            pct_within_72h_entry=("booked_within_72h_of_entry", lambda s: s.mean() * 100),
            pct_within_24h_exit=("booked_within_24h_of_exit", lambda s: s.mean() * 100),
            pct_within_48h_exit=("booked_within_48h_of_exit", lambda s: s.mean() * 100),
            pct_within_72h_exit=("booked_within_72h_of_exit", lambda s: s.mean() * 100),
        )
        .reset_index()
        .rename(columns={"Duration_clean": "duration_days"})
    )

    return {
        "overall_summary": overall_summary,
        "duration_distribution": duration_distribution,
        "by_duration": by_duration,
        "entry_lead_distribution": entry_lead_distribution,
        "exit_lead_distribution": exit_lead_distribution,
        "duration_x_entry_lead": duration_x_entry_lead,
        "duration_x_exit_lead": duration_x_exit_lead,
        "one_two_day_summary": one_two_day_summary,
    }


def analyse_last_minute_daily_pattern(df):
    """
    Analyse last month of bookings:
    - Day-by-day (by entry date)
    - Only bookings made 1–2 days before entry
    - Split by duration
    """
    usable = df.loc[~df["invalid_entry_lead"]].copy()

    max_date = usable["entryDate"].max().normalize()
    start_last_month = (max_date - pd.DateOffset(days=30)).normalize()

    last_month_df = usable[
        (usable["entryDate"] >= start_last_month) &
        (usable["entryDate"] <= max_date)
    ].copy()

    last_minute_df = last_month_df[
        last_month_df["lead_to_entry_calendar_days"].isin([1, 2])
    ].copy()

    last_month_df["entry_day"] = last_month_df["entryDate"].dt.normalize()
    last_minute_df["entry_day"] = last_minute_df["entryDate"].dt.normalize()

    daily_total_bookings = (
        last_month_df
        .groupby("entry_day")
        .agg(total_bookings=("bookingUuid", "count"))
        .reset_index()
    )

    daily_last_minute = (
        last_minute_df
        .groupby("entry_day")
        .agg(last_minute_bookings=("bookingUuid", "count"))
        .reset_index()
    )

    daily_summary = daily_total_bookings.merge(
        daily_last_minute,
        on="entry_day",
        how="left"
    )

    daily_summary["last_minute_bookings"] = daily_summary["last_minute_bookings"].fillna(0)
    daily_summary["pct_last_minute"] = (
        daily_summary["last_minute_bookings"] / daily_summary["total_bookings"] * 100
    )

    duration_breakdown = (
        last_minute_df
        .groupby(["entry_day", "duration_bucket"])
        .agg(bookings=("bookingUuid", "count"))
        .reset_index()
    )

    duration_pivot = duration_breakdown.pivot_table(
        index="entry_day",
        columns="duration_bucket",
        values="bookings",
        fill_value=0
    ).reset_index()

    short_stay_breakdown = (
        last_minute_df[last_minute_df["Duration_clean"].isin([1, 2])]
        .groupby(["entry_day", "Duration_clean"])
        .agg(bookings=("bookingUuid", "count"))
        .reset_index()
        .pivot_table(
            index="entry_day",
            columns="Duration_clean",
            values="bookings",
            fill_value=0
        )
        .reset_index()
        .rename(columns={1: "1_day_bookings", 2: "2_day_bookings"})
    )

    final_daily = daily_summary.merge(duration_pivot, on="entry_day", how="left")
    final_daily = final_daily.merge(short_stay_breakdown, on="entry_day", how="left")
    final_daily = final_daily.fillna(0).sort_values("entry_day")

    return {
        "daily_summary": daily_summary,
        "duration_breakdown": duration_pivot,
        "short_stay_breakdown": short_stay_breakdown,
        "final_daily": final_daily
    }


def load_fastpark_actuals(start, end):
    """
    Load actual FastPark entries/exits for the same service date window.
    """
    cols = [
        "BookingReference",
        "CheckInStarted",
        "ActualCheckedOutDate",
    ]

    entry_df = query(
        table="FastPark.v_EntryAndExits",
        columns=cols,
        where=[],
        date_column="CheckInStarted",
        start=start,
        end=end,
    )
    entry_df = pd.DataFrame(entry_df).copy()

    exit_df = query(
        table="FastPark.v_EntryAndExits",
        columns=cols,
        where=[],
        date_column="ActualCheckedOutDate",
        start=start,
        end=end,
    )
    exit_df = pd.DataFrame(exit_df).copy()

    if not entry_df.empty and "CheckInStarted" in entry_df.columns:
        entry_df["CheckInStarted"] = pd.to_datetime(entry_df["CheckInStarted"], errors="coerce")
    if not exit_df.empty and "ActualCheckedOutDate" in exit_df.columns:
        exit_df["ActualCheckedOutDate"] = pd.to_datetime(exit_df["ActualCheckedOutDate"], errors="coerce")

    actual_entries = entry_df.dropna(subset=["BookingReference", "CheckInStarted"]).copy()
    actual_entries = actual_entries.drop_duplicates(subset=["BookingReference"])
    actual_entries["entry_day"] = actual_entries["CheckInStarted"].dt.normalize()

    actual_exits = exit_df.dropna(subset=["BookingReference", "ActualCheckedOutDate"]).copy()
    actual_exits = actual_exits.drop_duplicates(subset=["BookingReference"])
    actual_exits["exit_day"] = actual_exits["ActualCheckedOutDate"].dt.normalize()

    return actual_entries, actual_exits


def build_creation_entry_duration_uplift(bookings_df, actual_entries_df, actual_exits_df, horizons=range(1, 15)):
    """
    Core logic:
    - Anchor late-booking behaviour on CREATION DATE relative to ENTRY DATE
    - Use DURATION / booked EXIT DATE to imply future exits from those same bookings
    - Compare both entries and implied exits to actuals

    Outputs:
    1. Entries: % of actual entries covered by bookings created by D1-D14 before entry
    2. Implied exits: % of actual exits covered by those same bookings
    3. Entry uplift arrays
    4. Exit uplift arrays
    5. Duration-specific visibility matrices for entry-side behaviour
    """
    bookings = bookings_df.copy()

    # Clean usable bookings
    bookings = bookings.loc[
        (~bookings["invalid_entry_lead"]) &
        (~bookings["invalid_exit_lead"])
    ].copy()

    # Final booked counts
    final_booked_entries = (
        bookings.groupby("entry_day")["bookingUuid"]
        .nunique()
        .rename("final_booked_entries")
    )

    final_booked_exits = (
        bookings.groupby("exit_day")["bookingUuid"]
        .nunique()
        .rename("final_booked_exits")
    )

    # Actual counts
    actual_entry_counts = (
        actual_entries_df.groupby("entry_day")["BookingReference"]
        .nunique()
        .rename("actual_entries")
    )

    actual_exit_counts = (
        actual_exits_df.groupby("exit_day")["BookingReference"]
        .nunique()
        .rename("actual_exits")
    )

    entry_daily_frames = []
    implied_exit_daily_frames = []

    entry_pct_actual_avg = {}
    entry_pct_actual_median = {}
    implied_exit_pct_actual_avg = {}
    implied_exit_pct_actual_median = {}

    entry_uplift_avg = {}
    entry_uplift_median = {}
    implied_exit_uplift_avg = {}
    implied_exit_uplift_median = {}

    # Duration-specific entry visibility to final bookings
    duration_entry_visible_avg = {}
    duration_entry_visible_median = {}

    duration_order = [
        "1 day", "2 days", "3-6 days", "7-9 days", "10-13 days",
        "14-20 days", "21-29 days", "30-59 days", "60-89 days", "90+ days", "Unknown"
    ]

    final_entry_by_duration = (
        bookings.groupby(["entry_day", "duration_bucket"])["bookingUuid"]
        .nunique()
        .rename("final_entry_duration_bookings")
        .reset_index()
    )

    for h in horizons:
        cutoff_label = f"D{h}"

        # -------------------------------------------------
        # BOOKINGS CREATED BY Dn BEFORE ENTRY
        # -------------------------------------------------
        seen = bookings.loc[
            bookings["creation_day"] <= (bookings["entry_day"] - pd.Timedelta(days=h))
        ].copy()

        # -------------------------
        # Entry-side counts from seen bookings
        # -------------------------
        seen_entries = (
            seen.groupby("entry_day")["bookingUuid"]
            .nunique()
            .rename("seen_entries")
        )

        entry_compare = pd.concat(
            [final_booked_entries, actual_entry_counts, seen_entries],
            axis=1
        ).fillna(0)

        entry_compare["horizon"] = cutoff_label
        entry_compare["pct_of_actuals"] = np.where(
            entry_compare["actual_entries"] > 0,
            entry_compare["seen_entries"] / entry_compare["actual_entries"] * 100,
            np.nan
        )
        entry_compare["pct_of_final_bookings"] = np.where(
            entry_compare["final_booked_entries"] > 0,
            entry_compare["seen_entries"] / entry_compare["final_booked_entries"] * 100,
            np.nan
        )
        entry_compare["uplift_to_actual_pct_raw"] = np.where(
            entry_compare["seen_entries"] > 0,
            (entry_compare["actual_entries"] - entry_compare["seen_entries"]) / entry_compare["seen_entries"] * 100,
            np.nan
        )
        entry_compare["uplift_to_actual_pct_clipped"] = entry_compare["uplift_to_actual_pct_raw"].clip(lower=0)
        entry_compare = entry_compare.reset_index().rename(columns={"entry_day": "service_day"})
        entry_daily_frames.append(entry_compare)

        # -------------------------
        # IMPLIED EXITS FROM THOSE SAME BOOKINGS
        # -------------------------
        implied_exits = (
            seen.groupby("exit_day")["bookingUuid"]
            .nunique()
            .rename("implied_exits_from_seen_bookings")
        )

        implied_exit_compare = pd.concat(
            [final_booked_exits, actual_exit_counts, implied_exits],
            axis=1
        ).fillna(0)

        implied_exit_compare["horizon"] = cutoff_label
        implied_exit_compare["pct_of_actuals"] = np.where(
            implied_exit_compare["actual_exits"] > 0,
            implied_exit_compare["implied_exits_from_seen_bookings"] / implied_exit_compare["actual_exits"] * 100,
            np.nan
        )
        implied_exit_compare["pct_of_final_bookings"] = np.where(
            implied_exit_compare["final_booked_exits"] > 0,
            implied_exit_compare["implied_exits_from_seen_bookings"] / implied_exit_compare["final_booked_exits"] * 100,
            np.nan
        )
        implied_exit_compare["uplift_to_actual_pct_raw"] = np.where(
            implied_exit_compare["implied_exits_from_seen_bookings"] > 0,
            (implied_exit_compare["actual_exits"] - implied_exit_compare["implied_exits_from_seen_bookings"]) / implied_exit_compare["implied_exits_from_seen_bookings"] * 100,
            np.nan
        )
        implied_exit_compare["uplift_to_actual_pct_clipped"] = implied_exit_compare["uplift_to_actual_pct_raw"].clip(lower=0)
        implied_exit_compare = implied_exit_compare.reset_index().rename(columns={"exit_day": "service_day"})
        implied_exit_daily_frames.append(implied_exit_compare)

        # -------------------------
        # Summary matrices
        # -------------------------
        entry_pct_actual_avg[cutoff_label] = {"Entries": entry_compare["pct_of_actuals"].mean()}
        entry_pct_actual_median[cutoff_label] = {"Entries": entry_compare["pct_of_actuals"].median()}

        implied_exit_pct_actual_avg[cutoff_label] = {"Implied Exits": implied_exit_compare["pct_of_actuals"].mean()}
        implied_exit_pct_actual_median[cutoff_label] = {"Implied Exits": implied_exit_compare["pct_of_actuals"].median()}

        entry_uplift_avg[cutoff_label] = {"Entries": entry_compare["uplift_to_actual_pct_clipped"].mean()}
        entry_uplift_median[cutoff_label] = {"Entries": entry_compare["uplift_to_actual_pct_clipped"].median()}

        implied_exit_uplift_avg[cutoff_label] = {"Implied Exits": implied_exit_compare["uplift_to_actual_pct_clipped"].mean()}
        implied_exit_uplift_median[cutoff_label] = {"Implied Exits": implied_exit_compare["uplift_to_actual_pct_clipped"].median()}

        # -------------------------
        # Duration visibility for entries
        # % of FINAL ENTRY BOOKINGS already created by Dn, by duration
        # -------------------------
        seen_entry_by_duration = (
            seen.groupby(["entry_day", "duration_bucket"])["bookingUuid"]
            .nunique()
            .rename("seen_entry_duration_bookings")
            .reset_index()
        )

        duration_compare = final_entry_by_duration.merge(
            seen_entry_by_duration,
            on=["entry_day", "duration_bucket"],
            how="left"
        )

        duration_compare["seen_entry_duration_bookings"] = duration_compare["seen_entry_duration_bookings"].fillna(0)
        duration_compare["pct_visible"] = np.where(
            duration_compare["final_entry_duration_bookings"] > 0,
            duration_compare["seen_entry_duration_bookings"] / duration_compare["final_entry_duration_bookings"] * 100,
            np.nan
        )

        avg_by_bucket = duration_compare.groupby("duration_bucket")["pct_visible"].mean()
        med_by_bucket = duration_compare.groupby("duration_bucket")["pct_visible"].median()

        duration_entry_visible_avg[cutoff_label] = avg_by_bucket.to_dict()
        duration_entry_visible_median[cutoff_label] = med_by_bucket.to_dict()

    entry_pct_actual_avg_df = pd.DataFrame(entry_pct_actual_avg).round(2)
    entry_pct_actual_median_df = pd.DataFrame(entry_pct_actual_median).round(2)

    implied_exit_pct_actual_avg_df = pd.DataFrame(implied_exit_pct_actual_avg).round(2)
    implied_exit_pct_actual_median_df = pd.DataFrame(implied_exit_pct_actual_median).round(2)

    entry_uplift_avg_df = pd.DataFrame(entry_uplift_avg).round(2)
    entry_uplift_median_df = pd.DataFrame(entry_uplift_median).round(2)

    implied_exit_uplift_avg_df = pd.DataFrame(implied_exit_uplift_avg).round(2)
    implied_exit_uplift_median_df = pd.DataFrame(implied_exit_uplift_median).round(2)

    duration_entry_visible_avg_df = (
        pd.DataFrame(duration_entry_visible_avg)
        .reindex(duration_order)
        .round(2)
    )

    duration_entry_visible_median_df = (
        pd.DataFrame(duration_entry_visible_median)
        .reindex(duration_order)
        .round(2)
    )

    # Arrays ready to drop into forecast script
    entry_uplift_array_median = [
        float(entry_uplift_median_df.loc["Entries", f"D{h}"]) for h in horizons
    ]
    implied_exit_uplift_array_median = [
        float(implied_exit_uplift_median_df.loc["Implied Exits", f"D{h}"]) for h in horizons
    ]

    entry_uplift_array_avg = [
        float(entry_uplift_avg_df.loc["Entries", f"D{h}"]) for h in horizons
    ]
    implied_exit_uplift_array_avg = [
        float(implied_exit_uplift_avg_df.loc["Implied Exits", f"D{h}"]) for h in horizons
    ]

    entry_daily_detail = pd.concat(entry_daily_frames, ignore_index=True)
    implied_exit_daily_detail = pd.concat(implied_exit_daily_frames, ignore_index=True)

    return {
        "entry_pct_actual_avg": entry_pct_actual_avg_df,
        "entry_pct_actual_median": entry_pct_actual_median_df,
        "implied_exit_pct_actual_avg": implied_exit_pct_actual_avg_df,
        "implied_exit_pct_actual_median": implied_exit_pct_actual_median_df,
        "entry_uplift_avg": entry_uplift_avg_df,
        "entry_uplift_median": entry_uplift_median_df,
        "implied_exit_uplift_avg": implied_exit_uplift_avg_df,
        "implied_exit_uplift_median": implied_exit_uplift_median_df,
        "duration_entry_visible_avg": duration_entry_visible_avg_df,
        "duration_entry_visible_median": duration_entry_visible_median_df,
        "entry_uplift_array_median": entry_uplift_array_median,
        "implied_exit_uplift_array_median": implied_exit_uplift_array_median,
        "entry_uplift_array_avg": entry_uplift_array_avg,
        "implied_exit_uplift_array_avg": implied_exit_uplift_array_avg,
        "entry_daily_detail": entry_daily_detail,
        "implied_exit_daily_detail": implied_exit_daily_detail,
    }


def plot_fastpark_lead_time(df):
    """
    Basic plots for lead-time and duration analysis.
    """
    usable = df.loc[~df["invalid_entry_lead"] & ~df["invalid_exit_lead"]].copy()

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    # 1. Entry lead time histogram
    axes[0, 0].hist(
        usable["lead_to_entry_days_exact"].clip(upper=120).dropna(),
        bins=40
    )
    axes[0, 0].set_title("Lead Time to Entry (days, capped at 120)")
    axes[0, 0].set_xlabel("Days before entry")
    axes[0, 0].set_ylabel("Number of bookings")

    # 2. Exit lead time histogram
    axes[0, 1].hist(
        usable["lead_to_exit_days_exact"].clip(upper=120).dropna(),
        bins=40
    )
    axes[0, 1].set_title("Lead Time to Exit (days, capped at 120)")
    axes[0, 1].set_xlabel("Days before exit")
    axes[0, 1].set_ylabel("Number of bookings")

    # 3. Duration distribution
    duration_counts = (
        usable["duration_bucket"]
        .value_counts()
        .reindex([
            "1 day", "2 days", "3-6 days", "7-9 days", "10-13 days",
            "14-20 days", "21-29 days", "30-59 days", "60-89 days", "90+ days", "Unknown"
        ])
        .fillna(0)
    )
    axes[1, 0].bar(duration_counts.index.astype(str), duration_counts.values)
    axes[1, 0].set_title("Duration Distribution")
    axes[1, 0].set_xlabel("Duration bucket")
    axes[1, 0].set_ylabel("Number of bookings")
    axes[1, 0].tick_params(axis="x", rotation=45)

    # 4. % booked within X hours of entry for 1-day and 2-day stays
    short_stays = usable[usable["Duration_clean"].isin([1, 2])].copy()
    if not short_stays.empty:
        chart_df = short_stays.groupby("Duration_clean").agg(
            within_24h=("booked_within_24h_of_entry", lambda s: s.mean() * 100),
            within_48h=("booked_within_48h_of_entry", lambda s: s.mean() * 100),
            within_72h=("booked_within_72h_of_entry", lambda s: s.mean() * 100),
        )
        x = np.arange(len(chart_df.index))
        width = 0.25

        axes[1, 1].bar(x - width, chart_df["within_24h"], width, label="<=24h")
        axes[1, 1].bar(x, chart_df["within_48h"], width, label="<=48h")
        axes[1, 1].bar(x + width, chart_df["within_72h"], width, label="<=72h")
        axes[1, 1].set_xticks(x)
        axes[1, 1].set_xticklabels([f"{int(v)} day" if v == 1 else f"{int(v)} days" for v in chart_df.index])
        axes[1, 1].set_title("Last-minute booking % for 1-day and 2-day stays")
        axes[1, 1].set_xlabel("Duration")
        axes[1, 1].set_ylabel("% of bookings")
        axes[1, 1].legend()
    else:
        axes[1, 1].text(0.5, 0.5, "No 1-day / 2-day bookings found", ha="center", va="center")
        axes[1, 1].set_axis_off()

    plt.tight_layout()
    plt.show()


def run_fastpark_lead_time_analysis(start, end, deduplicate=False):
    """
    Full analysis runner.
    """
    raw = load_fastpark_bookings(start=start, end=end, deduplicate=deduplicate)
    enriched = prepare_fastpark_lead_time(raw)
    outputs = summarize_fastpark_lead_time(enriched)

    print("\n=== OVERALL SUMMARY ===")
    print(outputs["overall_summary"].to_string(index=False))

    print("\n=== DURATION DISTRIBUTION ===")
    print(outputs["duration_distribution"].to_string(index=False))

    print("\n=== BY DURATION ===")
    print(outputs["by_duration"].to_string(index=False))

    print("\n=== ENTRY LEAD DISTRIBUTION ===")
    print(outputs["entry_lead_distribution"].to_string(index=False))

    print("\n=== EXIT LEAD DISTRIBUTION ===")
    print(outputs["exit_lead_distribution"].to_string(index=False))

    print("\n=== 1-DAY / 2-DAY SUMMARY ===")
    print(outputs["one_two_day_summary"].to_string(index=False))

    print("\n=== DURATION x ENTRY LEAD ===")
    print(outputs["duration_x_entry_lead"].to_string())

    print("\n=== DURATION x EXIT LEAD ===")
    print(outputs["duration_x_exit_lead"].to_string())

    daily_outputs = analyse_last_minute_daily_pattern(enriched)

    print("\n=== LAST MONTH DAILY SUMMARY (1–2 DAY LEAD) ===")
    print(daily_outputs["final_daily"].to_string(index=False))

    # ---------------------------------------------------------
    # NEW CORE LOGIC:
    # CREATION DATE -> ENTRY DATE -> DURATION -> IMPLIED EXITS
    # ---------------------------------------------------------
    actual_entries_df, actual_exits_df = load_fastpark_actuals(
        start=start,
        end=end
    )

    creation_duration_outputs = build_creation_entry_duration_uplift(
        bookings_df=enriched,
        actual_entries_df=actual_entries_df,
        actual_exits_df=actual_exits_df,
        horizons=range(1, 15)
    )

    print("\n=== AVG % OF ACTUAL ENTRIES COVERED BY BOOKINGS CREATED BY D1-D14 BEFORE ENTRY ===")
    print(creation_duration_outputs["entry_pct_actual_avg"].to_string())

    print("\n=== MEDIAN % OF ACTUAL ENTRIES COVERED BY BOOKINGS CREATED BY D1-D14 BEFORE ENTRY ===")
    print(creation_duration_outputs["entry_pct_actual_median"].to_string())

    print("\n=== AVG % OF ACTUAL EXITS COVERED BY IMPLIED EXITS FROM THOSE BOOKINGS ===")
    print(creation_duration_outputs["implied_exit_pct_actual_avg"].to_string())

    print("\n=== MEDIAN % OF ACTUAL EXITS COVERED BY IMPLIED EXITS FROM THOSE BOOKINGS ===")
    print(creation_duration_outputs["implied_exit_pct_actual_median"].to_string())

    print("\n=== AVG ENTRY UPLIFT TO ACTUALS (CREATION->ENTRY) ===")
    print(creation_duration_outputs["entry_uplift_avg"].to_string())

    print("\n=== MEDIAN ENTRY UPLIFT TO ACTUALS (CREATION->ENTRY) ===")
    print(creation_duration_outputs["entry_uplift_median"].to_string())

    print("\n=== AVG IMPLIED EXIT UPLIFT TO ACTUALS ===")
    print(creation_duration_outputs["implied_exit_uplift_avg"].to_string())

    print("\n=== MEDIAN IMPLIED EXIT UPLIFT TO ACTUALS ===")
    print(creation_duration_outputs["implied_exit_uplift_median"].to_string())

    print("\n=== AVG % OF FINAL ENTRY BOOKINGS VISIBLE BY DURATION AT D1-D14 ===")
    print(creation_duration_outputs["duration_entry_visible_avg"].to_string())

    print("\n=== MEDIAN % OF FINAL ENTRY BOOKINGS VISIBLE BY DURATION AT D1-D14 ===")
    print(creation_duration_outputs["duration_entry_visible_median"].to_string())

    print("\n=== ENTRY UPLIFT ARRAY (MEDIAN, READY FOR FORECAST) ===")
    print(creation_duration_outputs["entry_uplift_array_median"])

    print("\n=== IMPLIED EXIT UPLIFT ARRAY (MEDIAN, READY FOR FORECAST) ===")
    print(creation_duration_outputs["implied_exit_uplift_array_median"])

    return raw, enriched, outputs, creation_duration_outputs


raw_df, enriched_df, outputs, creation_duration_outputs = run_fastpark_lead_time_analysis(
    start="2026-01-01",
    end="2026-06-30",
    deduplicate=True
)
