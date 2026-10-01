import calendar
from datetime import date
import pandas as pd
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from modules.utils.db import get_engine

# =====================================================
# MONTHLY TRANSACTION TOTALS
# =====================================================

monthly_transactions = {

    (2026, 1): 12060,
    (2026, 2): 12093,
    (2026, 3): 14620,
    (2026, 4): 14586,
    (2026, 5): 16434,
    (2026, 6): 16103,
    (2026, 7): 14994,
    (2026, 8): 16315,
    (2026, 9): 20211,
    (2026,10): 20008,
    (2026,11): 14259,
    (2026,12): 11452,

    (2027, 1): 14093,
    (2027, 2): 16199,
    (2027, 3): 16423,
    (2027, 4): 18087,
    (2027, 5): 19617,
    (2027, 6): 21878,
    (2027, 7): 18365,
    (2027, 8): 18197,
    (2027, 9): 24469,
    (2027,10): 24601,
    (2027,11): 18674,
    (2027,12): 15072,

    (2028, 1): 17343,
    (2028, 2): 17991,
    (2028, 3): 20150,
    (2028, 4): 22365,
    (2028, 5): 19985,
    (2028, 6): 20463,
    (2028, 7): 19460,
    (2028, 8): 19353,
    (2028, 9): 24092,
    (2028,10): 25442,
    (2028,11): 15179,
    (2028,12): 12365,

    (2029, 1): 18711,
    (2029, 2): 18905,
    (2029, 3): 21925,
    (2029, 4): 24906,
    (2029, 5): 22579,
    (2029, 6): 22933,
    (2029, 7): 21570,
    (2029, 8): 21421,
    (2029, 9): 26060,
    (2029,10): 27255,
    (2029,11): 14930,
    (2029,12): 11935,

    (2030, 1): 18168,
    (2030, 2): 18409,
    (2030, 3): 21568,
    (2030, 4): 24874,
    (2030, 5): 24992,
    (2030, 6): 25508,
    (2030, 7): 23339,
    (2030, 8): 23183,
    (2030, 9): 27326,
    (2030,10): 28263,
    (2030,11): 16279,
    (2030,12): 13085,

    (2031, 1): 21057,
    (2031, 2): 20748,
    (2031, 3): 24610,
    (2031, 4): 28086,
    (2031, 5): 27604,
    (2031, 6): 28031,
    (2031, 7): 24939,
    (2031, 8): 24631,
    (2031, 9): 28947,
    (2031,10): 30273,
    (2031,11): 19085,
    (2031,12): 15160,
}

def build_historical_profile():

    engine = get_engine(
        dsn="AzureConnection",
        username="jamie_douglas"
    )

    sql = """
    SELECT
        BookingReference,
        CheckInStarted,
        ActualCheckedOutDate
    FROM FastPark.v_EntryAndExits
    WHERE CheckInStarted >= DATEADD(year,-3,GETDATE())
    """

    df = pd.read_sql(sql, con=engine)

    df["CheckInStarted"] = pd.to_datetime(
        df["CheckInStarted"]
    )

    df["ActualCheckedOutDate"] = pd.to_datetime(
        df["ActualCheckedOutDate"]
    )

    return df

def build_profile(df, time_col):

    working = df.dropna(
        subset=[time_col]
    ).copy()

    working["month"] = (
        working[time_col]
        .dt.month
    )

    working["week_of_month"] = (
        (working[time_col].dt.day - 1) // 7
    ) + 1

    working["weekday"] = (
        working[time_col]
        .dt.dayofweek
    )

    working["slot15"] = (
        working[time_col].dt.hour * 4
        +
        working[time_col].dt.minute // 15
    )

    profile = (
        working
        .groupby(
            [
                "month",
                "week_of_month",
                "weekday",
                "slot15"
            ]
        )
        .size()
        .reset_index(name="count")
    )

    profile["pct"] = (
        profile["count"]
        /
        profile.groupby(
            ["month"]
        )["count"]
        .transform("sum")
    )

    return profile

history_df = build_historical_profile()

entry_profile = build_profile(
    history_df,
    "CheckInStarted"
)

exit_profile = build_profile(
    history_df,
    "ActualCheckedOutDate"
)

print(
    entry_profile.groupby("month")["pct"].sum()
)

print(
    exit_profile.groupby("month")["pct"].sum()
)


def build_month_from_profile(
    year,
    month,
    monthly_total,
    profile
):

    month_profile = profile[
        profile["month"] == month
    ].copy()

    days_in_month = calendar.monthrange(
        year,
        month
    )[1]

    rows = {}

    for day in range(1, days_in_month + 1):

        d = date(year, month, day)

        rows[d] = {
            "Date": d
        }

        for slot in range(96):
            rows[d][str(slot)] = 0.0

    for _, prof_row in month_profile.iterrows():

        wom = int(
            prof_row["week_of_month"]
        )

        weekday = int(
            prof_row["weekday"]
        )

        slot15 = int(
            prof_row["slot15"]
        )

        allocation = (
            monthly_total
            * prof_row["pct"]
        )

        matching_days = []

        for day in range(1, days_in_month + 1):

            d = date(year, month, day)

            test_wom = (
                (d.day - 1) // 7
            ) + 1

            if (
                d.weekday() == weekday
                and
                test_wom == wom
            ):
                matching_days.append(d)

        if not matching_days:
            continue

        allocation_per_day = (
            allocation
            / len(matching_days)
        )

        for d in matching_days:

            rows[d][str(slot15)] += (
                allocation_per_day
            )

    output_rows = []

    for row in rows.values():

        for slot in range(96):
            row[str(slot)] = round(
                row[str(slot)]
            )

        output_rows.append(row)

    return output_rows


output_file = r"C:\Users\jamie_douglas\OneDrive - Edinburgh Airport Limited\Documents\transaction_breakdown.xlsx"

summary_rows = []

kiosk_summary_rows = []

KIOSK_SERVICE_TIME = 26.8
KIOSK_BUFFER = 10

KIOSK_CYCLE_TIME = KIOSK_SERVICE_TIME + KIOSK_BUFFER

KIOSKS = 5

kiosk_capacity = (
    3600 / KIOSK_CYCLE_TIME
) * KIOSKS

kiosk_capacity_15 = (
    kiosk_capacity
    * 0.25
)

ROOM_AREA = 56
SPACE_PER_PERSON = 1.3

room_capacity_people = (
    ROOM_AREA / SPACE_PER_PERSON
)

with pd.ExcelWriter(
    output_file,
    engine="openpyxl"
) as writer:

    for year in range(2026, 2032):

        entry_rows = []
        exit_rows = []

        for month in range(1, 13):

            if year == 2026 and month < 10:     
                continue

            total = monthly_transactions[(year, month)]

            month_entry_rows = (
                build_month_from_profile(
                    year,
                    month,
                    total,
                    entry_profile
                )
            )

            month_exit_rows = (
                build_month_from_profile(
                    year,
                    month,
                    total,
                    exit_profile
                )
            )

            entry_rows.extend(month_entry_rows)
            exit_rows.extend(month_exit_rows)

            # ==========================================
            # QUEUE RISK CALCULATION
            # ==========================================

            entry_month_df = (
                pd.DataFrame(month_entry_rows)
                .sort_values("Date")
                .reset_index(drop=True)
            )

            entry_loads = (
                entry_month_df[
                    [str(i) for i in range(96)]
                ]
                .values
                .flatten()
            )

            exit_month_df = (
                pd.DataFrame(month_exit_rows)
                .sort_values("Date")
                .reset_index(drop=True)
            )

            effective_loads = []

            for day_idx in range(len(entry_month_df)):

                for slot in range(96):

                    entry_val = entry_month_df.loc[
                        day_idx,
                        str(slot)
                    ]

                    # ==========================
                    # Exit occurs 2 hours later
                    # ==========================

                    exit_day_idx = day_idx
                    exit_slot = slot + 8

                    if exit_slot >= 96:
                        exit_slot -= 96
                        exit_day_idx += 1

                    if exit_day_idx < len(exit_month_df):

                        exit_val = exit_month_df.loc[
                            exit_day_idx,
                            str(exit_slot)
                        ]

                    else:
                        exit_val = 0

                    # ==========================================
                    # Convert exits into equivalent booth load
                    # ==========================================

                    effective_load = (
                        entry_val +
                        (exit_val * (12 / 37))
                    )

                    effective_loads.append(effective_load)

            dual_lane_capacity = (3600 / 37) * 2 * 0.25

            periods_80 = sum(
                x >= dual_lane_capacity * 0.80
                for x in effective_loads
            )

            periods_90 = sum(
                x >= dual_lane_capacity * 0.90
                for x in effective_loads
            )

            periods_100 = sum(
                x >= dual_lane_capacity
                for x in effective_loads
            )

            max_effective_load = max(effective_loads)

            max_utilisation = (
                max_effective_load /
                dual_lane_capacity
            ) * 100

            summary_rows.append({
                "Month": f"{calendar.month_abbr[month]}-{year}",
                "15-Min Periods >80% Capacity":
                    periods_80,
                "15-MinPeriods >90% Capacity":
                    periods_90,
                "15-Min Periods >100% Capacity":
                    periods_100,
                "Peak Effective Load":
                    round(max_effective_load, 1),
                "Peak Utilisation (%)":
                    round(max_utilisation, 1),
            })


            ROOM_TIME_MINUTES = 2

            space_1 = []
            space_2 = []
            space_3 = []
            space_4 = []

            for arrivals in entry_loads:

                space_1.append(
                    arrivals * 1 * (2/60) * 1.3
                )

                space_2.append(
                    arrivals * 2 * (2/60) * 1.3
                )

                space_3.append(
                    arrivals * 3 * (2/60) * 1.3
                )

                space_4.append(
                    arrivals * 4 * (2/60) * 1.3
                )

            kiosk_capacity_exceeded = sum(
                x > kiosk_capacity_15
                for x in entry_loads
            )

            kiosk_summary_rows.append({

                "Month":
                    f"{calendar.month_abbr[month]}-{year}",

                f"Kiosk Capacity Exceeded (>{kiosk_capacity:.0f}/hr)":
                    kiosk_capacity_exceeded,

                "Min m² (1 Person/Car)":
                    round(min(space_1), 1),

                "Max m² (1 Person/Car)":
                    round(max(space_1), 1),

                "Min m² (2 People/Car)":
                    round(min(space_2), 1),

                "Max m² (2 People/Car)":
                    round(max(space_2), 1),

                "Min m² (3 People/Car)":
                    round(min(space_3), 1),

                "Max m² (3 People/Car)":
                    round(max(space_3), 1),

                "Min m² (4 People/Car)":
                    round(min(space_4), 1),

                "Max m² (4 People/Car)":
                    round(max(space_4), 1)
            })



        entry_df = pd.DataFrame(entry_rows)
        entry_df = entry_df.sort_values("Date")

        exit_df = pd.DataFrame(exit_rows)
        exit_df = exit_df.sort_values("Date")

        entry_df.to_excel(
            writer,
            sheet_name=f"Entry_{year}",
            index=False
        )

        exit_df.to_excel(
            writer,
            sheet_name=f"Exit_{year}",
            index=False
        )

    summary_df = pd.DataFrame(summary_rows)

    summary_df.to_excel(
        writer,
        sheet_name="Photobooth Capacity",
        startrow=6,
        index=False
    )

    worksheet = writer.sheets["Photobooth Capacity"]

    worksheet["A1"] = "Photobooth Utilisation Risk (Hours per Month)"

    worksheet["A2"] = (
        "Entry vehicles use the photobooth at their entry hour."
    )

    worksheet["A3"] = (
        "Exit vehicles use the photobooth 2 hours prior to their scheduled exit hour."
    )

    worksheet["A4"] = (
        "Entry service time = 37 seconds. Exit service time = 12 seconds."
    )

    worksheet["A5"] = (
        "Effective Load = Entries + (Exits × 12/37). "
        "Entry service time = 37 seconds. "
        "Exit service time = 12 seconds. "
        "Two photobooths assumed available. "
        "Results show the number of hours where effective demand exceeds 80%, 90% and 100% of total photobooth capacity."
    )

    kiosk_df = pd.DataFrame(kiosk_summary_rows)

    kiosk_df.to_excel(
        writer,
        sheet_name="Kiosk Capacity",
        startrow=7,
        index=False
    )

    worksheet = writer.sheets["Kiosk Capacity"]

    worksheet["A1"] = (
        "Kiosk Room Space Requirement"
    )

    worksheet["A2"] = (
        "Queue area available = 56m² using the full flexible-barrier footprint."
    )

    worksheet["A3"] = (
        "Space standard = 1.3m² per person."
    )

    worksheet["A4"] = (
        "Five kiosks assumed available."
    )

    worksheet["A5"] = (
        "Average kiosk transaction time = 26.8 seconds."
    )

    worksheet["A6"] = (
        "A 10 second transition time between users has been assumed."
    )

    worksheet["A7"] = (
        f"Effective kiosk capacity = {kiosk_capacity:.1f} vehicles per hour."
    )

    worksheet["A8"] = (
        "Average occupancy within the queue area assumed to be 2 minutes."
    )

    worksheet["A9"] = (
        "Required Space (m²) = Vehicles per Hour × People per Vehicle × (2 ÷ 60) × 1.3."
    )

    worksheet["A10"] = (
        "Results show the minimum and maximum queue area required during each month."
    )

print(f"Created {output_file}")