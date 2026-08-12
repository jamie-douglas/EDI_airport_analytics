import sys
import pathlib
from pathlib import Path
from datetime import datetime, timedelta

sys.path.append(
    str(pathlib.Path(__file__).resolve().parents[1])
)


import time
import pandas as pd


from modules.utils.query import query
from modules.utils.dates import to_datetime, add_date_parts
from modules.utils.progress import step

from modules.domain.prm.minibus import passenger_level_flags

from modules.domain.prm.efficiency import (
    vehicle_job_service_time,
    bucket_and_prepare,
    prm_per_vehicle_bucket,
    rolling_sums_and_labels,
    peak_rolling_hour_VM,
    VM_utilisation,
    median_std_VM_PRMs,
    hour_of_day_average,
)


# ============================================================
# CONFIGURATION
# ============================================================

ANALYSIS_START = "2026-07-20"

OPERATING_START = "14:00"
OPERATING_END = "18:00"

WEEKDAY_NUMBERS = [0, 1, 2, 3, 4]
# Monday = 0
# Tuesday = 1
# Wednesday = 2
# Thursday = 3
# Friday = 4


# ============================================================
# LOAD PRM DATA
# ============================================================

def load_prm_data(
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Load billable PRM jobs for the specified operation-date range.

    The `end` date is exclusive.

    Parameters
    ----------
    start : str
        Inclusive start date in ISO format, for example "2026-07-20".

    end : str
        Exclusive end date in ISO format.

    Returns
    -------
    pandas.DataFrame
        Job, passenger, employee, vehicle, flight and location
        information for billable PRM activity.
    """

    start_op = start.replace("-", "")
    end_op = end.replace("-", "")

    df = query(
        table="PRM.CompletedServicesByJob",
        columns=[
            "RequestID AS [Job ID]",
            "PassengerID AS [Passenger ID]",
            "FlightID AS [Flight ID]",
            "AirlineCode_IATA AS [Airline Code]",
            "FlightNumber AS [Flight Number]",
            "Sector",
            "ArrDep AS [A/D]",
            "currentSSRCode AS [SSR Code]",
            "startService_DateTime_Local AS [Job Start Time]",
            "finishService_DateTime_Local AS [Job End Time]",
            "disregardCode AS [Disregard Code]",
            "scheduledDestinationConcourse AS [Scheduled DO Concourse]",
            "scheduledDestinationLocation AS [Scheduled DO Location]",
            "scheduledDestinationLocationType AS [Scheduled DO Location Type]",
            "ScheduledDateTime_Local AS [Scheduled Flight Time]",
            "actualPickupLocation AS [Actual PU Location]",
            "actualPickupConcourse AS [Actual PU Concourse]",
            "actualPickupLocationType AS [Actual PU Location Type]",
            "actualDestinationConcourse AS [Actual DO Concourse]",
            "actualDestinationLocation AS [Actual DO Location]",
            "actualDestinationLocationType AS [Actual DO Location Type]",
            "EmployeeName AS [Employee]",
            "VehicleShortName AS [Vehicle Model]",
            "VehicleTypeName AS [Vehicle Type]",
            "StandCode AS [Stand]",
        ],
        where=[
            "BillingPRM = 1",
            "Operation_DateID_Local >= :start_op",
            "Operation_DateID_Local < :end_op",
        ],
        params={
            "start_op": start_op,
            "end_op": end_op,
        },
        query_option="OPTION (RECOMPILE)",
    )

    df = to_datetime(
        df,
        [
            "Job Start Time",
            "Job End Time",
            "Scheduled Flight Time",
        ],
    )

    df = add_date_parts(
        df,
        col="Job Start Time",
        day=True,
    )

    return df


# ============================================================
# FILTER TO THE REQUIRED OPERATING WINDOW
# ============================================================

def filter_operating_window(
    prm_df: pd.DataFrame,
    start_time: str = "14:30",
    end_time: str = "18:00",
) -> pd.DataFrame:
    """
    Restrict PRM jobs to Monday-Friday and the specified
    daily operating window.

    Inclusion rule
    --------------
    Monday-Friday

    start_time <= Job Start Time < end_time

    Therefore, with the defaults:

        14:30 <= Job Start Time < 18:00

    Parameters
    ----------
    prm_df : pandas.DataFrame
        PRM job-level data.

    start_time : str, default "14:30"
        Inclusive beginning of the operating window.

    end_time : str, default "18:00"
        Exclusive end of the operating window.

    Returns
    -------
    pandas.DataFrame
        PRM records whose job starts fall within the required
        weekday and time-of-day window.
    """

    df = prm_df.copy()

    df = to_datetime(
        df,
        [
            "Scheduled Flight Time",
        ],
    )

    df = df.dropna(
        subset=[
            "Job Start Time",
            "Job End Time",
            "Scheduled Flight Time",
        ]
    )

    start_clock = pd.to_datetime(start_time).time()
    end_clock = pd.to_datetime(end_time).time()

    weekday_mask = df["Job Start Time"].dt.weekday.isin(
        WEEKDAY_NUMBERS
    )

    time_mask = (
        (df["Scheduled Flight Time"].dt.time >= start_clock)
        & (df["Scheduled Flight Time"].dt.time < end_clock)
    )

    filtered_df = df.loc[
        weekday_mask & time_mask
    ].copy()

    filtered_df["Analysis Date"] = (
        filtered_df["Scheduled Flight Time"].dt.normalize()
    )

    filtered_df["Weekday"] = (
        filtered_df["Scheduled Flight Time"].dt.day_name()
    )

    return filtered_df


# ============================================================
# SSR UTILISATION BY VEHICLE TYPE
# ============================================================

def ssr_usage_by_vehicle_type(
    prm_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate the unique passenger count and percentage represented
    by each SSR code within each vehicle type.

    The percentage denominator is the total number of unique
    passengers associated with the vehicle type.

    SSR Percentage =
        Unique passengers for Vehicle Type x SSR Code
        ------------------------------------------------
        Total unique passengers for Vehicle Type

    Parameters
    ----------
    prm_df : pandas.DataFrame
        PRM data containing:

        - Passenger ID
        - Vehicle Type
        - SSR Code

    Returns
    -------
    pandas.DataFrame
        Columns:

        - Vehicle Type
        - SSR Code
        - SSR Passenger Count
        - Vehicle Passenger Total
        - SSR Percentage
    """

    df = prm_df.copy()

    df["Vehicle Type"] = (
        df["Vehicle Type"]
        .fillna("No Vehicle")
        .astype(str)
        .str.strip()
    )

    df["SSR Code"] = (
        df["SSR Code"]
        .fillna("Unknown")
        .astype(str)
        .str.strip()
        .replace("", "Unknown")
    )

    # One record per passenger, vehicle type and SSR code.
    passenger_ssr = df[
        [
            "Passenger ID",
            "Vehicle Type",
            "SSR Code",
        ]
    ].drop_duplicates()

    # Unique passengers for each Vehicle Type x SSR Code.
    ssr_counts = (
        passenger_ssr
        .groupby(
            [
                "Vehicle Type",
                "SSR Code",
            ],
            dropna=False,
        )["Passenger ID"]
        .nunique()
        .reset_index(
            name="SSR Passenger Count"
        )
    )

    # Total unique passengers for each vehicle type.
    vehicle_totals = (
        passenger_ssr
        .groupby(
            "Vehicle Type",
            dropna=False,
        )["Passenger ID"]
        .nunique()
        .reset_index(
            name="Vehicle Passenger Total"
        )
    )

    ssr_usage = ssr_counts.merge(
        vehicle_totals,
        on="Vehicle Type",
        how="left",
    )

    ssr_usage["SSR Percentage"] = (
        ssr_usage["SSR Passenger Count"]
        / ssr_usage["Vehicle Passenger Total"]
        * 100
    )

    ssr_usage = ssr_usage.sort_values(
        by=[
            "Vehicle Type",
            "SSR Percentage",
            "SSR Code",
        ],
        ascending=[
            True,
            False,
            True,
        ],
    ).reset_index(drop=True)

    return ssr_usage


# ============================================================
# VEHICLE UTILISATION FOR THE OPERATING WINDOW
# ============================================================

def operating_window_vehicle_utilisation(
    prm_df: pd.DataFrame,
    start_time: str = "14:30",
    end_time: str = "18:00",
):
    """
    Calculate vehicle-model rolling-hour activity and utilisation
    for Monday-Friday between the requested operating times.

    This uses the existing efficiency.py helper functions but removes
    overnight, weekend and out-of-window rolling periods before the
    final statistics are calculated.

    Parameters
    ----------
    prm_df : pandas.DataFrame
        PRM data already restricted to the required job-start window.

    start_time : str, default "14:30"
        Inclusive beginning of the analysis window.

    end_time : str, default "18:00"
        Exclusive end of the analysis window.

    Returns
    -------
    tuple
        (
            hour_average_long,
            hour_average_pivot,
            overall_average,
            peak_df,
            utilisation_df,
            stats_df,
            rolling_window_df,
        )
    """

    if prm_df.empty:
        empty = pd.DataFrame()

        return (
            empty,
            empty,
            empty,
            empty,
            empty,
            empty,
            empty,
        )

    # --------------------------------------------------------
    # 1. Build 15-minute vehicle-model buckets
    # --------------------------------------------------------

    bucket_df = bucket_and_prepare(
        prm_df,
        vehicle_model=True,
    )

    bucket_counts = prm_per_vehicle_bucket(
        bucket_df,
        vehicle_model=True,
    )

    rolling_df = rolling_sums_and_labels(
        bucket_counts,
        vehicle_model=True,
    )

    # --------------------------------------------------------
    # 2. Remove weekends and times outside the requested window
    # --------------------------------------------------------

    start_clock = pd.to_datetime(start_time).time()
    end_clock = pd.to_datetime(end_time).time()

    weekday_mask = rolling_df["Bucket"].dt.weekday.isin(
        WEEKDAY_NUMBERS
    )

    time_mask = (
        (rolling_df["Bucket"].dt.time >= start_clock)
        & (rolling_df["Bucket"].dt.time < end_clock)
    )

    rolling_window_df = rolling_df.loc[
        weekday_mask & time_mask
    ].copy()

    # --------------------------------------------------------
    # 3. Calculate statistics only from accepted windows
    # --------------------------------------------------------

    peak_df = peak_rolling_hour_VM(
        rolling_window_df
    )

    utilisation_df = VM_utilisation(
        rolling_window_df
    )

    stats_df = median_std_VM_PRMs(
        rolling_window_df
    )

    (
        hour_average_long,
        hour_average_pivot,
        overall_average,
    ) = hour_of_day_average(
        rolling_window_df,
        vehicle_model=True,
    )

    return (
        hour_average_long,
        hour_average_pivot,
        overall_average,
        peak_df,
        utilisation_df,
        stats_df,
        rolling_window_df,
    )


# ============================================================
# MAIN ANALYSIS
# ============================================================

if __name__ == "__main__":

    start = ANALYSIS_START

    output_folder = Path("outputs/PRM/minibus_trial")
    output_folder.mkdir(parents=True, exist_ok=True)    

    # The SQL end date is exclusive, so use tomorrow to include
    # all available records from today.
    today = pd.Timestamp.today().normalize()

    end = (
        today + pd.Timedelta(days=1)
    ).strftime("%Y-%m-%d")

    t0 = time.perf_counter()

    print(
        "\n"
        "============================================================\n"
        "PRM AFTERNOON OPERATION ANALYSIS\n"
        "============================================================\n"
    )

    print(f"Date range: {start} to {today:%Y-%m-%d}")
    print("Days: Monday to Friday")
    print(
        f"Daily job-start window: "
        f"{OPERATING_START} to {OPERATING_END}\n"
    )

    # ========================================================
    # 1. LOAD PRM DATA
    # ========================================================

    print("[1/5] Loading PRM jobs...")

    prm_df = load_prm_data(
        start=start,
        end=end,
    )

    t1 = step(
        t0,
        f"Loaded PRM rows: {len(prm_df):,}",
    )

    # ========================================================
    # 2. FILTER TO MONDAY-FRIDAY, 14:30-18:00
    # ========================================================

    print(
        "[2/5] Filtering to Monday-Friday, "
        "14:30-18:00..."
    )

    operation_df = filter_operating_window(
        prm_df,
        start_time=OPERATING_START,
        end_time=OPERATING_END,
    )

    t2 = step(
        t1,
        (
            "Operating-window PRM rows retained: "
            f"{len(operation_df):,}"
        ),
    )

    if operation_df.empty:
        raise ValueError(
            "No PRM records were found for Monday-Friday "
            f"between {OPERATING_START} and {OPERATING_END} "
            f"from {start} to {today:%Y-%m-%d}."
        )

    print(
        "\nOperating-window coverage:"
    )

    print(
        operation_df[
            [
                "Analysis Date",
                "Weekday",
            ]
        ]
        .drop_duplicates()
        .sort_values("Analysis Date")
        .to_string(index=False)
    )

    # ========================================================
    # 3. PASSENGER FLAGS
    # ========================================================

    print(
        "\n[3/5] Building passenger-level flags..."
    )

    flags_df = passenger_level_flags(
        operation_df
    )

    t3 = step(
        t2,
        f"Passenger flags built: {len(flags_df):,}",
    )

    # ========================================================
    # 4. VEHICLE SERVICE TIMES AND UTILISATION
    # ========================================================

    print(
        "\n[4/5] Calculating vehicle service times "
        "and utilisation..."
    )

    vehicle_service_times = vehicle_job_service_time(
        operation_df
    )

    t4 = step(
        t3,
        (
            "Vehicle Type Service Times Calculated:\n"
            f"{vehicle_service_times.to_string(index=False)}"
        ),
    )

    (
        utilisation_by_time,
        utilisation_by_time_pivot,
        average_rolling_hour_usage,
        peak_rolling_hours,
        vehicle_utilisation,
        rolling_hour_statistics,
        rolling_window_detail,
    ) = operating_window_vehicle_utilisation(
        operation_df,
        start_time=OPERATING_START,
        end_time=OPERATING_END,
    )

    t5 = step(
        t4,
        (
            "Vehicle Utilisation Calculated:\n"
            f"{vehicle_utilisation.to_string(index=False)}"
        ),
    )

    print(
        "\n=== AVERAGE PRMs PER VEHICLE MODEL "
        "BY ROLLING-HOUR WINDOW ==="
    )

    print(
        utilisation_by_time_pivot.to_string(
            index=False
        )
    )

    print(
        "\n=== OVERALL AVERAGE PRMs "
        "PER VEHICLE MODEL ==="
    )

    print(
        average_rolling_hour_usage.to_string(
            index=False
        )
    )

    print(
        "\n=== PEAK ROLLING HOUR "
        "BY VEHICLE MODEL ==="
    )

    print(
        peak_rolling_hours.to_string(
            index=False
        )
    )

    print(
        "\n=== VEHICLE-MODEL UTILISATION ==="
    )

    print(
        vehicle_utilisation.to_string(
            index=False
        )
    )

    print(
        "\n=== ROLLING-HOUR MEDIAN "
        "AND STANDARD DEVIATION ==="
    )

    print(
        rolling_hour_statistics.to_string(
            index=False
        )
    )

    # ========================================================
    # 5. SSR USE BY VEHICLE TYPE
    # ========================================================

    print(
        "\n[5/5] Calculating SSR-code use "
        "by vehicle type..."
    )

    ssr_vehicle_usage = ssr_usage_by_vehicle_type(
        operation_df
    )

    t6 = step(
        t5,
        (
            "SSR Use by Vehicle Type Calculated:\n"
            f"{ssr_vehicle_usage.to_string(index=False)}"
        ),
    )

    print(
        "\n============================================================\n"
        "SSR PERCENTAGE BY VEHICLE TYPE\n"
        "============================================================\n"
    )

    for vehicle_type, subset in ssr_vehicle_usage.groupby(
        "Vehicle Type",
        sort=True,
    ):

        print(
            f"\n=== {vehicle_type.upper()} ==="
        )

        print(
            subset[
                [
                    "SSR Code",
                    "SSR Passenger Count",
                    "Vehicle Passenger Total",
                    "SSR Percentage",
                ]
            ].to_string(
                index=False,
                formatters={
                    "SSR Percentage": (
                        lambda value: f"{value:.2f}%"
                    )
                },
            )
        )

    print(
        "\n============================================================\n"
        "ANALYSIS COMPLETE\n"
        "============================================================\n"
    )

    t7 = step(
        t6,
        "Full PRM afternoon operation analysis completed",
    )

    print(
        "\n============================================================\n"
        "EXPORTING TO EXCEL\n"
        "============================================================\n"
    )

    run_date = datetime.now().strftime("%Y%m%d")

    output_file = (
        output_folder /
        f"trial_KPIs_{run_date}.xlsx"
    )

    summary_df = pd.DataFrame({

    "Metric": [
        "Analysis Start Date",
        "Analysis End Date",
        "Days Included",
        "Operating Window",
        "PRM Rows",
        "Unique Passengers",
        "Unique Vehicle Models",
        "Ambulift Passengers",
        "Minibus Passengers",
        "No Vehicle Passengers"
    ],

    "Value": [
        start,
        pd.Timestamp.today().strftime("%Y-%m-%d"),
        "Monday-Friday",
        f"{OPERATING_START}-{OPERATING_END}",
        len(operation_df),
        operation_df["Passenger ID"].nunique(),
        operation_df["Vehicle Model"].nunique(),
        operation_df.loc[
            operation_df["Vehicle Type"] == "Ambulift",
            "Passenger ID"
        ].nunique(),
        operation_df.loc[
            operation_df["Vehicle Type"] == "Mini Bus",
            "Passenger ID"
        ].nunique(),
        operation_df.loc[
            operation_df["Vehicle Type"].fillna("No Vehicle") == "No Vehicle",
            "Passenger ID"
        ].nunique()
    ]

    })

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

        summary_df.to_excel(
            writer,
            sheet_name="Summary",
            index=False
        )

        vehicle_service_times.to_excel(
            writer,
            sheet_name="Vehicle Service Times",
            index=False
        )

        vehicle_utilisation.to_excel(
            writer,
            sheet_name="Vehicle Utilisation",
            index=False
        )

        rolling_hour_statistics.to_excel(
            writer,
            sheet_name="Rolling Hour Stats",
            index=False
        )

        average_rolling_hour_usage.to_excel(
            writer,
            sheet_name="Avg Rolling Hour Usage",
            index=False
        )

        utilisation_by_time_pivot.to_excel(
            writer,
            sheet_name="Rolling Hour Profile",
            index=False
        )

        peak_rolling_hours.to_excel(
            writer,
            sheet_name="Peak Rolling Hours",
            index=False
        )

        ssr_vehicle_usage.to_excel(
            writer,
            sheet_name="SSR by Vehicle",
            index=False
        )

    t8 = step(
            t7,
            f"\nExcel output created:\n{output_file}",
        )
