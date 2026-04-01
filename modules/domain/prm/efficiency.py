#modules/domain/prm/efficiency.py

import pandas as pd

from modules.utils.dates import to_datetime
from modules.analytics.grouping import group_average, group_sum, group_unique
from modules.analytics.timeseries import bucket_time, rolling_sum, peak_rolling_window

#----------------------------------------------------
# SERVICE TIME FUNCTIONS
#----------------------------------------------------

def vehicle_job_service_time(prm_df: pd.DataFrame,  start=None, end=None, passenger_flags=False):
    """
    Takes a dataframe and returns the average service time for a Vehicle Type ['Ambulift', 'Mini Bus', 'No Vehicle'] for a defined period

    Parameters
    ----------
    prm_df:
        pandas.DataFrame with columns ["Vehicle Type", "Job Start Time", "Job End Time"], note: if passenger_flags is True this also needs the column ["PassengerType"]
    start: str, end: str | default = False
        dates for using if wanting to filter to smaller time period

    Returns
    ----------
        pandas.DataFrame with columns:
            Vehicle Type | Average Service Time (minutes)

    Drops duplicate Job ID as to not count the same vehicle service time twice
    """

    x = prm_df.copy()
    
    x = to_datetime(x, ["Job Start Time", "Job End Time"])
    x["Vehicle Type"] = x["Vehicle Type"].fillna("No Vehicle")

    if passenger_flags:
        x = x[x["PassengerType"] == "Both"]
    
    if start:
        x = x[x["Job Start Time"] >= start]
    if end:
        x = x[x["Job End Time"] < end]

    x.drop_duplicates(subset=["Job ID"], keep='first', inplace=True)

    x["Job Service Time"] = (x["Job End Time"] - x["Job Start Time"]).dt.total_seconds() / 60


    serv_time_df = group_average(x, by_cols="Vehicle Type", value_col="Job Service Time", out_col="Average Service Time (minutes)")

    return serv_time_df

def end_to_end_service_time(flags_df: pd.DataFrame, start=None, end=None):
    """
    Takes a dataframe and returns the average end-to-end service time for a single passenger per Passenger Flag ["Ambulift Only", "Mini Bus Only", "Both", "No Vehicle"]
    Drops duplicate Job ID as to not count the same vehicle service time twice
    
    Parameters
    ---------
    flags_df:
        pandas.DataFrame with columns ["Job Start Time", "Job End Time"], note: if passenger_flags is True this also needs the column ["PassengerType"]
    start: str, end: str | default = False
        dates for using if wanting to filter to smaller time period

    Returns
    ---------
    pandas.DataFrame with columns:
            PassengerType | Average End-to-end Service Time (minutes)

    

    """
    x = flags_df.copy()
    
    x = to_datetime(x, ["Job Start Time", "Job End Time"])

    if start:
        x = x[x["Job Start Time"] >= start]
    if end:
        x = x[x["Job End Time"] < end]

    x.drop_duplicates(subset=["Job ID"], keep='first', inplace=True)

    x["Job Service Time"] = (x["Job End Time"] - x["Job Start Time"]).dt.total_seconds() / 60
   
    #Total service time per passenger
    passenger_totals = group_sum(x, by_cols=["Passenger ID"], value_col="Job Service Time", out_col = "Total Service Time")
    
    #bring PassengerType back in (1 row per passenger)
    passenger_types = x[["Passenger ID", "PassengerType"]].drop_duplicates()
    passenger_totals = passenger_totals.merge(passenger_types, on= "Passenger ID", how="left")

    #Average total service time per passenger type
    results = group_average(passenger_totals, by_cols=["PassengerType"], value_col="Total Service Time", out_col="Average End-to-end Service Time (minutes)")

    return results


#----------------------------------------------------
# VEHICLE USAGE FUNCTION AND HELPER FUNCTIONS
#----------------------------------------------------

def bucket_and_prepare(prm_df, vehicle_model=False):
    bucket_df = bucket_time(prm_df, time_col="Job Start Time", freq="15min", out_col="Bucket")

    bucket_df["Vehicle Type"] = bucket_df["Vehicle Type"].fillna("No Vehicle")

    if vehicle_model:
        bucket_df["Vehicle Model"] = bucket_df["Vehicle Model"].fillna("Unknown")
    
    return bucket_df

def prm_per_vehicle_bucket(bucket_df, vehicle_model=False):
    
    if vehicle_model:
        by_cols=["Bucket", "Vehicle Type", "Vehicle Model"]
    else:
        by_cols=["Bucket", "Vehicle Type"]
    
    bucket_counts = group_unique(bucket_df, by_cols=by_cols, id_col="Passenger ID").rename(columns={"Unique Count": "PRMs"})

    full_range = pd.DataFrame({
        "Bucket": pd.date_range(
            start=bucket_counts["Bucket"].min(),
            end=bucket_counts["Bucket"].max(),
            freq="15min"
        )
    })

    if vehicle_model:
        vehicle_types = bucket_counts[["Vehicle Type", "Vehicle Model"]].drop_duplicates()
    else:
        vehicle_types = pd.DataFrame({"Vehicle Type": bucket_counts["Vehicle Type"].unique()})

    full_index = (
        full_range.assign(key=1)
        .merge(vehicle_types.assign(key=1), on="key")
        .drop(columns="key")
    )

    if vehicle_model:
        on = ["Bucket", "Vehicle Type", "Vehicle Model"]
    else:
        on = ["Bucket", "Vehicle Type"]

    bucket_counts = (
        full_index.merge(bucket_counts,
                         on=on,
                         how="left")
                   .fillna({"PRMs": 0})
    )

    return bucket_counts

def rolling_sums_and_labels(bucket_counts, vehicle_model=False):
    
    if vehicle_model:
        groupby_keys=["Vehicle Type", "Vehicle Model"]
    else:
        groupby_keys=["Vehicle Type"]
    
    rolling_df = rolling_sum(
        bucket_counts,
        time_col="Bucket",
        value_col="PRMs",
        window="60min",
        out_col="RollingHourPRMs",
        groupby_keys=groupby_keys
    )
        
    bucket_minutes = 15
    window_minutes = 60
    offset = window_minutes - bucket_minutes  # 45

    rolling_df["WindowStart"] = rolling_df["Bucket"] - pd.Timedelta(minutes=offset)
    rolling_df["WindowEnd"] = rolling_df["Bucket"] + pd.Timedelta(minutes=bucket_minutes)

    rolling_df["WindowLabel"] = (
        rolling_df["WindowStart"].dt.strftime("%H:%M")
        + "–" +
        rolling_df["WindowEnd"].dt.strftime("%H:%M")
    )

    return rolling_df

def peak_rolling_hour_VM(rolling_df):

    peak_results = []

    for (vt, vm), subdf in rolling_df.groupby(["Vehicle Type", "Vehicle Model"]):
        peak_val, w_start, w_end = peak_rolling_window(
            subdf,
            time_col="Bucket",
            roll_col="RollingHourPRMs",
            bucket_minutes=15,
            bucket_count=4,   # 4 × 15min = 60min rolling hour
        )

        peak_results.append({
            "Vehicle Type": vt,
            "Vehicle Model": vm,
            "Peak PRMs in Any Rolling Hour": peak_val,
            "Peak Window Start": w_start,
            "Peak Window End": w_end,
        })

    Peak_df = pd.DataFrame(peak_results)

    return Peak_df

def VM_utilisation(rolling_df):
    utilisation_rows = []

    for (vt, vm), subdf in rolling_df.groupby(["Vehicle Type", "Vehicle Model"]):
        
        total_windows = len(subdf)
        active_windows = (subdf["RollingHourPRMs"] > 0).sum()
        utilisation = active_windows / total_windows if total_windows else 0

        active_avg = (
            subdf.loc[subdf["RollingHourPRMs"] > 0, "RollingHourPRMs"].mean()
            if active_windows else 0
        )

        utilisation_rows.append({
            "Vehicle Type": vt,
            "Vehicle Model": vm,
            "Utilisation %": utilisation * 100,
            "Active-Hour Avg PRMs": active_avg,
            "Total Windows": total_windows,
            "Active Windows": active_windows
        })

    Utilisation_df = pd.DataFrame(utilisation_rows)

    return Utilisation_df

def median_std_VM_PRMs(rolling_df):
    stats_rows = []

    for (vt, vm), subdf in rolling_df.groupby(["Vehicle Type", "Vehicle Model"]):

        all_vals = subdf["RollingHourPRMs"]

        median_all = all_vals.median()
        std_all = all_vals.std(ddof=0)  # population std (change to ddof=1 if you want sample std)

        # --- Active hours ---
        active_vals = all_vals[all_vals > 0]

        median_active = active_vals.median() if len(active_vals) else 0
        std_active = active_vals.std(ddof=0) if len(active_vals) else 0

        stats_rows.append({
            "Vehicle Type": vt,
            "Vehicle Model": vm,
            "Median PRMs (All Hours)": median_all,
            "StdDev PRMs (All Hours)": std_all,
            "Median PRMs (Active Hours)": median_active,
            "StdDev PRMs (Active Hours)": std_active
        })

    Stats_df = pd.DataFrame(stats_rows)

    return Stats_df

def hour_of_day_average(rolling_df, vehicle_model=False):
    if vehicle_model:
        by_cols_A = ["WindowLabel", "Vehicle Type", "Vehicle Model"]
        columns = "Vehicle Model"
        by_cols_B = ["Vehicle Type", "Vehicle Model"]
    else:
        by_cols_A = ["WindowLabel", "Vehicle Type"]
        columns = "Vehicle Type"
        by_cols_B = ["Vehicle Type"]

    A_raw = group_average(
        rolling_df,
        by_cols=by_cols_A,
        value_col="RollingHourPRMs",
        out_col="Avg PRMs per Rolling Hour"
    )

    # 6B. Pivot: WindowLabel × Vehicle Model
    A_pivot = A_raw.pivot(
        index="WindowLabel",
        columns=columns,
        values="Avg PRMs per Rolling Hour"
    ).reset_index()

    # 6C. Overall average PRMs per vehicle model
    B = group_average(
        rolling_df,
        by_cols=by_cols_B,
        value_col="RollingHourPRMs",
        out_col="Avg PRMs per Rolling Hour"
    )

    return A_raw, A_pivot, B

def rolling_hour_vehicle_usage(prm_df: pd.DataFrame, vehicle_model=False):
    """
    Computes rolling-hour PRM usage per Vehicle Type AND optionally per Vehicle Model.

    Returns:
      A_raw       : long-format hour-of-day averages
      A_pivot     : pivot table (WindowLabel × VehicleType/VehicleModel)
      B           : overall average PRMs per rolling hour
      stats_df    : median/std per vehicle model (if vehicle_model=True, else None)
      utilisation_df : utilisation metrics (if vehicle_model=True, else None)
      peak_df     : peak rolling-hour windows (if vehicle_model=True, else None)
    """

    bucket_df = bucket_and_prepare(prm_df, vehicle_model=vehicle_model)

    bucket_counts = prm_per_vehicle_bucket(bucket_df, vehicle_model=vehicle_model)

    rolling_df = rolling_sums_and_labels(bucket_counts, vehicle_model=vehicle_model)

    if vehicle_model:
        peak_df = peak_rolling_hour_VM(rolling_df)

        utilisation_df = VM_utilisation(rolling_df)

        stats_df = median_std_VM_PRMs(rolling_df)
    else:
        peak_df=None
        utilisation_df=None
        stats_df=None
    
    A_raw, A_pivot, B = hour_of_day_average(rolling_df, vehicle_model=vehicle_model)

    return A_raw, A_pivot, B, peak_df, utilisation_df, stats_df

 

#----------------------------------------------------
# STAFF COUNT FUNCTIONS (BY FLIGHT/VEHICLE TYPE)
#----------------------------------------------------


def get_prm_count_per_flight(df, flight_cols):
    """
    Compute unique PRM (Passenger) counts per flight.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset containing passenger records.
    flight_cols : list[str]
        Columns that uniquely identify a flight.

    Returns
    -------
    pandas.DataFrame
        Columns:
            * flight_cols
            PRM Count
    """
    return (
        group_unique(df, by_cols=flight_cols, id_col="Passenger ID")
        .rename(columns={"Unique Count": "PRM Count"})
    )


def get_employee_count_per_flight(df, flight_cols):
    """
    Compute unique employee counts per flight.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset.
    flight_cols : list[str]
        Columns that uniquely identify a flight.

    Returns
    -------
    pandas.DataFrame
        Columns:
            * flight_cols
            Employee Count
    """
    return (
        group_unique(df, by_cols=flight_cols, id_col="Employee")
        .rename(columns={"Unique Count": "Employee Count"})
    )

def get_wch_counts_per_flight(df, flight_cols):
    
    """
    Count wheelchair SSR Code passengers per flight.

    Includes:
    - WCHC Count
    - WCHS Count
    - WCHR Count

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset containing SSR codes.
    flight_cols : list[str]
        Columns identifying a unique flight.

    Returns
    -------
    pandas.DataFrame
        Columns:
            * flight_cols
            WCHC Count
            WCHS Count
            WCHR Count
    """
    
    wch = df[df["SSR Code"].isin(["WCHC", "WCHS", "WCHR"])].copy()

    wchc = (
        group_unique(
            wch[wch["SSR Code"] == "WCHC"],
            by_cols=flight_cols + ["Vehicle Type"],
            id_col="Passenger ID"
        )
        .rename(columns={"Unique Count": "WCHC Count"})
    )

    wchs = (
        group_unique(
            wch[wch["SSR Code"] == "WCHS"],
            by_cols=flight_cols + ["Vehicle Type"],
            id_col="Passenger ID"
        )
        .rename(columns={"Unique Count": "WCHS Count"})
    )

    wchr = (
        group_unique(
            wch[wch["SSR Code"] == "WCHR"],
            by_cols=flight_cols + ["Vehicle Type"],
            id_col="Passenger ID"
        )
        .rename(columns={"Unique Count": "WCHR Count"})
    )

    return (
        wchc.merge(wchs, on=flight_cols + ["Vehicle Type"], how="outer")
            .merge(wchr, on=flight_cols + ["Vehicle Type"], how="outer")
            .fillna(0)
    )



def get_disregard_counts_per_flight(df, flight_cols):
    """
    Count specific Disregard Codes per flight.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset containing Disregard Code values.
    flight_cols : list[str]
        Columns identifying a unique flight.

    Returns
    -------
    pandas.DataFrame
        Columns:
            * flight_cols
            No Assistance Count
            No Show Count
            Passenger self-boarding Count
    """

    noa  = (df["Disregard Code"] == "No Assistance").astype(int)
    nos  = (df["Disregard Code"] == "No Show").astype(int)
    psb  = (df["Disregard Code"] == "Passenger self-boarding").astype(int)

    grouped = (
        df.assign(_noa=noa, _nos=nos, _psb=psb)
          .groupby(flight_cols, dropna=False)[["_noa", "_nos", "_psb"]]
          .sum()
          .reset_index()
          .rename(columns={
              "_noa": "No Assistance Count",
              "_nos": "No Show Count",
              "_psb": "Passenger self-boarding Count"
          })
    )

    return grouped




def get_vehicle_count(df, flight_cols):
    """
    Compute unique vehicle counts per (flight × vehicle type).

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset.
    flight_cols : list[str]
        Columns that uniquely identify a flight.

    Returns
    -------
    pandas.DataFrame
        Columns:
            * flight_cols
            Vehicle Type
            Vehicle Count
    """
    return (
        group_unique(
            df, by_cols=flight_cols + ["Vehicle Type"], id_col="Vehicle Model"
        ).rename(columns={"Unique Count": "Vehicle Count"})
    )


def get_prm_count_per_vehicle(df, flight_cols):
    """
    Compute unique PRM (Passenger) counts per (flight × vehicle type).

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset.
    flight_cols : list[str]
        Columns that uniquely identify a flight.

    Returns
    -------
    pandas.DataFrame
        Columns:
            * flight_cols
            Vehicle Type
            PRM Count per Vehicle
    """
    return (
        group_unique(
            df, by_cols=flight_cols + ["Vehicle Type"], id_col="Passenger ID"
        ).rename(columns={"Unique Count": "PRM Count per Vehicle"})
    )


def get_employee_count_per_vehicle(df, flight_cols):
    """
    Compute unique employee counts per (flight × vehicle type).

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset.
    flight_cols : list[str]
        Columns that uniquely identify a flight.

    Returns
    -------
    pandas.DataFrame
        Columns:
            * flight_cols
            Vehicle Type
            Employee Count per Vehicle
    """
    return (
        group_unique(
            df, by_cols=flight_cols + ["Vehicle Type"], id_col="Employee"
        ).rename(columns={"Unique Count": "Employee Count per Vehicle"})
    )


def get_prm_bin_stats(flight_totals, vehicle_breakdown, flight_cols):
    """
    Compute PRM-count bin statistics:
        - Average employee count per flight
        - Average employee count per vehicle

    Parameters
    ----------
    flight_totals : pandas.DataFrame
        Output of PRM and employee totals per flight.
    vehicle_breakdown : pandas.DataFrame
        Output of vehicle-level breakdown.
    flight_cols : list[str]
        Columns that uniquely identify a flight.

    Returns
    -------
    pandas.DataFrame
        Columns:
            PRM Count
            Avg Employee Count
            Avg Employee Count Per Vehicle
    """
    
# Pivot staff by vehicle type
    pivot = (
        vehicle_breakdown
        .pivot_table(
            index=flight_cols,
            columns="Vehicle Type",
            values="Employee Count per Vehicle",  # raw total per flight per vehicle type
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    # Merge PRM counts
    merged = flight_totals.merge(pivot, on=flight_cols, how="left")

    # Calculate averages for each PRM bin
    result = (
        merged.groupby("PRM Count")
        .agg(
            Avg_Staff_Total=("Employee Count", "mean"),
            Avg_Staff_Ambulift=("Ambulift", "mean"),
            Avg_Staff_MiniBus=("Mini Bus", "mean"),
            Avg_Staff_NoVehicle=("No Vehicle", "mean")
        )
        .reset_index()
    )

    return result



def build_flight_prm_employee_summary(df):
    """
    Build the three main analytical outputs for PRM and employee activity:
        1. Flight totals (PRM Count, Employee Count)
        2. Vehicle breakdown (PRM per vehicle, employees per vehicle, vehicle count)
        3. PRM-count bin statistics (avg employees per PRM count level)

    Parameters
    ----------
    df : pandas.DataFrame
        Full dataset containing:
            Passenger ID
            Employee ID
            Vehicle ID
            Vehicle Type
            Flight identification fields

    Returns
    -------
    tuple of pandas.DataFrame
        (flight_totals, vehicle_breakdown, prm_bin_stats)
    """

    mask_no_vehicle = df["Vehicle Type"].isna()

    df.loc[mask_no_vehicle, "Vehicle Type"] = "No Vehicle"
    df.loc[mask_no_vehicle, "Vehicle Model"] = "No Vehicle"

    mask_unknown_model = df["Vehicle Model"].isna() & ~mask_no_vehicle
    df.loc[mask_unknown_model, "Vehicle Model"] = "Unknown"

    # Define once here
    flight_cols = ["Flight ID", "Day", "Airline Code", "Flight Number"]
   
    
    n_before = len(df)
    df = df.dropna(subset=["Job Start Time", "Job End Time"]).copy()
    n_after = len(df)

    print(f"[INFO] Removed {n_before - n_after:,} rows with null start/end times")


    # ---- Flight totals ----
    prm_counts = get_prm_count_per_flight(df, flight_cols)
    employee_counts = get_employee_count_per_flight(df, flight_cols)
    wchc_s_flight = get_wchc_s_count_per_flight(df, flight_cols)
    disregard_flight = get_disregard_counts_per_flight(df, flight_cols)

    flight_totals = (
        prm_counts#
        .merge(employee_counts, on=flight_cols, how="left")
        .merge(wchc_s_flight, on=flight_cols, how="left")
        .merge(disregard_flight, on=flight_cols, how="left")
    )

    # ---- Vehicle breakdown ----
    prm_by_vehicle = get_prm_count_per_vehicle(df, flight_cols)
    emp_by_vehicle = get_employee_count_per_vehicle(df, flight_cols)
    vehicle_count = get_vehicle_count(df, flight_cols)
    wchc_s_vehicle = get_wchc_s_count_per_vehicle(df, flight_cols)

    vehicle_breakdown = (
        prm_by_vehicle
        .merge(emp_by_vehicle, on=flight_cols + ["Vehicle Type"], how="left")
        .merge(vehicle_count, on=flight_cols + ["Vehicle Type"], how="left")
        .merge(prm_counts.rename(columns={"PRM Count": "PRM Count Total"}), on=flight_cols, how="left")
        .merge(wchc_s_vehicle, on=flight_cols + ["Vehicle Type"], how="left")
    )
    vehicle_breakdown["PRM Count per Vehicle"] = (vehicle_breakdown["PRM Count per Vehicle"]/vehicle_breakdown["Vehicle Count"])
    vehicle_breakdown["Employee Count per Vehicle"] = (vehicle_breakdown["Employee Count per Vehicle"]/vehicle_breakdown["Vehicle Count"])

    # ---- PRM count bin stats ----
    prm_bin_stats = get_prm_bin_stats(
        flight_totals,
        vehicle_breakdown,
        flight_cols
    )

    return flight_totals, vehicle_breakdown, prm_bin_stats
