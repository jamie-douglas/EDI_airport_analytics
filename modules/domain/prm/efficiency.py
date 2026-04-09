#modules/domain/prm/efficiency.py

import pandas as pd
from typing import Tuple, Optional, List

from modules.utils.dates import to_datetime
from modules.analytics.grouping import group_average, group_sum, group_unique, stats_grouping
from modules.analytics.timeseries import bucket_time, rolling_sum, peak_rolling_window
from modules.analytics.penetration import row_penetration

from modules.config import DOOR_LOCATIONS

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

def wait_time_statistics(flags_df: pd.DataFrame, start=None, end=None):
    """
    Takes a dataframe and returns arrival waiting-time statistics for vehicles before chocks on.  
    
    Parameters
    ---------
    flags_df:
        pandas.DataFrame with columns ["Job Start Time", "Location Arrival DT", "PassengerType", "Vehicle Type"]
    start: str, end: str | default = False
        dates for using if wanting to filter to smaller time period

    Returns
    ---------
    mean, median, min, max waiting time (minutes) and standard deviation by Vehicle Type and by Vehicle Type x Passenger Type

    Drops duplicate Job ID as to not count the same vehicle service time twice

    """
    x = flags_df.copy()


    x = to_datetime(x, ["Job Start Time", "Job End Time", "Location Arrival DT"])

    if start:
        x = x[x["Job Start Time"] >= start]
    if end:
        x = x[x["Job End Time"] < end]

    x.drop_duplicates(subset=["Job ID"], keep='first', inplace=True)

    x["Wait Time"] = (x["Job Start Time"] - x["Location Arrival DT"]).dt.total_seconds() / 60

   
    #-------- Statistics by Vehicle Type ------
    wait_stats_by_vehicle = stats_grouping(
        x,
        by_cols=["Vehicle Type"],
        value_col="Wait Time",
        out_prefix="Wait Time (min)"
    )

    #Stats by Vehicle Type x Passenger Type
    wait_stats_by_vehicle_and_passengertype = stats_grouping(
        x,
        by_cols=["Vehicle Type", "PassengerType"],
        value_col="Wait Time",
        out_prefix="Wait Time (min)"
    )

    return wait_stats_by_vehicle, wait_stats_by_vehicle_and_passengertype

def travel_between_jobs(
        prm_df: pd.DataFrame,
        vehicle_model_col: str = "Vehicle Model",
        vehicle_type_col: str = "Vehicle Type",
        day_col: str = "Day", 
        arrival_col: str = "Location Arrival DT",
        job_end_col: str = "Job End Time", 
        destination_col: str = "Actual DO Location",
        pickup_col: str = "Actual PU Location",
        max_travel_minutes: int = 30) -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    
    """ Calculates empirical travel-time statistics between job locations by analysing
        gaps between consecutive jobs for the same vehicle on the same day.

        For each vehicle-day combination:
        - Jobs are ordered by Job Start Time
        - Consecutive jobs are compared
        - A movement is recorded only if:
                Job Start Time (current) > Job End Time (previous)
                AND gap >= min_gap_minutes
                AND destination != pickup location

        This infers real-world repositioning travel times around the airport. 
        
        
    Parameters
    ----------
    jobs_df : pandas.DataFrame
        Input job-level dataset containing one row per vehicle job.

    
    vehicle_model_col : str, default "Vehicle Model"
        Physical vehicle identifier (used for sequencing jobs).

    vehicle_type_col : str, default "Vehicle Type"
        Operational vehicle class (used for aggregation).


    day_col : str, default "Day"
        Column identifying the operational day.

    arrival_col : str, default "Location Arrival DT"
        Arrival timestamp column.

    job_end_col : str, default "Job End Time"
        Job end timestamp column.

    destination_col : str, default "Actual Destination"
        Location where the previous job ended.

    pickup_col : str, default "Actual Pickup Location"
        Location where the next job starts.

    max_travel_minutes : int, default 20
        Maximum allowable gap (in minutes) between jobs to be treated
        as genuine inter-job travel.

    Returns
    -------
    
    route_stats : pandas.DataFrame
        Aggregated travel-time statistics by:
            [Vehicle Type, From Location, To Location]

    movements : pandas.DataFrame
        Row-level inferred movements between jobs.

    """

    
    
    df = prm_df.copy()

    # --- Ensure datetime fields ---
    df = to_datetime(df, [arrival_col, job_end_col])

    
    # --- Drop rows with any required nulls ---
    required_cols = [
        vehicle_model_col,
        vehicle_type_col,
        day_col,
        arrival_col,
        job_end_col,
        destination_col,
        pickup_col,
    ]

    df = df.dropna(subset=required_cols)


    # --- Sort jobs by physical vehicle ---
    df = df.sort_values(
        by=[vehicle_model_col, day_col, arrival_col]
    )

    # --- Group by physical vehicle + day ---
    group_keys = [vehicle_model_col, day_col]

    df["Prev Job End Time"] = df.groupby(group_keys)[job_end_col].shift(1)
    df["Prev Destination"] = df.groupby(group_keys)[destination_col].shift(1)

    # --- Valid inter-job movements only ---
    movements = df[
        (df["Prev Job End Time"].notna()) &
        (df[arrival_col] > df["Prev Job End Time"])
    ].copy()

    # --- Calculate travel time ---
    movements["Travel Time (min)"] = (
        movements[arrival_col] - movements["Prev Job End Time"]
    ).dt.total_seconds() / 60

    # --- Apply clean-up rules ---
    movements = movements[movements["Travel Time (min)"] <= max_travel_minutes]

    movements["From Location"] = movements["Prev Destination"]
    movements["To Location"] = movements[pickup_col]

    movements = movements[
        movements["From Location"] != movements["To Location"]
    ]

    # --- Aggregate by Vehicle Type (not Model) ---
    route_stats = stats_grouping(
        movements,
        by_cols=[vehicle_type_col, "From Location", "To Location"],
        value_col="Travel Time (min)",
        out_prefix="Travel Time (min)",
    )

    # --- Add observation count ---
    route_counts = (
        movements
        .groupby(
            [vehicle_type_col, "From Location", "To Location"],
            dropna=False,
        )
        .size()
        .reset_index(name="Observation Count")
    )

    route_stats = route_stats.merge(
        route_counts,
        on=[vehicle_type_col, "From Location", "To Location"],
        how="left",
    )

    return route_stats, movements


def build_stand_performance_metrics(
    job_df: pd.DataFrame,
    movements_df: pd.DataFrame,
    *,
    stand_col: str = "Stand",
    vehicle_type_col: str = "Vehicle Type",
    job_start_col: str = "Job Start Time",
    job_end_col: str = "Job End Time",
    arrival_col: str = "Location Arrival DT",
    do_location_col: str = "Actual DO Location",
    max_travel_minutes: int = 30,
) -> pd.DataFrame:
    """
    Builds stand-level performance metrics by combining:

    1. Job-level metrics at the stand
       - Wait time at stand  = Job Start - Actual Arrival
       - Job duration at stand = Job End - Job Start

    2. Travel FROM stand TO doors (job-based)
       - Stand → CTA / IA1 / IA2 / Dom Arr Doors

    3. Travel FROM doors TO stand (movement-based)
       - CTA / IA1 / IA2 / Dom Arr → Stand

    All metrics are aggregated by:
        [Stand, Vehicle Type, Category, Counterparty]

    Output is sorted by Stand (numeric order) then Vehicle Type.

    Returns
    -------
    pandas.DataFrame
        Columns:
        Stand | Vehicle Type | Metric | Counterparty | Mean | Median | Min | Max | Std Dev
    """

    # -------------------------------------------------
    # PREP & CLEAN
    # -------------------------------------------------
    df = job_df.copy()

    df = to_datetime(df, [job_start_col, job_end_col, arrival_col])

    df = df.dropna(
        subset=[
            stand_col,
            vehicle_type_col,
            job_start_col,
            job_end_col,
            arrival_col,
        ]
    )

    outputs = []
    # -------------------------------------------------
    # 1) JOB-LEVEL METRICS AT STAND
    # -------------------------------------------------
    df["Wait Time @ Stand (min)"] = (
        df[job_start_col] - df[arrival_col]
    ).dt.total_seconds() / 60

    df["Job Duration @ Stand (min)"] = (
        df[job_end_col] - df[job_start_col]
    ).dt.total_seconds() / 60

    for metric_col in [
        "Wait Time @ Stand (min)",
        "Job Duration @ Stand (min)",
    ]:
        long = df[[stand_col, vehicle_type_col, metric_col]].rename(
            columns={metric_col:"Value"}
        )
        stats = stats_grouping(
            long,
            by_cols=[stand_col, vehicle_type_col],
            value_col="Value",
            out_prefix=None,
        )
        stats["Metric"] = metric_col
        stats["Counterparty"] = None
        outputs.append(stats)

    # -------------------------------------------------
    # 2) STAND → DOORS (JOB + TRAVEL TIME)
    # -------------------------------------------------
    stand_to_doors = df[
        df[do_location_col].isin(DOOR_LOCATIONS)
    ].copy()


    std_long = stand_to_doors.assign(
        Metric="Stand → Door Job + Travel Time (min)",
        Counterparty=stand_to_doors[do_location_col],
        Value = stand_to_doors["Job Duration @ Stand (min)"]
    )[
        [stand_col, vehicle_type_col, "Metric", "Counterparty", "Value"]
    ]

    std_stats = stats_grouping(
        std_long,
        by_cols=[stand_col, vehicle_type_col, "Metric", "Counterparty"],
        value_col="Value",
        out_prefix=None,
    )

    outputs.append(std_stats)

    # -------------------------------------------------
    # 3) DOORS → STAND (MOVEMENT-BASED TRAVEL)
    # -------------------------------------------------
    door_to_stand = movements_df[
        movements_df["From Location"].isin(DOOR_LOCATIONS)
    ].copy()

    door_to_stand = door_to_stand[
        door_to_stand["Travel Time (min)"] <= max_travel_minutes
    ]

    door_to_stand["Stand"] = (
        door_to_stand["To Location"].astype(str).str.extract(r"^[A-Za-z]*([0-9]+[A-Za-z]?)")[0] # Extract numeric part of stand code
    )

    door_to_stand = door_to_stand.dropna(subset=["Stand"])

    dts_long = door_to_stand.assign(
        Metric="Door → Stand Travel Time (min)",
        Counterparty=door_to_stand["From Location"],
        Value=door_to_stand["Travel Time (min)"]
    )[
        ["Stand", vehicle_type_col, "Metric", "Counterparty", "Value"]
    ]

    dts_stats = stats_grouping(
        dts_long,
        by_cols=["Stand", vehicle_type_col, "Metric", "Counterparty"],
        value_col="Value",
        out_prefix=None,
    )

    outputs.append(dts_stats)

    # -------------------------------------------------
    # COMBINE ALL METRICS
    # -------------------------------------------------
    final = pd.concat(outputs, ignore_index=True)

    #Sort by numeric stand order
   
    final["_StandNum"] = (
        final["Stand"].astype(str).str.extract(r"(\d+)").astype(float)
    )
    final["_StandSuffix"] = (
        final["Stand"].astype(str).str.extract(r"\d+([A-Za-z]?)").fillna("")
    )

    final = final.sort_values(
        by=[
            "_StandNum",
            "_StandSuffix",
            vehicle_type_col,
            "Metric",
            "Counterparty",
        ]
    ).drop(columns=["_StandNum", "_StandSuffix"])


    return final



def arrival_time_statistics(flags_df: pd.DataFrame, flight_df: pd.DataFrame, start=None, end=None):
    """
    Takes a dataframe and returns arrival waiting-time statistics for vehicles before chocks on.  
    
    Parameters
    ---------
    flags_df:
        pandas.DataFrame with columns ["Flight Number", "Day", "Airline Code", "Location Arrival DT", "PassengerType", "Vehicle Type"]
    flight_df:
        pandas.DataFrame with columns ["Flight Number", "Day", "Airline Code", "Flight Number", "Chocks DT"]
    start: str, end: str | default = False
        dates for using if wanting to filter to smaller time period

    Returns
    ---------
    mean, median, min, max arrival time (minutes) and standard deviation by Vehicle Type and by Vehicle Type x Passenger Type

    Drops duplicate Job ID as to not count the same vehicle service time twice

    """
    x = flags_df.copy()
    z = flight_df.copy()

    x = to_datetime(x, ["Job Start Time", "Job End Time", "Location Arrival DT"])
    y = to_datetime(z, ["Chocks DT"])

    if start:
        x = x[x["Job Start Time"] >= start]
    if end:
        x = x[x["Job End Time"] < end]

    prm_flight_merge = x.merge(y, on=["Flight Number", "Day", "Airline Code"], how="left") 

    prm_flight_merge.drop_duplicates(subset=["Job ID"], keep='first', inplace=True)

    prm_flight_merge["Arrival Time before Chocks"] = (prm_flight_merge["Chocks DT"] - prm_flight_merge["Location Arrival DT"]).dt.total_seconds() / 60
   
    #-------- Statistics by Vehicle Type ------
    stats_by_vehicle = stats_grouping(
        prm_flight_merge,
        by_cols=["Vehicle Type"],
        value_col="Arrival Time before Chocks",
        out_prefix="Arrival Time before Chocks (min)"
    )

    #Stats by Vehicle Type x Passenger Type
    stats_by_vehicle_and_passengertype = stats_grouping(
        prm_flight_merge,
        by_cols=["Vehicle Type", "PassengerType"],
        value_col="Arrival Time before Chocks",
        out_prefix="Arrival Time before Chocks (min)"
    )

    return stats_by_vehicle, stats_by_vehicle_and_passengertype

def avg_travel_time_by_stand_and_location(
    job_df: pd.DataFrame,
    vehicle_type: str
) -> pd.DataFrame:
    """
    Returns:
    StandCode | Avg time to CTADoors | IA1Doors | IA2Doors | DomArrDoors
    """

    DO_LOCATIONS = [
        "CTA Doors",
        "IA1 Doors",
        "IA2 Doors",
        "Dom Arr Doors",
    ]
    print(job_df[job_df["Actual DO Location"].isin(DO_LOCATIONS)])

    sub = job_df[
        (job_df["Vehicle Type"] == vehicle_type) &
        (job_df["Actual DO Location"].isin(DO_LOCATIONS))
    ].copy()

    pivot = (
        sub
        .groupby(["Stand", "Actual DO Location"], dropna=False)["Travel Time (mins)"]
        .mean()
        .reset_index()
        .pivot(
            index="Stand",
            columns="Actual DO Location",
            values="Travel Time (mins)"
        )
        .reset_index()
    )

    return pivot.rename(columns={
        "Stand": "StandCode",
        "CTADoors": "Avg travel time to CTADoors",
        "IA1Doors": "Avg travel time to IA1Doors",
        "IA2Doors": "Avg travel time to IA2Doors",
        "DomArrDoors": "Avg travel time to DomArrDoors",
    })

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

    utilisation_df = pd.DataFrame(utilisation_rows)

    
    # ==================================================
    # SUMMARY CALCULATIONS (weighted, active hours only)
    # ==================================================

    summaries = []

    for vt_label, vt_filter in [
        ("ALL AMBULIFTS", rolling_df["Vehicle Type"] == "Ambulift"),
        ("ALL MINIBUSES", rolling_df["Vehicle Type"] == "Mini Bus"),
        ("ALL VEHICLES", rolling_df["Vehicle Type"].isin(["Ambulift", "Mini Bus"]))
    ]:
        sub = rolling_df.loc[vt_filter].copy()
        active_sub = sub[sub["RollingHourPRMs"] > 0]

        total_active_windows = len(active_sub)
        total_windows = len(sub)

        avg_prms_active = (
            active_sub["RollingHourPRMs"].sum() / total_active_windows
            if total_active_windows else 0
        )

        utilisation = (
            total_active_windows / total_windows
            if total_windows else 0
        )

        summaries.append({
            "Vehicle Type": vt_label,
            "Vehicle Model": "ALL",
            "Utilisation %": utilisation * 100,
            "Active-Hour Avg PRMs": avg_prms_active,
            "Total Windows": total_windows,
            "Active Windows": total_active_windows
        })

    summary_df = pd.DataFrame(summaries)

    # Combine detailed + summary
    utilisation_df = pd.concat(
        [utilisation_df, summary_df],
        ignore_index=True
    )

    return utilisation_df

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
# PER FLIGHT COUNT FUNCTIONS (BY FLIGHT/VEHICLE TYPE)
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

def get_secondarySSR_count_penrate(ecac_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute secondary SSR counts and penetration rates within each primary PRM group.

    The function normalises the pipe-delimited 'PRM Secondary String' column
    (e.g. "WCMP|WCHS|MEDA") by exploding it into one secondary SSR per row,
    then calculates:
        - unique passenger counts per (Primary PRM, Secondary SSR)
        - total unique passengers per Primary PRM
        - penetration rate of each secondary SSR within its primary PRM

    Parameters
    ----------
    ecac_df : pandas.DataFrame
        Input PRM dataset containing at minimum the following columns:
            - 'Passenger ID'
            - 'Primary PRM'
            - 'PRM Secondary String'

    Returns
    -------
    pandas.DataFrame
        A DataFrame with one row per Primary PRM / Secondary SSR combination,
        containing:
            - Primary PRM
            - Secondary SSR
            - Secondary Count      (unique passengers with this secondary SSR)
            - Primary Total        (total unique passengers with this primary PRM)
            - Penetration Rate     (Secondary Count / Primary Total)
    """

    df = ecac_df.copy()

    # ----------------------------
    # Normalise secondary SSRs
    # ----------------------------
    df = df.dropna(subset=["PRM Secondary String"])
    df["Secondary SSR"] = df["PRM Secondary String"].str.split(r"[,\|]", regex=True)
    df = df.explode("Secondary SSR")

    assert not df["Secondary SSR"].str.contains(",", na=False).any()

    
    df["Secondary SSR"] = df["Secondary SSR"].str.strip()
    df = df[df["Secondary SSR"] != ""]
    df = df[df["Secondary SSR"] != df["Primary PRM"]]

    
    print(
        df["Secondary SSR"]
        .value_counts()
        .head(20)
    )



    # ----------------------------
    # Count per Primary / Secondary
    # ----------------------------
    sec_counts = group_unique(
        df,
        by_cols=["Primary PRM", "Secondary SSR"],
        id_col="PassengerID"
    ).rename(columns={
        "Unique Count": "Secondary Count"
    })

    # ----------------------------
    # Primary PRM totals
    # ----------------------------
    prim_totals = group_unique(
        ecac_df,
        by_cols=["Primary PRM"],
        id_col="PassengerID"
    ).rename(columns={
        "Unique Count": "Primary Total"
    })

    # ----------------------------
    # Merge + penetration
    # ----------------------------
    out = sec_counts.merge(
        prim_totals,
        on="Primary PRM",
        how="left"
    )

    out = row_penetration(
        out,
        numerator_col="Secondary Count",
        denominator_col="Primary Total",
        out_col="Penetration Rate"
    )

    return out.sort_values(
        ["Primary PRM", "Secondary Count"],
        ascending=[True, False]
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
    wchc_s_flight = get_wch_counts_per_flight(df, flight_cols)
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
    wchc_s_vehicle = get_wch_counts_per_flight(df, flight_cols)

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

#----------------AVERAGE TRAVEL TIMES--------------------

def build_job_level_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate to one row per Job ID (vehicle movement),
    removing duplicated employee rows.
    """

    job_cols = [
        "Job ID",
        "Vehicle Type",
        "Stand",
        "Actual DO Location",
        "Job Start Time",
        "Job End Time",
    ]

    # Keep one record per Job ID
    job_df = (
        df[job_cols]
        .drop_duplicates(subset=["Job ID"])
        .copy()
    )

    # Compute travel time in minutes
    job_df["Travel Time (mins)"] = (
        (job_df["Job End Time"] - job_df["Job Start Time"])
        .dt.total_seconds() / 60
    )

    # Defensive: remove invalid / negative durations
    job_df = job_df[job_df["Travel Time (mins)"] > 0]

    return job_df


