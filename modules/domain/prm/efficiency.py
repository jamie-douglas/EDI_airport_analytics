import pandas as pd

from modules.utils.dates import to_datetime
from modules.analytics.grouping import group_average, group_sum, group_unique
from modules.analytics.timeseries import bucket_time, rolling_sum, peak_rolling_window

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

def rolling_hour_vehicle_usage(prm_df: pd.DataFrame):
    """
    Computes:
    A) Hour-of day average rolling-hour PRM rates per Vehicle Type
    B) Overall rolling-hour average PRM rates per Vehicle Type (entire period)
    
    Returns
    --------
    A_raw: long-format hour of day averages
    A_pivot: pivoted wide table with Vehicle Types as columns
    B: overall averages per Vehicle Type
    """

    #1. Bucket into 15 minute intervals
    df= bucket_time(prm_df, time_col="Job Start Time", freq="15min", out_col="Bucket")

    #Ensure Vehicle Type is filled
    df["Vehicle Type"] = df["Vehicle Type"].fillna("No Vehicle")

    #2. Count PRMs per vehicle type in each bucket
    bucket_counts=group_unique(df, by_cols=["Bucket", "Vehicle Type"], id_col="Passenger ID").rename(columns={"Unique Count": "PRMs"})

    #create continuous bucket grid before rolling
    
    full_range = pd.DataFrame({
        "Bucket": pd.date_range(
            start=bucket_counts["Bucket"].min(),
            end=bucket_counts["Bucket"].max(),
            freq="15min"
        )
    })

    vehicle_types = bucket_counts["Vehicle Type"].unique()

    full_index = (
        full_range.assign(key=1)
        .merge(pd.DataFrame({"Vehicle Type": vehicle_types, "key":1}), on="key")
        .drop(columns="key")
    )

    bucket_counts = (
        full_index.merge(bucket_counts, on=["Bucket", "Vehicle Type"], how="left")
                .fillna({"PRMs": 0})
    )

    #3. Compute rolling 1-hour sums (60 minutes, i.e., 4 x15 min buckets)
    rolling = rolling_sum(
        bucket_counts, 
        time_col="Bucket",
        value_col="PRMs",
        window="60min",
        out_col="RollingHourPRMs",
        groupby_keys=["Vehicle Type"]
    )
    # --- Add rolling hour start, end, and label --

    bucket_minutes = 15
    window_minutes = 60
    offset = window_minutes - bucket_minutes  # 45

    rolling["WindowStart"] = rolling["Bucket"] - pd.Timedelta(minutes=offset)
    rolling["WindowEnd"] = rolling["Bucket"] + pd.Timedelta(minutes=bucket_minutes)

    rolling["WindowLabel"] = (
        rolling["WindowStart"].dt.strftime("%H:%M")
        + "–" +
        rolling["WindowEnd"].dt.strftime("%H:%M")
    )


    #4A Hour of day average
    A_raw = group_average(
        rolling,
        by_cols=["WindowLabel", "Vehicle Type"],
        value_col="RollingHourPRMs",
        out_col="Avg PRMs per Rolling Hour"
    )

    # Pivot A (Hour x Vehicle Type)
    A_pivot = A_raw.pivot(
        index="WindowLabel",
        columns="Vehicle Type",
        values="Avg PRMs per Rolling Hour"
    ).reset_index()

    #Reorder vehicle types
    vehicle_order = ["Ambulift", "Mini Bus", "No Vehicle"]
    cols = ["WindowLabel"] + [c for c in vehicle_order if c in A_pivot.columns]
    A_pivot = A_pivot[cols]

    #4B OVerall average per vehicle type
    B = group_average(
        rolling, 
        by_cols=["Vehicle Type"],
        value_col="RollingHourPRMs",
        out_col="Avg PRMs per Rolling Hour"
    )

    return A_raw, A_pivot, B


def rolling_hour_vehicle_model_usage(prm_df: pd.DataFrame):
    """
    Computes rolling-hour PRM usage per Vehicle Type AND per individual Vehicle Model.

    Returns:
      A_raw   : long-format table (WindowLabel x Vehicle Model)
      A_pivot : pivot table (WindowLabel rows, Vehicle Models columns)
      B       : overall average PRMs/hour per Vehicle Model
    """

    # 1. Bucket into 15-minute intervals
    df = bucket_time(
        prm_df,
        time_col="Job Start Time",
        freq="15min",
        out_col="Bucket"
    )

    # Ensure Vehicle Type and Model fields exist
    df["Vehicle Type"] = df["Vehicle Type"].fillna("No Vehicle")
    df["Vehicle Model"] = df["Vehicle Model"].fillna("Unknown")


    # 2. Count unique PRMs per bucket x vehicle type x vehicle model
    bucket_counts = group_unique(
        df,
        by_cols=["Bucket", "Vehicle Type", "Vehicle Model"],
        id_col="Passenger ID"
    ).rename(columns={"Unique Count": "PRMs"})


    # 3. Create a continuous 15-min grid
    full_range = pd.DataFrame({
        "Bucket": pd.date_range(
            start=bucket_counts["Bucket"].min(),
            end=bucket_counts["Bucket"].max(),
            freq="15min"
        )
    })

    vehicle_combos = (
        bucket_counts[["Vehicle Type", "Vehicle Model"]]
        .drop_duplicates()
    )

    full_index = (
        full_range.assign(key=1)
        .merge(vehicle_combos.assign(key=1), on="key")
        .drop(columns="key")
    )

    bucket_counts = (
        full_index.merge(bucket_counts,
                         on=["Bucket", "Vehicle Type", "Vehicle Model"],
                         how="left")
                   .fillna({"PRMs": 0})
    )

    # 4. Rolling 1-hour sum per vehicle model
    rolling = rolling_sum(
        bucket_counts,
        time_col="Bucket",
        value_col="PRMs",
        window="60min",
        out_col="RollingHourPRMs",
        groupby_keys=["Vehicle Type", "Vehicle Model"]
    )

    # 5. Add rolling window labels
    bucket_minutes = 15
    window_minutes = 60
    offset = window_minutes - bucket_minutes  # 45

    rolling["WindowStart"] = rolling["Bucket"] - pd.Timedelta(minutes=offset)
    rolling["WindowEnd"] = rolling["Bucket"] + pd.Timedelta(minutes=bucket_minutes)

    rolling["WindowLabel"] = (
        rolling["WindowStart"].dt.strftime("%H:%M")
        + "–" +
        rolling["WindowEnd"].dt.strftime("%H:%M")
    )

    
    # 5.--- Peak rolling-hour per vehicle model ---
    peak_results = []

    for (vt, vm), subdf in rolling.groupby(["Vehicle Type", "Vehicle Model"]):
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

    
    # --- UTILISATION & active-hour throughput per vehicle ---
    utilisation_rows = []

    for (vt, vm), subdf in rolling.groupby(["Vehicle Type", "Vehicle Model"]):
        
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



    # 6A. Hour-of-day averages (per Vehicle Model)
    A_raw = group_average(
        rolling,
        by_cols=["WindowLabel", "Vehicle Type", "Vehicle Model"],
        value_col="RollingHourPRMs",
        out_col="Avg PRMs per Rolling Hour"
    )

    # 6B. Pivot: WindowLabel × Vehicle Model
    A_pivot = A_raw.pivot(
        index="WindowLabel",
        columns="Vehicle Model",
        values="Avg PRMs per Rolling Hour"
    ).reset_index()

    # 6C. Overall average PRMs per vehicle model
    B = group_average(
        rolling,
        by_cols=["Vehicle Type", "Vehicle Model"],
        value_col="RollingHourPRMs",
        out_col="Avg PRMs per Rolling Hour"
    )

    return A_raw, A_pivot, B, Peak_df, Utilisation_df
