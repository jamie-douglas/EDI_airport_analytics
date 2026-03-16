#modules/domain/prm/ambulift.py

import pandas as pd

from modules.analytics.timeseries import bucket_time
from modules.analytics.grouping import group_unique

def group_ambulift_by_time(prm_df: pd.DataFrame, time_col: str, freq: str, out_col: str = "TimeBucket") -> pd.DataFrame:
    """
    Groups Ambulify records by a time buck and counts unique passengers
    
    Parameters
    ----------
    prm_df: pandas.DataFrame
        Clean PRM dataset including 'Vehicle Type' with string "ambulift" for ambulift jobs, Passenger ID, time_col
    time_col: str
        Name of the datetime column to bucket
    freq: str
        Pandas frequency alias for flooring e.g. "D" (Day), "M" (Month), "Y" (Calendar Year), "15min" (!5 minutes)
    out_col: str, default "TimeBucket
        Name of the output bucket column
        
    Returns
    ----------
    pandas.DataFrame containing out_col, Ambulift PRMs (unique "Passenger ID" per bucket)
    """

    amb = prm_df[prm_df["Vehicle Type"] == "Ambulift"]
    if amb.empty:
        return pd.DataFram({out_col: pd.Series(dtype="datetime64[ns]"), "Ambulift PRMs": pd.Series(dtype="int64")})
    
    bucketed = bucket_time(amb, time_col=time_col, freq=freq, out_col=out_col)
    out = group_unique(bucketed, [out_col], id_col="Passenger ID")
    out = out.rename(columns = {"Unique Count": "Ambulift PRMs"})

    return out

