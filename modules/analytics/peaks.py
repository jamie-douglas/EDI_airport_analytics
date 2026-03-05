#modules/analytics/peaks.py

import pandas as pd
from typing import Tuple

def peak_day(series_by_day: pd.Series) -> Tuple[pd.Timestamp, float]:
    """
    Returns the timestamp and value corresponding to the maximum value of a date-indexed series. 

    Parameters
    ----------
    series_by_day: pandas.Series
        Series indexed by date

    Returns
    --------
    (pandas.Timestamp, float)
        Peak day and its corresponding value
    """

    peak_dt = series_by_day.idxmax()
    return pd.to_datetime(peak_dt), float(series_by_day.loc[peak_dt])