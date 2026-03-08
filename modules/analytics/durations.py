#modules/analytics/durations.py
import pandas as pd

def mean_duration_seconds(df: pd.DataFrame, start_col: str, end_col: str, max_minutes: float | None = None) -> float:
    """
    Calculates the mean duration in seconds between two datetime columns, optionally excluding duratioons longer than a specified number of minutes

    Parameters
    ----------
    df: pandas.DataFrame
        Input DataFrame
    start_col: str
        The name of the column representing the start time.
    end_col: str
        The name of the column representing the end time.
    max_minutes: float, optional
        Maximum allowed duration (in minutes). Durations above this are excluded

    Returns
    ---------
    float
        The mean duration in seconds between the start and end times.
    """
    
    x = df.copy()

    x[start_col] = pd.to_datetime(x[start_col], errors="coerce")
    x[end_col] = pd.to_datetime(x[end_col], errors="coerce")

    secs= (x[end_col] - x[start_col]).dt.total_seconds()
    if max_minutes is not None:
        secs = secs[secs <= max_minutes * 60]

    return float(secs.mean())

def duration_validation_summary(df: pd.DataFrame, start_col: str, end_col: str, recorded_secs_col: str) -> pd.DataFrame:
    """
    Validates computed duraations against a recored duration column

    Parameters
    ----------
    df: pandas.DataFrame
        Input DataFrame
    start_col: str
        Recorded start timestamp column
    end_col: str
        Recorded end timestamp column
    recorded_secs_col: str
        Column containing recorded duration in seconds to compare against

    Returns
    ---------
    pandas.DataFrame
        A summary DataFrame containing the average actual duration, invalid count, and total row count
    """

    x = df.copy()
    x[start_col] = pd.to_datetime(x[start_col], errors="coerce")
    x[end_col] = pd.to_datetime(x[end_col], errors="coerce")

    actual = (x[end_col] - x[start_col]).dt.total_seconds()
    recorded = pd.to_numeric(x[recorded_secs_col], errors="coerce")

    mismatch = (actual != recorded)

    return pd.DataFrame({
        "Metric": ["Average Duration (secs)", "Invalid Duration Count", "Total Transactions"],
        "Value": [actual.mean(), int(mismatch.sum()), len(x)]
    })
