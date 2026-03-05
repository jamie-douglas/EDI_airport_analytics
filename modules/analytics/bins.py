#modules/analytics/bins.py
import numpy as np
import pandas as pd
from typing import Sequence

def histogram_counts(values: pd.Series, bins: Sequence[float]) -> pd.DataFrame:
    """
    Computes histogram counts, returning bin start, end, midpoint and count

    Parameters
    ----------
    values: pandas.Series
        Numeric values to bin
    bins: sequence of float
        Bin edges
    
    Returns
    ---------
    pandas.DataFrame
        DataFrame with columns:
        ['Bin Start', 'Bin End', 'Bin Midpoint', 'Count']
    """

    
    vals = pd.to_numeric(values, errors="coerce").dropna()
    counts, edges = np.histogram(vals, bins=bins)
    mids = [(edges[i] + edges[i+1]) / 2 for i in range(len(edges) - 1)]

    return pd.DataFrame({
        "Bin Start": edges[:-1],
        "Bin End": edges[1:],
        "Bin Midpoint": mids,
        "Count": counts
        })
