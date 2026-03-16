#modules/analytics/penetration.py
import pandas as pd
from typing import Tuple


def simple_penetration(numerator_df: pd.DataFrame, numerator_col: str, denominator_df: pd.DataFrame, denominator_col: str) -> Tuple[float, pd.DataFrame]:
    """
    Calculates the penetration rate using the ration of summed numerator_col values to summed denominator_col values.

    Parameters
    ----------
    numerator_df: pandas.DataFrame
        The DataFrame containing the numerator values.
    numerator_col: str
        Column name for numerator values
    denominator_df: pandas.DataFrame
        The DataFrame containing the denominator values.
    denominator_col: str
        column name for denominator values.

    Returns
    ---------
    (float, pandas.DataFrame)
        Penetratin rate and a summary DataFrame
    """

    num = float(pd.to_numeric(numerator_df[numerator_col], errors="coerce").sum())
    den = float(pd.to_numeric(denominator_df[denominator_col], errors="coerce").sum())

    rate = num / den if den > 0 else float("nan")

    return rate, pd.DataFrame({
        "Total_Numerator": [num],
        "Total_Denominator": [den],
        "Penetration Rate": [rate]
    })


def row_penetration(df: pd.DataFrame,
                    numerator_col: str,
                    denominator_col: str,
                    out_col: str = "Penetration Rate") -> pd.DataFrame:
    """
    Computes per-row penetration = numerator_col / denominator_col (vectorised).

    Parameters
    ----------
    df : pandas.DataFrame
        Input table (e.g., monthly PRM summary).
    numerator_col : str
        Column to use as the numerator (e.g., 'Unique Count').
    denominator_col : str
        Column to use as the denominator (e.g., 'Total Pax').
    out_col : str, default 'Penetration Rate'
        Name of the output penetration column.

    Returns
    -------
    pandas.DataFrame
        Copy of df with a new `out_col` containing row-wise penetration values.
    """
    out = df.copy()
    num = pd.to_numeric(out[numerator_col], errors="coerce")
    den = pd.to_numeric(out[denominator_col], errors="coerce")
    out[out_col] = num.divide(den).where(den.ne(0))
    
    return out
