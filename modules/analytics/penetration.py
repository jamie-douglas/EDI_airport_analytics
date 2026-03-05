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