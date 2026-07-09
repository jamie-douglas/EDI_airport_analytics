# modules/analytics/penetration.py

"""
Penetration analytics helpers.

Includes:
- simple_penetration
- row_penetration
- build_penetration_and_ssr_mix
"""

import pandas as pd
from typing import Tuple

from prm_opt.ingest_s25 import ingest_s25, load_flight_data
from prm_opt.config import WCHS_OWN_CHAIR_PROB


def simple_penetration(
    numerator_df: pd.DataFrame,
    numerator_col: str,
    denominator_df: pd.DataFrame,
    denominator_col: str
) -> Tuple[float, pd.DataFrame]:
    """
    Calculates the penetration rate using the ratio of summed numerator_col values
    to summed denominator_col values.

    Parameters
    ----------
    numerator_df : pandas.DataFrame
        The DataFrame containing the numerator values.
    numerator_col : str
        Column name for numerator values.
    denominator_df : pandas.DataFrame
        The DataFrame containing the denominator values.
    denominator_col : str
        Column name for denominator values.

    Returns
    -------
    tuple
        Penetration rate and a summary DataFrame.
    """

    num = float(pd.to_numeric(numerator_df[numerator_col], errors="coerce").sum())
    den = float(pd.to_numeric(denominator_df[denominator_col], errors="coerce").sum())

    rate = num / den if den > 0 else float("nan")

    return rate, pd.DataFrame({
        "Total_Numerator": [num],
        "Total_Denominator": [den],
        "Penetration Rate": [rate]
    })


def row_penetration(
    df: pd.DataFrame,
    numerator_col: str,
    denominator_col: str,
    out_col: str = "Penetration Rate"
) -> pd.DataFrame:
    """
    Computes per-row penetration = numerator_col / denominator_col.

    Parameters
    ----------
    df : pandas.DataFrame
        Input table, e.g. monthly PRM summary.
    numerator_col : str
        Column to use as the numerator, e.g. 'Unique Count'.
    denominator_col : str
        Column to use as the denominator, e.g. 'Total Pax'.
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


def build_penetration_and_ssr_mix(
    s25_start: str,
    s25_end: str,
):
    """
    Returns:
      penetration_rates : Airline Code x CountryName penetration
      ssr_mix           : Airline Code x CountryName x SSR Code mix
    """

    # =====================================================
    # Load data
    # =====================================================
    df_prm = ingest_s25(s25_start, s25_end)
    df_flights = load_flight_data(s25_start, s25_end)

    # =====================================================
    # Penetration (PRM pax / total pax)
    # =====================================================
    # PRM passengers (unique people)
    prm_counts = (
        df_prm.groupby(["Airline Code", "CountryName"])["Passenger ID"]
        .nunique()
        .reset_index(name="prm_count")
    )

    # Total passengers (FLIGHT-level, no duplication)
    pax_totals = (
        df_flights.groupby(["Airline Code", "CountryName"])["Pax"]
        .sum()
        .reset_index(name="pax")
    )

    penetration = prm_counts.merge(
        pax_totals,
        on=["Airline Code", "CountryName"],
        how="left",
    ).fillna({"pax": 0})

    penetration["penetration"] = penetration.apply(
        lambda r: r.prm_count / r.pax if r.pax > 0 else 0.0,
        axis=1,
    )

    # =====================================================
    # SSR mix (within PRM passengers)
    # =====================================================
    ssr = (
        df_prm.groupby(["Airline Code", "CountryName", "SSR Code"])
        .size()
        .reset_index(name="count")
    )

    ssr = ssr.merge(
        ssr.groupby(["Airline Code", "CountryName"])["count"]
        .sum()
        .reset_index(name="total"),
        on=["Airline Code", "CountryName"],
    )

    ssr["share"] = ssr["count"] / ssr["total"]

    def own_chair_rate(code: str) -> float:
        if code == "WCHC":
            return 1.0
        if code == "WCHS":
            return float(WCHS_OWN_CHAIR_PROB)
        return 0.0

    ssr["own_chair_rate"] = ssr["SSR Code"].apply(own_chair_rate)

    return penetration, ssr