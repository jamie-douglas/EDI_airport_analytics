#modules/domain/prm/ambulift.py

import pandas as pd
from typing import Optional, Union

from modules.utils.dates import assign_effective_month
from modules.analytics.timeseries import bucket_time
from modules.analytics.grouping import group_unique
from modules.analytics.penetration import row_penetration


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
        return pd.DataFrame({out_col: pd.Series(dtype="datetime64[ns]"), "Ambulift PRMs": pd.Series(dtype="int64")})
    
    bucketed = bucket_time(amb, time_col=time_col, freq=freq, out_col=out_col)
    out = group_unique(bucketed, [out_col], id_col="Passenger ID")
    out = out.rename(columns = {"Unique Count": "Ambulift PRMs"})

    return out

def ambulift_breakdowns(
    prm_df: pd.DataFrame,
    vehicle_col: str = "Vehicle Type",
    ssr_col: str = "SSR Code",
    booking_col: str = "Adhoc Or Planned",
    id_col: str = "Passenger ID",
) -> dict[str, pd.DataFrame]:
    """
    Compute Ambulift user breakdowns by SSR Code and by Adhoc/Planned.

    Parameters
    ----------
    prm_df : pandas.DataFrame
        Full PRM dataset returned from load_prm_data().
    vehicle_col : str, default "Vehicle Type"
        Column identifying service type. Ambulift jobs use value "Ambulift".
    ssr_col : str, default "SSR Code"
        Column containing SSR category.
    booking_col : str, default "Adhoc Or Planned"
        Indicates whether the job was Ad-Hoc or Planned.
    id_col : str, default "Passenger ID"
        Unique passenger identifier.

    Returns
    -------
    dict[str, pandas.DataFrame]
        {
            "by_ssr":  [SSR Code, Ambulift Users, Total Ambulift Users, % of Ambulift Users],
            "by_booking": [Adhoc Or Planned, Ambulift Users, Total Ambulift Users, % of Ambulift Users]
        }

    Notes
    -----
    • Universe is restricted to rows where Vehicle Type == 'Ambulift'.
    • Percentages are relative to total unique ambulift users.
    """

    amb = prm_df[prm_df[vehicle_col] == "Ambulift"].copy()

    if amb.empty:
        return {
            "by_ssr": pd.DataFrame(columns=[ssr_col, "Ambulift Users", "% of Ambulift Users"]),
            "by_booking": pd.DataFrame(columns=[booking_col, "Ambulift Users", "% of Ambulift Users"]),
        }

    denom = amb[id_col].nunique()

    # ---By SSR Code ---
    by_ssr = group_unique(amb, [ssr_col], id_col=id_col).rename(columns={"Unique Count": "Ambulift Users"}).copy()
    by_ssr["_denom_total"] = float(denom)
    by_ssr = row_penetration(by_ssr, "Ambulift Users", "_denom_total", "% of Ambulift Users")
    by_ssr["% of Ambulift Users"] *= 100.0
    by_ssr["Total Ambulift Users"] = int(denom)
    by_ssr = by_ssr.drop(columns=["_denom_total"]).sort_values(ssr_col).reset_index(drop=True)

    # --- By Adhoc or planned ---
    by_booking = group_unique(amb, [booking_col], id_col=id_col).rename(columns={"Unique Count": "Ambulift Users"}).copy()
    by_booking["_denom_total"] = float(denom)
    by_booking = row_penetration(by_booking, "Ambulift Users", "_denom_total", "% of Ambulift Users")
    by_booking["% of Ambulift Users"] *= 100.0
    by_booking["Total Ambulift Users"] = int(denom)
    by_booking = by_booking.drop(columns=["_denom_total"]).sort_values(booking_col).reset_index(drop=True)

    return {"by_ssr": by_ssr, "by_booking": by_booking}


def group_ambulift_by_effective_month(
    prm_df: pd.DataFrame,
    *,
    id_col: str = "Passenger ID",
    date_col: str = "Operation Date",
    ad_col: str = "A/D",
    vehicle_col: str = "Vehicle Type",
    out_col: str = "Month",
    window_start: Optional[Union[str, pd.Timestamp]] = None,
) -> pd.DataFrame:
    """
    Group Ambulift jobs by Effective Month and count unique Ambulift passengers.

    Effective Month Logic
    ---------------------
    A job is attributed to the *earlier* month when:
        • It and the previous job for the same Passenger ID occur one day apart
          (e.g. 31 Jan → 1 Feb), AND
        • The month changes across those days, AND
        • The Arrival/Departure indicator (A/D) is the same, indicating the same
          operational movement rather than a true return journey.

    Parameters
    ----------
    prm_df : pandas.DataFrame
        Full PRM dataset containing at least:
            - 'Passenger ID'
            - date_col
            - ad_col
            - vehicle_col
    id_col : str, default "Passenger ID"
        Unique passenger identifier column.
    date_col : str, default "Operation Date"
        Date column used to derive Effective Month.
    ad_col : str, default "A/D"
        Arrival/Departure indicator.
    vehicle_col : str, default "Vehicle Type"
        Column identifying Ambulift jobs (value "Ambulift").
    out_col : str, default "Month"
        Name of the output effective-month column.
    window_start : str or pandas.Timestamp, optional
        Start (inclusive) of the reporting window; prevents reassignment to pre-window months.

    Returns
    -------
    pandas.DataFrame
        Table with one row per month:
            ['Month', 'Ambulift PRMs']
    """
    # Filter only ambulift rows
    amb = prm_df[prm_df[vehicle_col] == "Ambulift"].copy()

    if amb.empty:
        return pd.DataFrame({
            out_col: pd.Series(dtype="datetime64[ns]"),
            "Ambulift PRMs": pd.Series(dtype="int64")
        })

    # Assign effective month with same logic as PRM
    amb_eff = assign_effective_month(
        amb,
        id_col=id_col,
        date_col=date_col,
        ad_col=ad_col,
        out_col="Effective Month",
        window_start=window_start,
    )

    # Group by corrected month
    grouped = (
        amb_eff.groupby("Effective Month")[id_col]
               .nunique()
               .reset_index(name="Ambulift PRMs")
               .rename(columns={"Effective Month": out_col})
    )

    return grouped
