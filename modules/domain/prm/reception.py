
import pandas as pd
from modules.analytics.grouping import group_unique
from modules.analytics.penetration import row_penetration
from modules.config import PRM_LANDSIDE_VALUES, PRM_AIRSIDE_VALUES


#---------------------------------------------------------------
# Landside Reception Centre breakdowns
#---------------------------------------------------------------

def landside_RC_breakdowns(
    prm_df: pd.DataFrame,
    ssr_col: str = "SSR Code",
    booking_col: str = "Adhoc Or Planned",
    id_col: str = "Passenger ID",
    pickup_col: str = "Pickup Location",
    dest_col: str = "Destination Location"
) -> dict[str, pd.DataFrame]:
    """
    Compute Landside Reception Centre breakdowns by SSR Code and by Adhoc/Planned.

    Parameters
    ----------
    prm_df : pandas.DataFrame
        Full PRM dataset returned from load_prm_data().
    ssr_col : str, default "SSR Code"
        Column representing SSR category.
    booking_col : str, default "Adhoc Or Planned"
        Indicates whether the request was Ad-Hoc or Planned.
    id_col : str, default "Passenger ID"
        Unique passenger identifier.
    pickup_col : str
        Pickup location for the PRM service.
    dest_col : str
        Destination location for the PRM service.

    Returns
    -------
    dict[str, pandas.DataFrame]
        {
            "by_ssr":     DataFrame with columns:
                [SSR Code, Landside RC Users, % of Landside RC Users],
            "by_booking": DataFrame with columns:
                [Adhoc Or Planned, Landside RC Users, % of Landside RC Users]
        }

    Notes
    -----
    • Universe contains rows where pickup or destination exactly matches the
      configured PRM_LANDSIDE_VALUES list.
    """

    # Exact match on either pickup or destination
    mask = (
        prm_df[pickup_col].isin(PRM_LANDSIDE_VALUES)
        | prm_df[dest_col].isin(PRM_LANDSIDE_VALUES)
    )

    ls = prm_df[mask].copy()

    if ls.empty:
        return {
            "by_ssr": pd.DataFrame(columns=[ssr_col, "Landside RC Users", "% of Landside RC Users"]),
            "by_booking": pd.DataFrame(columns=[booking_col, "Landside RC Users", "% of Landside RC Users"]),
        }

    denom = ls[id_col].nunique()

    # ---- By SSR Code ----
    by_ssr = group_unique(ls, [ssr_col], id_col=id_col).rename(
        columns={"Unique Count": "Landside RC Users"}
    ).copy()

    by_ssr["_denom_total"] = float(denom)
    by_ssr = row_penetration(by_ssr, "Landside RC Users", "_denom_total", "% of Landside RC Users")
    by_ssr["% of Landside RC Users"] *= 100.0
    by_ssr["Total Landside RC Users"] = int(denom)
    by_ssr = by_ssr.drop(columns=["_denom_total"]).sort_values(ssr_col).reset_index(drop=True)

    # ---- By Adhoc/Planned ----
    by_booking = group_unique(ls, [booking_col], id_col=id_col).rename(
        columns={"Unique Count": "Landside RC Users"}
    ).copy()

    by_booking["_denom_total"] = float(denom)
    by_booking = row_penetration(by_booking, "Landside RC Users", "_denom_total", "% of Landside RC Users")
    by_booking["% of Landside RC Users"] *= 100.0
    by_booking["Total Landside RC Users"] = int(denom)
    by_booking = by_booking.drop(columns=["_denom_total"]).sort_values(booking_col).reset_index(drop=True)

    return {"by_ssr": by_ssr, "by_booking": by_booking}


#---------------------------------------------------------------
# Airside Reception Centre breakdowns
#---------------------------------------------------------------

def airside_RC_breakdowns(
    prm_df: pd.DataFrame,
    ssr_col: str = "SSR Code",
    booking_col: str = "Adhoc Or Planned",
    id_col: str = "Passenger ID",
    pickup_col: str = "Pickup Location",
    dest_col: str = "Destination Location"
) -> dict[str, pd.DataFrame]:
    """
    Compute Airside Reception Centre breakdowns by SSR Code and by Adhoc/Planned.

    Parameters
    ----------
    prm_df : pandas.DataFrame
        Full PRM dataset returned from load_prm_data().
    ssr_col : str, default "SSR Code"
        Column representing SSR classification.
    booking_col : str, default "Adhoc Or Planned"
        Indicates whether the request was Ad-Hoc or Planned.
    id_col : str, default "Passenger ID"
        Unique passenger identifier.
    pickup_col : str
        Pickup location for the PRM service.
    dest_col : str
        Destination location for the PRM service.

    Returns
    -------
    dict[str, pandas.DataFrame]
        {
            "by_ssr":     DataFrame with columns:
                [SSR Code, Airside RC Users, % of Airside RC Users],
            "by_booking": DataFrame with columns:
                [Adhoc Or Planned, Airside RC Users, % of Airside RC Users]
        }

    Notes
    -----
    • Universe contains rows where pickup or destination exactly matches the
      configured PRM_AIRSIDE_VALUES list.
    """

    # Any exact-match on either pickup or destination
    mask = (
        prm_df[pickup_col].isin(PRM_AIRSIDE_VALUES)
        | prm_df[dest_col].isin(PRM_AIRSIDE_VALUES)
    )

    ar = prm_df[mask].copy()

    if ar.empty:
        return {
            "by_ssr": pd.DataFrame(columns=[ssr_col, "Airside RC Users", "% of Airside RC Users"]),
            "by_booking": pd.DataFrame(columns=[booking_col, "Airside RC Users", "% of Airside RC Users"]),
        }

    denom = ar[id_col].nunique()

    # ---- By SSR Code ----
    by_ssr = group_unique(ar, [ssr_col], id_col=id_col).rename(
        columns={"Unique Count": "Airside RC Users"}
    ).copy()

    by_ssr["_denom_total"] = float(denom)
    by_ssr = row_penetration(by_ssr, "Airside RC Users", "_denom_total", "% of Airside RC Users")
    by_ssr["% of Airside RC Users"] *= 100.0
    by_ssr["Total Airside RC Users"] = int(denom)
    by_ssr = by_ssr.drop(columns=["_denom_total"]).sort_values(ssr_col).reset_index(drop=True)

    # ---- By Adhoc/Planned ----
    by_booking = group_unique(ar, [booking_col], id_col=id_col).rename(
        columns={"Unique Count": "Airside RC Users"}
    ).copy()

    by_booking["_denom_total"] = float(denom)
    by_booking = row_penetration(by_booking, "Airside RC Users", "_denom_total", "% of Airside RC Users")
    by_booking["% of Airside RC Users"] *= 100.0
    by_booking["Total Airside RC Users"] = int(denom)
    by_booking = by_booking.drop(columns=["_denom_total"]).sort_values(booking_col).reset_index(drop=True)

    return {"by_ssr": by_ssr, "by_booking": by_booking}
