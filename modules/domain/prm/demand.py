import pandas as pd


from modules.analytics.growth import period_growth
from modules.analytics.grouping import group_unique, group_sum
from modules.analytics.timeseries import bucket_time
from modules.analytics.penetration import row_penetration
from modules.utils.dates import assign_effective_month
from modules.config import JETBRIDGE_STANDS


#---------------------------------------------------------------
# PRM time-based grouping
#---------------------------------------------------------------

def group_prm_by_time(prm_df: pd.DataFrame, time_col: str, freq: str, out_col: str = "TimeBucket") -> pd.DataFrame:
    """
    Generic time-based grouping for PRMs using bucket_time (e.g. Day: 'D', Month: 'M', Year: 'Y') + group_unique
    
    Parameters
    -----------
    prm_df: pandas.DataFrame
        Clean PRM dataset including "Passenger ID" and the chosen time column
    time_col: str
        Name of the datetime column to bucket
    freq: str
        Pandas freqeuency alias (e.g., 'D', 'M', 'H', 'W', etc.)
    out_col: str, default = "TimeBucket"
        Name of the bucketed time column

    Returns
    -----------
    pandas.DataFrame:
        columns: out_col (bucketed time), Unique Count (unique passenger ID per bucket)
    """
    #Add bucketed itme column
    bucketed = bucket_time(prm_df, time_col=time_col, freq=freq, out_col=out_col)
    return group_unique(bucketed, by_cols=[out_col], id_col="Passenger ID")

#---------------------------------------------------------------
# Pax time-based grouping
#---------------------------------------------------------------

def group_pax_by_time(pax_df: pd.DataFrame, time_col: str, freq: str, out_col: str = "TimeBucket") -> pd.DataFrame:
    """
    Generic time-based grouping for passenger totals using bucket_time (e.g. Day: 'D', Month: 'M', Year: 'Y') + group_sum
    
    Parameters
    -----------
    pax_df: pandas.DataFrame
        Clean pax dataset including time_col and "Pax
    time_col: str
        Name of the datetime column to bucket
    freq: str
        Pandas freqeuency alias (e.g., 'D', 'M', 'H', 'W', etc.)
    out_col: str, default = "TimeBucket"
        Name of the bucketed time column

    Returns
    -----------
    pandas.DataFrame:
        columns: out_col (bucketed time), Unique Count (unique passenger ID per bucket)
    """
    #Add bucketed itme column
    bucketed = bucket_time(pax_df, time_col=time_col, freq=freq, out_col=out_col)
    return group_sum(bucketed, by_cols=[out_col], value_col="Pax", out_col="Total Pax")

#---------------------------------------------------------------
# Merge PRM and Pax (time-based)
#---------------------------------------------------------------

def merge_pax(prm_group_df: pd.DataFrame, pax_group_df: pd.DataFrame, bucket_col: str) -> pd.DataFrame:
    """Merge PRM grouped results with Pax grouped results on a shared bucket column
    
    Parameters
    -----------
    prm_group_df: pandas.DataFrame
        must include: bucket_col, Unique Count
    pax_group_df: pandas.DataFrame
        must include: bucket_col, Total Pax
    bucket_col: str
        The common column to merge on
         
    Returns
    ----------
     pandas.DataFrame
        PRM and Pax merged on bucket_col
    """

    return prm_group_df.merge(pax_group_df, on=bucket_col, how="left")

#---------------------------------------------------------------
# break down by SSR Code, Adhoc or Planned
#---------------------------------------------------------------

def prm_breakdowns (prm_df: pd.DataFrame, ssr_col: str = "SSR Code", booking_col: str = "Adhoc Or Planned", id_col: str = "Passenger ID", window_start: str | pd.Timestamp | None=None) -> dict[str, pd.DataFrame]:
    """
    Compute PRM demand breakdowns by SSR Code and by Adhoc/Planned
    
    Parameters
    ----------
    prm_df: pandas.DataFrame
        input dataset, must_include: ssr_col, plan_col, id_col
    ssr_col: str, default "SSR Code"
        Column containing SSR category for each PRM record
    booking_col: str, default "Adhoc Or Planned"
        Column indicating whether the request was Ad-Hoc or Planned
    id_col: str, default "Passenger ID"
        Unique passenger identifier to deduplicate on. 

    Returns
    ---------
    dict[str, pandas.DataFrame]
        {
            "by_ssr": DataFrame with columns:
                [SSR Code, Unique Count, Total PRM, % of PRM Demand],
            "by_booking": DataFrame with columns:
                [Adhoc Or Planned, Unique Count, Total PRM, % of PRM Demand]
        }
    
    Notes
    ----------
    - Percentages are computed relative to the total unique PRM passengers within prm_df
    - 'demand is defined as distinct (Passenger ID, Effective Month)
    
    """
    #Assign Effective Month
    eff = assign_effective_month(
        prm_df.copy(),
        id_col=id_col,
        date_col="Operation Date",
        ad_col="A/D",
        out_col="Effective Month",
        window_start=window_start,
    )

    # #Adjusted Total PRM = distinct (Passenger,EM Month) across the window
    # pairs=eff[[id_col, "Effective Month"]].dropna().drop_duplicates()
    # adjusted_total = int(len(pairs))

    #Helper: count pairs per group (SSR/Booking)
    def _count_pairs(df: pd.DataFrame, group_col: str, out_count_col: str) -> pd.DataFrame:
        gpairs = (
            df[[group_col, id_col, "Effective Month"]]
            .dropna()
            .drop_duplicates()
            .groupby(group_col)
            .size()
            .reset_index(name=out_count_col)
        )
        return gpairs
    
    total_prm_unique = prm_df[id_col].nunique()

    # --- By SSR Code ---
    by_ssr = group_unique(prm_df, [ssr_col], id_col=id_col).copy()
    by_ssr["_denom_total"] = float(total_prm_unique)
    by_ssr = row_penetration(by_ssr, "Unique Count", "_denom_total", "% of PRM Demand")
    by_ssr["% of PRM Demand"] *= 100.0
    by_ssr["Total PRM"] = int(total_prm_unique)
    by_ssr = by_ssr.drop(columns = ["_denom_total"]).sort_values(ssr_col).reset_index(drop=True)

    # #By SSR Code (Adjusted)
    # by_ssr = _count_pairs(eff, ssr_col, "Unique Count")
    # by_ssr["_denom_total"] = float(adjusted_total)
    # by_ssr = row_penetration(by_ssr, "Unique Count", "_denom_total", "% of PRM Demand")
    # by_ssr["% of PRM Demand"] *= 100.0
    # by_ssr["Total PRM"] = adjusted_total
    # by_ssr = by_ssr.drop(columns=["_denom_total"]).sort_values(ssr_col).reset_index(drop=True)

    # --- By Adhoc/Planned ---
    by_booking = group_unique(prm_df, [booking_col], id_col = id_col).copy()
    by_booking["_denom_total"] = float(total_prm_unique)
    by_booking = row_penetration(by_booking, "Unique Count", "_denom_total", "% of PRM Demand")
    by_booking["% of PRM Demand"] *= 100.0
    by_booking["Total PRM"] = int(total_prm_unique)
    by_booking = by_booking.drop(columns=["_denom_total"]).sort_values(booking_col).reset_index(drop=True)

    # #By Adhoc/Planned (Adjusted)
    # by_booking = _count_pairs(eff, booking_col, "Unique Count")
    # by_booking["_denom_total"] = float(adjusted_total)
    # by_booking = row_penetration(by_booking, "Unique Count", "_denom_total", "% of PRM Demand")
    # by_booking["% of PRM Demand"] *= 100.0
    # by_booking["Total PRM"] = adjusted_total
    # by_booking = by_booking.drop(columns=["_denom_total"]).sort_values(booking_col).reset_index(drop=True)

    return {"by_ssr": by_ssr, "by_booking": by_booking}


#---------------------------------------------------------------
# Compare with Budget (demand and penetration)
#---------------------------------------------------------------

def add_budget_comparison(grouped_df: pd.DataFrame, budget_df: pd.DataFrame, bucket_col:str) -> pd.DataFrame:
    """
    Merge budget PRM demand and budget Penetration rate, then compute differences vs budget for PRM demand and Penetration rate (Assumed pre-computed in script)
    
    Parameters
    ----------
    grouped_df: pandas.DataFrame
        Must include bucket_col, 'Unique Count', 'Penetration Rate'
    budget_df: pandas.DataFrame
        Must include: bucket_col, Budget PRM Demand, Budget Penetration Rate
    bucket_col : str
        The shared grouping key (e.g. "Month", "Day", "TimeBucket")
    
    Returns
    ---------
    pandas.DataFrame
        grouped_df with Budget PRM Demand, Budget Penetration Rate, Diff vs Budget PRM Demand (%), Diff vs Budget Penetration Rate (%)
    """

    df = grouped_df.merge( budget_df[[bucket_col, "Budget PRM Demand", "Budget Penetration Rate"]], on=bucket_col, how="left")

    df["Diff vs Budget PRM (%)"] = (df["Unique Count"] - df["Budget PRM Demand"]) / df["Budget PRM Demand"] * 100
    df["Diff vs Budget Penetration Rate (%)"] = (df["Penetration Rate"] - df["Budget Penetration Rate"]) / df["Budget Penetration Rate"] * 100

    return df

def compute_complaints_rolling_window(
    budget_df: pd.DataFrame,
    window: int = 3,
    value_col: str = "Complaints Per 1k",
    wide: bool = True
) -> pd.DataFrame:
    """
    Computes a rolling-window average of complaints for the most recent N years.

    Parameters
    ----------
    budget_df : pandas.DataFrame
        Must include:
            • 'Year'
            • value_col  (default: 'Complaints Per 1k')
    window : int, default 3
        Number of trailing years to compute means for.
    value_col : str
        Column to average.
    wide : bool, default = True

    Returns
    -------
    pandas.DataFrame

        If Wide=True (default):
        returns a single-row DataFrame with three columns:
            - Avg Complaints <Y-2>
            - Avg Complaints <Y-1>
            - Avg Complaints <Y>
            where Y is the latest year present in budget_df
        
        If wide=False:
                returns ['Year', 'Avg Complaints]
    """
    
    years = pd.to_numeric(budget_df["Year"], errors="coerce").dropna().astype(int)
    if years.empty:
        if wide:
            return pd.DataFrame([{
                "Avg Complaints <Y-2>": None,
                "Avg Complaints <Y-1>": None,
                "Avg Complaints <Y>"  : None,
            }])
        else:
            return pd.DataFrame(columns=["Year", "Avg Complaints"])

    latest_year = int(years.max())
    last_n_years = [latest_year - (window - 1) + i for i in range(window)]

    tidy = (
        budget_df[budget_df["Year"].isin(last_n_years)]
        .groupby("Year")[value_col]
        .mean()
        .reset_index()
        .rename(columns={value_col: "Avg Complaints"})
        .sort_values("Year")
        .reset_index(drop=True)
    )

    if not wide:
        return tidy

    # legacy wide: 1 row with 3 columns labelled with literal years
    out = {}
    for y in last_n_years:
        ser = tidy.loc[tidy["Year"] == y, "Avg Complaints"]
        out[f"Avg Complaints {y}"] = float(ser.iloc[0]) if not ser.empty else float("nan")

    return pd.DataFrame([out])



def compute_ecac_yearly_means(
    budget_df: pd.DataFrame,
    arr_col: str = "ECAC Arrivals",
    dep_col: str = "ECAC Departures"
) -> pd.DataFrame:
    """
    Computes yearly averages for ECAC Arrivals and Departures.

    Parameters
    ----------
    budget_df : pandas.DataFrame
        Must include:
            • 'Year'
            • arr_col  (default: 'ECAC Arrivals')
            • dep_col  (default: 'ECAC Departures')

    Returns
    -------
    pandas.DataFrame
        Columns:
            • Year
            • ECAC Arrivals
            • ECAC Departures
    """
    return (
        budget_df.groupby("Year")[[arr_col, dep_col]]
                 .mean()
                 .reset_index()
    )
