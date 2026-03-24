
# Scripts/prm_report_v2.py
import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import argparse
import time
import pandas as pd
from pathlib import Path

# utils
from modules.utils.query import query
from modules.utils.dates import to_datetime, add_date_parts, assign_effective_month
from modules.utils.excel import write_once_then_update
from modules.utils.progress import step

# analytics
from modules.analytics.penetration import row_penetration
from modules.analytics.peaks import peak_day
from modules.analytics.grouping import group_sum, group_unique_by_effective_month, count_distinct_id_by_effective_month
from modules.analytics.growth import period_growth

# domain (logic)
from modules.domain.prm.demand import group_prm_by_time, group_pax_by_time, merge_pax, add_budget_comparison, compute_complaints_rolling_window, compute_ecac_yearly_means, prm_breakdowns
from modules.domain.prm.ambulift import group_ambulift_by_effective_month, ambulift_breakdowns
from modules.domain.prm.reception import landside_RC_breakdowns, airside_RC_breakdowns

BUDGET_EXCEL_PATH = r"C:\Users\jamie_douglas\OneDrive - Edinburgh Airport Limited\Documents\PRM Report\PRM_Data.xlsx"


# ---------------------------------------------------------------
# Load Data
# ---------------------------------------------------------------

def load_prm_data(start: str, end: str) -> pd.DataFrame:
    """ 
    Load PRM Data where Billing PRM = 1, for time period 
    
    Parameters
    ----------
    start: str
        Start of window (ISO format)
    end: str
        End of window (ISO format)
    
     Returns
    ----------
    pd.DataFrame
        Dataframe of PRM Data with columns:
        ['Job ID', 'Passenger ID', 'Operation Date', 'Vehicle Type', 'Operation Date_day', 'Operation Date_month', 'Operation Date_year']
    """

    
    start_op = start.replace("-", "")   # "2025-01-01" → "20250101"
    end_op   = end.replace("-", "")     # "2026-01-01" → "20260101"


    df = query(
        table="PRM.CompletedServicesByJob",
        columns = [
            "RequestID AS [Job ID]",
            "PassengerID AS [Passenger ID]",
            "Operation_DateID_Local AS [Operation Date]",
            "ArrDep AS [A/D]",
            "VehicleTypeName AS [Vehicle Type]",
            "adhocOrPlanned AS [Adhoc Or Planned]",
            "actualPickupLocation AS [Pickup Location]",
            "actualDestinationLocation AS [Destination Location]",
            "currentSSRCode AS [SSR Code]" 
        ],
        where = ["BillingPRM = 1",
                 "Operation_DateID_Local >= :start_op",
                 "Operation_DateID_Local < :end_op",
        ],
        params= {"start_op": start_op, "end_op": end_op},
        query_option = "OPTION (RECOMPILE)",
    )
    
    df = to_datetime(df, "Operation Date")
    df = add_date_parts(df, "Operation Date", day=True, year=True)

    return df


def load_passenger_data(start: str, end: str) -> pd.DataFrame:
    """
    Load flight historical data for time period.
    
    Parameters
    ----------
    start : str
        Start of window (ISO format 'YYYY-MM-DD', inclusive).
    end : str
        End of window (ISO format 'YYYY-MM-DD', exclusive).
    
    Returns
    -------
    pandas.DataFrame
        DataFrame of Pax totals with columns:
        ['Actual DateTime', 'Pax', 'Day', 'Year']
    """
    df = query(
        table="Eal.FlightPerformance",
        columns = [
            "ActualDateTime_Local AS [Actual DateTime]",
            "Passengers AS [Pax]",
        ],
        date_column = "ActualDateTime_Local",
        start=start, end=end,
    )
    df = to_datetime(df, "Actual DateTime")
    df = add_date_parts(df, "Actual DateTime", day=True, year=True)
    
    return df


def load_prm_budget_data(df_path: str | None = None) -> pd.DataFrame:
    """
    Load PRM budget Excel. If df_path is None, falls back to BUDGET_EXCEL_PATH.

    Parameters
    ----------
    df_path : str or None
        Path to the budget Excel file. Uses default if omitted.

    Returns
    -------
    pandas.DataFrame
        Budget DataFrame with normalized Month and standard columns:
        ['Month', 'Budget Penetration Rate', 'Budget PRM Demand', 'Budget Ambu PRM',
         'Complaints Per 1k', 'ECAC Arrivals', 'ECAC Departures', 'Year', 'Month Name']
    """
    path = df_path or BUDGET_EXCEL_PATH

    budget_df = pd.read_excel(path)
    budget_df["Month"] = pd.to_datetime(budget_df["Month"], dayfirst=True)
    budget_df = budget_df.rename(columns = {
        "Budget_Pen_Rate" : "Budget Penetration Rate",
        "Budget_PRM_Demand": "Budget PRM Demand",
        "Budget_Ambi_PRM" : "Budget Ambu PRM" ,
        "Complaints_Per_1k" : "Complaints Per 1k",
        "ECAC_Arr" : "ECAC Arrivals",
        "ECAC_Dep": "ECAC Departures"
    })

    cols = [col for col in budget_df.columns if col !="Month"]
    budget_df[cols] = budget_df[cols].apply(pd.to_numeric, errors="coerce")

    budget_df = budget_df.sort_values("Month")
    budget_df = add_date_parts(budget_df, "Month", year=True, month_name=True)

    return budget_df


# ---------------------------------------------------------------
# Build Monthly/Daily/Yearly
# ---------------------------------------------------------------

def build_monthly(prm_df: pd.DataFrame, pax_df: pd.DataFrame, budget_df: pd.DataFrame, *, window_start: str) -> pd.DataFrame:
    """
    Build the monthly PRM + Pax summary using Effective Month reassignment
    (prevents double counting at month boundaries). Ambulift monthly counts also
    use the same Effective Month logic for consistency.

    Parameters
    ----------
    prm_df : pandas.DataFrame
        PRM dataset including ['Passenger ID', 'Operation Date', 'A/D', 'Vehicle Type'].
    pax_df : pandas.DataFrame
        Pax dataset including ['Actual DateTime', 'Pax'].
    budget_df : pandas.DataFrame
        Budget dataset including ['Month', 'Budget PRM Demand', 'Budget Penetration Rate'].
    window_start : str
        Start date (inclusive) of the reporting window; guards Effective Month reassignment.

    Returns
    -------
    pandas.DataFrame
        Monthly summary with:
        ['Month', 'Unique Count', 'Total Pax', 'Penetration Rate',
         'Budget PRM Demand', 'Budget Penetration Rate',
         'Diff vs Budget PRM (%)', 'Diff vs Budget Penetration Rate (%)',
         'Ambulift PRMs', 'Percent Ambulift', 'Year', 'Month Name']
    """
    # PRM uniques by Effective Month (A/D-guarded spillover reassignment)
    prm_monthly = group_unique_by_effective_month(
        prm_df,
        id_col="Passenger ID",
        date_col="Operation Date",
        ad_col="A/D",
        out_col="Month",
        window_start=window_start,
    )

    # Pax monthly (unchanged)
    pax_monthly = group_pax_by_time(pax_df, "Actual DateTime", "M", out_col="Month")

    # Merge PRM & Pax → penetration → budget comparison
    monthly = merge_pax(prm_monthly, pax_monthly, "Month")
    monthly = row_penetration(monthly, "Unique Count", "Total Pax")
    monthly = add_budget_comparison(monthly, budget_df, "Month")

    # Ambulift monthly using the same Effective Month rule
    amb_monthly = group_ambulift_by_effective_month(
        prm_df,
        id_col="Passenger ID",
        date_col="Operation Date",
        ad_col="A/D",
        vehicle_col="Vehicle Type",
        out_col="Month",
        window_start=window_start,
    )
    monthly = monthly.merge(amb_monthly, on="Month", how="left")
    monthly["Ambulift PRMs"] = monthly["Ambulift PRMs"].fillna(0)

    # % Ambulift among PRMs
    monthly = row_penetration(monthly, "Ambulift PRMs", "Unique Count", out_col="Percent Ambulift")

    # Cosmetics
    monthly["Year"]       = monthly["Month"].dt.year
    monthly["Month Name"] = monthly["Month"].dt.strftime("%b")

    return monthly


def build_daily(prm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build daily PRM uniques (unchanged).

    Parameters
    ----------
    prm_df : pandas.DataFrame
        PRM dataset including 'Operation Date' and 'Passenger ID'.

    Returns
    -------
    pandas.DataFrame
        Daily PRM uniques: ['Day', 'Unique Count']
    """
    return group_prm_by_time(prm_df, "Operation Date", "D", out_col="Day")


def build_yearly(prm_df: pd.DataFrame, monthly: pd.DataFrame, daily: pd.DataFrame, budget_df: pd.DataFrame, *, window_start: str) -> pd.DataFrame:
    """
    Build yearly summary using the 'distinct (Passenger ID, Effective Month)' rule
    to avoid month-boundary inflation of PRM totals. Pax and Ambulift totals are
    summed from monthly.

    Parameters
    ----------
    prm_df : pandas.DataFrame
        PRM dataset including ['Passenger ID', 'Operation Date', 'A/D'].
    monthly : pandas.DataFrame
        Monthly summary with PRM/Pax/Ambulift aggregates.
    daily : pandas.DataFrame
        Daily PRM uniques used for peak-day detection.
    budget_df : pandas.DataFrame
        Budget dataset including 'Year', 'Budget PRM Demand', 'Budget Penetration Rate'.
    window_start : str
        Start date (inclusive) that guards Effective Month reassignment.

    Returns
    -------
    pandas.DataFrame
        Yearly summary with:
        ['Year', 'Unique Count', 'Ambulift PRMs', 'Total Pax',
         'Penetration Rate', 'Budget PRM Demand', 'Budget Penetration Rate',
         'Diff vs Budget PRM (%)', 'Diff vs Budget Penetration Rate (%)',
         'Peak Day', 'Peak Count', <complaints>, 'ECAC Arrivals', 'ECAC Departures']
    """
    # Annual Pax & Ambulift from monthly aggregates
    y_pax = group_sum(monthly, ["Year"], "Total Pax", "Total Pax")
    y_amb = group_sum(monthly, ["Year"], "Ambulift PRMs", "Ambulift PRMs")

    # Correct yearly PRM unique count from Effective Month on raw rows
    prm_eff = assign_effective_month(
        prm_df,
        id_col="Passenger ID",
        date_col="Operation Date",
        ad_col="A/D",
        out_col="Effective Month",
        window_start=window_start,
    )
    prm_eff["Year"] = prm_eff["Effective Month"].dt.year
    y_prm = (
        prm_eff.groupby("Year", dropna=False)["Passenger ID"]
               .nunique()
               .reset_index(name="Unique Count")
    )
    #Adjusted yearly demand = distinct(Passenger, Effective Month) pairs
    pairs=prm_eff[["Passenger ID", "Effective Month"]].dropna().drop_duplicates()
    pairs["Year"] = pairs["Effective Month"].dt.year
    y_adjusted = (
        pairs.groupby("Year")
        .size()
        .reset_index(name="Adjusted Yearly PRM Demand")
    )

    # Merge and compute penetration
    yearly = (
        y_prm
            .merge(y_adjusted, on="Year", how="left")
            .merge(y_amb, on="Year", how="left")
            .merge(y_pax, on="Year", how="left")
    )
    yearly = row_penetration(yearly, numerator_col="Adjusted Yearly PRM Demand", denominator_col="Total Pax", out_col="Penetration Rate")

    # Year-level budget comparison
    budget_year = budget_df.groupby("Year", as_index=False).agg({
        "Budget PRM Demand": "sum",
        "Budget Penetration Rate": "mean"
    })
    yearly = add_budget_comparison(yearly, budget_year, bucket_col="Year")

    # Peak PRM day
    peak_date, peak_count = peak_day(daily.set_index("Day")["Unique Count"])
    yearly["Peak Day"] = peak_date
    yearly["Peak Count"] = peak_count

    # Complaints & ECAC
    complaints = compute_complaints_rolling_window(budget_df, window=3, value_col="Complaints Per 1k", wide=True)
    yearly = yearly.merge(complaints, how="cross")
    yearly = yearly.merge(
        compute_ecac_yearly_means(budget_df),
        on="Year",
        how="left"
    )

    return yearly


# ---------------------------------------------------------------
# Build Breakdown tables (SSR + Adhoc/Planned)
# ---------------------------------------------------------------

def build_breakdowns(prm_df: pd.DataFrame, start: str) -> dict[str, pd.DataFrame]:
    """
    Build PRM, Ambulift, Landside RC, and Airside RC breakdowns and return the merged summary tables used for Excel output.
    
    Parameters
    ----------
    prm_df : pandas.DataFrame
        Input PRM dataset.
        
    Returns
    -------
    dict[str, pandas.DataFrame]
        {
            "by_ssr": DataFrame with columns:
                [SSR Code,
                 Unique Count, Total PRM, % of PRM Demand,
                 Ambulift Users, Total Ambulift Users, % of Ambulift Users,
                 Landside RC Users, Total Landside RC Users, % of Landside RC Users,
                 Airside RC Users, Total Airside RC Users, % of Airside RC Users],
            "by_booking": DataFrame with analogous columns keyed by 'Adhoc Or Planned'
        } 
    """
    # Compute 4 domain-level breakdowns
    prm_bk  = prm_breakdowns(prm_df, window_start=start)
    ambu_bk = ambulift_breakdowns(prm_df)
    land_bk = landside_RC_breakdowns(prm_df)
    air_bk  = airside_RC_breakdowns(prm_df)

    # ---------- By SSR Code ----------
    by_ssr = (
        prm_bk["by_ssr"][["SSR Code", "Unique Count", "Total PRM", "% of PRM Demand"]]
        .merge(ambu_bk["by_ssr"][["SSR Code", "Ambulift Users", "Total Ambulift Users", "% of Ambulift Users"]],
               on="SSR Code", how="outer")
        .merge(land_bk["by_ssr"][["SSR Code", "Landside RC Users", "Total Landside RC Users", "% of Landside RC Users"]],
               on="SSR Code", how="outer")
        .merge(air_bk["by_ssr"][["SSR Code", "Airside RC Users", "Total Airside RC Users", "% of Airside RC Users"]],
               on="SSR Code", how="outer")
    ).fillna(0.0).sort_values("SSR Code").reset_index(drop=True)

    # ---------- By Booking (Ad-Hoc / Planned) ----------
    by_booking = (
        prm_bk["by_booking"][["Adhoc Or Planned", "Unique Count", "Total PRM", "% of PRM Demand"]]
        .merge(ambu_bk["by_booking"][["Adhoc Or Planned", "Ambulift Users", "Total Ambulift Users", "% of Ambulift Users"]],
               on="Adhoc Or Planned", how="outer")
        .merge(land_bk["by_booking"][["Adhoc Or Planned", "Landside RC Users", "Total Landside RC Users", "% of Landside RC Users"]],
               on="Adhoc Or Planned", how="outer")
        .merge(air_bk["by_booking"][["Adhoc Or Planned", "Airside RC Users", "Total Airside RC Users", "% of Airside RC Users"]],
               on="Adhoc Or Planned", how="outer")
    ).fillna(0.0).sort_values("Adhoc Or Planned").reset_index(drop=True)

    return {"by_ssr": by_ssr, "by_booking": by_booking}


# ---------------------------------------------------------------
# DEBUGGING (before and after effective month)
# ---------------------------------------------------------------

def _clean_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Remove any pandas Period or helper columns that Excel cannot store."""
    bad_cols = [
        "EffMonth",
        "PrevEffMonth", "PrevEffMonthP",
        "CurrEffMonthP",
        "RawMonth",
        "PrevRawMonth", "CurrRawMonth",
        "OpDateOnly", "PrevOpDateOnly",
        "DayDiff",
        "RawMonthChanged", "EffMonthChanged",
    ]
    return df.drop(columns=[c for c in bad_cols if c in df.columns], errors="ignore")



def debug_prm_spanning_months_effective(
    prm_df: pd.DataFrame,
    start: str,
    excel_out: str | None = None,
    *,
    id_col: str = "Passenger ID",
    date_col: str = "Operation Date",
    ad_col: str = "A/D",
) -> dict[str, pd.DataFrame]:
    """
    Debug PRMs that:
      • appear in >1 month
      • cross midnight (consecutive days)
      • cross midnight *and* cross months

    Computes the three metrics both:
      - BEFORE Effective Month (raw Operation Date month)
      - AFTER  Effective Month reassignment

    Writes a single "PRM Debug Summary (Before vs After EM)" sheet showing:
      Metric | Before | After | Reduction

    Also writes detail tabs for the AFTER view:
      - "EM: PRMs in >1 Month"
      - "EM: Cross-Midnight PRMs"
      - "EM: Cross-Midnight Across Months"

    Returns a dict with dataframes used.
    """
    import pandas as pd

    # ---- Guard ----
    if prm_df is None or prm_df.empty:
        summary = pd.DataFrame({
            "Metric": [
                "Distinct PRMs in >1 month",
                "Distinct PRMs crossing midnight",
                "Distinct PRMs crossing midnight AND crossing months",
            ],
            "Before (raw)": [0, 0, 0],
            "After (Effective Month)": [0, 0, 0],
            "Reduction": [0, 0, 0],
        })
        if excel_out:
            write_once_then_update(excel_out, "PRM Debug Summary (Before vs After EM)",
                                   summary, anchor="A2", include_header=True)
        return {
            "before_multi_month": pd.DataFrame(),
            "before_cross_midnight": pd.DataFrame(),
            "before_cross_midnight_cross_month": pd.DataFrame(),
            "after_multi_month": pd.DataFrame(),
            "after_cross_midnight": pd.DataFrame(),
            "after_cross_midnight_cross_month": pd.DataFrame(),
            "summary": summary,
        }

    # ========= BEFORE (raw Operation Date month) =========
    raw = prm_df.copy()
    raw[date_col] = pd.to_datetime(raw[date_col], errors="coerce")
    raw["RawMonth"] = raw[date_col].dt.to_period("M")

    # (1) In >1 raw month
    raw_month_counts = raw.groupby(id_col)["RawMonth"].nunique()
    raw_multi_ids = raw_month_counts[raw_month_counts > 1].index
    before_multi_month = (
        raw[raw[id_col].isin(raw_multi_ids)]
           .sort_values([id_col, date_col])
           .drop(columns=["RawMonth"])
    )

    # For cross-midnight, compute day diffs
    rsort = raw.sort_values([id_col, date_col]).copy()
    rsort["OpDateOnly"] = rsort[date_col].dt.normalize()
    rsort["PrevOpDateOnly"] = rsort.groupby(id_col)["OpDateOnly"].shift(1)
    rsort["DayDiff"] = (rsort["OpDateOnly"] - rsort["PrevOpDateOnly"]).dt.days

    # (2) Cross-midnight (any consecutive-day occurrence)
    before_cross_midnight_ids = rsort.groupby(id_col)["DayDiff"].apply(lambda s: (s == 1).any())
    before_cross_midnight_ids = before_cross_midnight_ids[before_cross_midnight_ids].index
    before_cross_midnight = rsort[rsort[id_col].isin(before_cross_midnight_ids)].copy()
    before_cross_midnight = before_cross_midnight.drop(columns=["RawMonth"], errors="ignore")

    # (3) Cross-midnight AND cross raw month
    rsort["PrevRawMonth"] = rsort["PrevOpDateOnly"].dt.to_period("M")
    rsort["CurrRawMonth"] = rsort["OpDateOnly"].dt.to_period("M")
    rsort["RawMonthChanged"] = rsort["PrevRawMonth"] != rsort["CurrRawMonth"]
    raw_cross_month_mask = (rsort["DayDiff"] == 1) & (rsort["RawMonthChanged"])
    before_cross_midnight_cross_month_ids = rsort.loc[raw_cross_month_mask, id_col].unique()
    before_cross_midnight_cross_month = rsort[rsort[id_col].isin(before_cross_midnight_cross_month_ids)].copy()
    before_cross_midnight_cross_month = before_cross_midnight_cross_month.drop(columns=["RawMonth"], errors="ignore")

    before_counts = {
        "multi": int(len(raw_multi_ids)),
        "xmid": int(len(before_cross_midnight_ids)),
        "xmid_xmon": int(len(before_cross_midnight_cross_month_ids)),
    }

    # ========= AFTER (Effective Month) =========
    eff = assign_effective_month(
        prm_df.copy(),
        id_col=id_col,
        date_col=date_col,
        ad_col=ad_col,
        out_col="Effective Month",
        window_start=start,   # ensures we don't reassign into pre-window months
    )
    eff[date_col] = pd.to_datetime(eff[date_col], errors="coerce")
    eff["EffMonth"] = eff["Effective Month"].dt.to_period("M")

    # (1) In >1 Effective Month
    eff_month_counts = eff.groupby(id_col)["EffMonth"].nunique()
    eff_multi_ids = eff_month_counts[eff_month_counts > 1].index
    after_multi_month = (
        eff[eff[id_col].isin(eff_multi_ids)]
           .sort_values([id_col, date_col])
           .drop(columns=["EffMonth"])
    )

    # (2) Cross-midnight (any consecutive-day occurrence) – same day logic,
    #     but now reported on the EM-adjusted frame so you can inspect rows post-reassignment.
    esort = eff.sort_values([id_col, date_col]).copy()
    esort["OpDateOnly"] = esort[date_col].dt.normalize()
    esort["PrevOpDateOnly"] = esort.groupby(id_col)["OpDateOnly"].shift(1)
    esort["DayDiff"] = (esort["OpDateOnly"] - esort["PrevOpDateOnly"]).dt.days

    after_cross_midnight_ids = esort.groupby(id_col)["DayDiff"].apply(lambda s: (s == 1).any())
    after_cross_midnight_ids = after_cross_midnight_ids[after_cross_midnight_ids].index
    after_cross_midnight = esort[esort[id_col].isin(after_cross_midnight_ids)].copy()

    # (3) Cross-midnight AND cross Effective Month
    esort["PrevEffMonth"] = esort.groupby(id_col)["Effective Month"].shift(1)
    esort["PrevEffMonthP"] = esort["PrevEffMonth"].dt.to_period("M")
    esort["CurrEffMonthP"] = esort["Effective Month"].dt.to_period("M")
    esort["EffMonthChanged"] = esort["PrevEffMonthP"] != esort["CurrEffMonthP"]
    eff_cross_month_mask = (esort["DayDiff"] == 1) & (esort["EffMonthChanged"])
    after_cross_midnight_cross_month_ids = esort.loc[eff_cross_month_mask, id_col].unique()
    after_cross_midnight_cross_month = esort[esort[id_col].isin(after_cross_midnight_cross_month_ids)].copy()

    after_counts = {
        "multi": int(len(eff_multi_ids)),
        "xmid": int(len(after_cross_midnight_ids)),
        "xmid_xmon": int(len(after_cross_midnight_cross_month_ids)),
    }

    # ========= Summary: Before vs After =========
    summary = pd.DataFrame({
        "Metric": [
            "Distinct PRMs in >1 month",
            "Distinct PRMs crossing midnight",
            "Distinct PRMs crossing midnight AND crossing months",
        ],
        "Before (raw)": [
            before_counts["multi"],
            before_counts["xmid"],
            before_counts["xmid_xmon"],
        ],
        "After (Effective Month)": [
            after_counts["multi"],
            after_counts["xmid"],
            after_counts["xmid_xmon"],
        ],
    })
    summary["Reduction"] = summary["Before (raw)"] - summary["After (Effective Month)"]

    # ========= Write to Excel (optional) =========
    if excel_out:
        # Summary (side-by-side)
        write_once_then_update(
            excel_out, "Debug Raw vs EM",
            summary, anchor="A2", include_header=True
        )

        # AFTER details (so you can inspect the leftover cases)
        write_once_then_update(
            excel_out, "EM >1 Month",
            _clean_for_excel(after_multi_month), anchor="A2", include_header=True
        )
        write_once_then_update(
            excel_out, "EM Cross-Midnight PRMs",
            _clean_for_excel(after_cross_midnight), anchor="A2", include_header=True
        )
        write_once_then_update(
            excel_out, "EM Cross-Midnight Months",
            _clean_for_excel(after_cross_midnight_cross_month), anchor="A2", include_header=True
        )

    return {
        # before
        "before_multi_month": before_multi_month,
        "before_cross_midnight": before_cross_midnight,
        "before_cross_midnight_cross_month": before_cross_midnight_cross_month,
        # after
        "after_multi_month": after_multi_month,
        "after_cross_midnight": after_cross_midnight,
        "after_cross_midnight_cross_month": after_cross_midnight_cross_month,
        # summary
        "summary": summary,
    }



# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------

def main(start: str, end: str, budget_path: str, excel_out: str | None):
    """
    Run the PRM report (v2) for a given window and optionally write to Excel.

    Parameters
    ----------
    start : str
        Start date (inclusive) in ISO format 'YYYY-MM-DD'.
    end : str
        End date (exclusive) in ISO format 'YYYY-MM-DD'.
    budget_path : str
        Optional path to the PRM budget Excel (uses default if omitted).
    excel_out : str or None
        Output Excel path. When None, skips writing.

    Returns
    -------
    None
        Prints progress and optionally writes Excel sheets.
    """
    print("\nPRM REPORT (v2)")
    print(f"Window : {start} → {end}\n")

    t0 = time.perf_counter()

    # 1) PRM
    print("[1/8] Loading PRM jobs…")
    prm_df = load_prm_data(start, end)
    t1 = step(t0, f"Loaded PRM rows: {len(prm_df):,}")
    if prm_df.empty:
        print("No PRM jobs found. Exiting.")
        return

    # 2) Pax
    print("[2/8] Loading Pax…")
    pax_df = load_passenger_data(start, end)
    t2 = step(t1, f"Loaded Pax rows: {len(pax_df):,}")

    # 3) Budget
    print("[3/8] Loading Budget Excel…")
    budget_df = load_prm_budget_data(budget_path)
    t3 = step(t2, "Budget loaded.")

    # 4) Monthly + Daily
    print("[4/8] Building Monthly & Daily…")
    monthly = build_monthly(prm_df, pax_df, budget_df, window_start=start)
    daily   = build_daily(prm_df)
    t4 = step(t3, "Monthly & Daily computed.")

    # 5) Yearly
    print("[5/8] Building Yearly summary…")
    yearly = build_yearly(prm_df, monthly, daily, budget_df, window_start=start)
    t5 = step(t4, "Yearly summary built.")

    # 6) Growth (use Effective Month counting rule)
    print("[6/8] Calculating PRM Growth…")
    prm_demand_growth = period_growth(
        loader_fn=load_prm_data,
        start=start,
        end=end,
        years_back=3,
        id_col="Passenger ID",
        count_strategy=lambda df: count_distinct_id_by_effective_month(
            df,
            id_col="Passenger ID",
            date_col="Operation Date",
            ad_col="A/D",
            window_start=start,
        ),
    )
    t6 = step(t5, "PRM Growth Calculated.")

    # 7) Breakdowns
    print("[7/8] Calculating SSR Code and Booking Type Breakdown…")
    breakdowns = build_breakdowns(prm_df, start)
    t7 = step(t6, "SSR Code and Booking Type Breakdown Calculated.")

    # 8) Excel output
    if excel_out:
        print("[8/8] Writing Excel output…")
        write_once_then_update(excel_out, "Monthly_PRMs",   monthly, anchor="A2", include_header=True)
        write_once_then_update(excel_out, "Yearly_Summary", yearly,  anchor="A2", include_header=True)
        write_once_then_update(excel_out, "3 Year PRM Demand Growth", prm_demand_growth)
        write_once_then_update(excel_out, "PRM Breakdown by SSR",     breakdowns["by_ssr"],     anchor="A2", include_header=True)
        write_once_then_update(excel_out, "PRM Breakdown by Booking", breakdowns["by_booking"], anchor="A2", include_header=True)

        # --- TEMP debug tabs ---
        print("     • Adding debug tabs: multi-month / cross-midnight …")
        debug_prm_spanning_months_effective(prm_df, start=start, excel_out= excel_out)

        step(t7, f"Excel updated → {excel_out}")

    print("\n✔ PRM report (v2) complete.\n")


# ================================================================
# ENTRYPOINT
# ================================================================
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="PRM Report (v2, Effective Month aware)")
    p.add_argument("--start",  required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end",    required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--budget", required=False, help="Path to PRM Budget Excel (optional: uses default if omitted")
    p.add_argument("--out",    default=None, help="Output Excel path")
    args = p.parse_args()

    main(args.start, args.end, args.budget, args.out)
