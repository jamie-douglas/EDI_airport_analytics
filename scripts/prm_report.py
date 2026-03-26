#Scripts/prm_report.py
import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import argparse
import time
import pandas as pd
from pathlib import Path

#utils
from modules.utils.query import query
from modules.utils.dates import to_datetime, add_date_parts
from modules.utils.excel import write_once_then_update
from modules.utils.progress import step

#analytics
from modules.analytics.penetration import row_penetration
from modules.analytics.peaks import peak_day
from modules.analytics.grouping import group_sum
from modules.analytics.growth import period_growth

#domain (logic))
from modules.domain.prm.demand import group_prm_by_time, group_pax_by_time, merge_pax, add_budget_comparison, compute_complaints_rolling_window, compute_ecac_yearly_means, prm_breakdowns
from modules.domain.prm.ambulift import group_ambulift_by_time, ambulift_breakdowns
from modules.domain.prm.reception import landside_RC_breakdowns, airside_RC_breakdowns

BUDGET_EXCEL_PATH = r"C:\Users\jamie_douglas\OneDrive - Edinburgh Airport Limited\Documents\PRM Report\PRM_Data.xlsx"

#---------------------------------------------------------------
# Load Data
#---------------------------------------------------------------


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

def load_passenger_data(start:str, end:str) -> pd.DataFrame:
    """
    Load flight historical data for time period
    
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
        ['Actual DateTime', 'Pax', 'Actual DateTime_month', 'Actual DateTime_year', ''Actual DateTime_day']
    
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

def load_prm_budget_data (df_path:str | None = None):
    """ Load PRM budget Excel. If df_path is None, falls back to BUDGET_EXCEL_PATH"""

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

#---------------------------------------------------------------
# Build Monthly/Daily/Yearly
#---------------------------------------------------------------

def build_monthly(prm_df, pax_df, budget_df):
    prm_monthly = group_prm_by_time(prm_df, "Operation Date", "M", out_col="Month")
    pax_monthly = group_pax_by_time(pax_df, "Actual DateTime", "M", out_col="Month")

    monthly = merge_pax(prm_monthly, pax_monthly, "Month")
    monthly = row_penetration(monthly, "Unique Count", "Total Pax")

    monthly = add_budget_comparison(monthly, budget_df, "Month")


    amb_monthly = group_ambulift_by_time(prm_df, "Operation Date", "M", out_col="Month")

    monthly=monthly.merge(amb_monthly, on= "Month", how="left")
    monthly["Ambulift PRMs"] = monthly["Ambulift PRMs"].fillna(0)


    monthly = row_penetration(monthly, "Ambulift PRMs", "Unique Count", out_col="Percent Ambulift")
    
    monthly["Year"]       = monthly["Month"].dt.year
    monthly["Month Name"] = monthly["Month"].dt.strftime("%b")


    return monthly

def build_daily(prm_df):
    return group_prm_by_time(prm_df, "Operation Date", "D", out_col="Day")

def build_yearly(monthly, daily, budget_df):
    #Annual aggregates
    y_prm = group_sum(monthly, ["Year"], "Unique Count", "Distinct PRMs")
    y_pax = group_sum(monthly, ["Year"], "Total Pax", "Total Pax")
    y_amb = group_sum(monthly, ["Year"], "Ambulift PRMs", "Ambulift PRMs")


    #merge together
    yearly = (
        y_prm.merge(y_amb, on="Year")
            .merge(y_pax, on="Year")
    )

    #Rename to match budget-comparison schema
    yearly = yearly.rename(columns={"Distinct PRMs": "Unique Count"})

    #Compute yearly penetration
    yearly = row_penetration(yearly, numerator_col="Unique Count", denominator_col="Total Pax", out_col="Penetration Rate")

    
    # --- Aggregate budget to Year level first ---
    budget_year = budget_df.groupby("Year", as_index=False).agg({
        "Budget PRM Demand": "sum",
        "Budget Penetration Rate": "mean"
    })

    # --- Apply budget comparison --
    yearly = add_budget_comparison(yearly, budget_year, bucket_col="Year")


    

    #Peak PRM day
    peak_date, peak_count = peak_day(daily.set_index("Day")["Unique Count"])
    yearly["Peak Day"] = peak_date
    yearly["Peak Count"] = peak_count

    #Complaints:
    complaints = compute_complaints_rolling_window(budget_df, window=3, value_col="Complaints Per 1k", wide=True)
    yearly = yearly.merge(
        complaints, how = "cross")
    
    yearly = yearly.merge(
        compute_ecac_yearly_means(budget_df),
        on="Year",
        how="left"
    )

    return yearly

#---------------------------------------------------------------
# Build Breakdown tables (SSR + Adhoc/Planned)
#---------------------------------------------------------------
def build_breakdowns(prm_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Build PRM, Ambulift, Landside RC, and Airside RC breakdowns and return the merged summary tables used for Excel output.
    
    Parameters
    ----------
    prm_df: pandas.DataFrame
        input prm dataset
        
    Returns
    --------
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

    #Compute 4 domain-level breakdowns
    prm_bk = prm_breakdowns(prm_df)
    ambu_bk = ambulift_breakdowns(prm_df)
    land_bk = landside_RC_breakdowns(prm_df)
    air_bk = airside_RC_breakdowns(prm_df)

    
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

#---------------------------------------------------------------
# DEBUGGING
#---------------------------------------------------------------


def debug_prm_spanning_months(prm_df: pd.DataFrame, excel_out: str | None = None):
    """
    TEMP DEBUG: Create Excel tabs to show:
       • PRMs in multiple months
       • PRMs crossing midnight (consecutive days)
       • PRMs crossing midnight *and* crossing into a different month (likely cause of discrepancies)

    Tabs written (if excel_out provided):
      - "PRMs in Multiple Months"
      - "Cross-Midnight PRMs"
      - "Cross-Midnight Across Months"
      - "PRM Debug Summary"

    Returns dict of all DataFrames for interactive use.
    """

    import pandas as pd

    if prm_df is None or prm_df.empty:
        summary = pd.DataFrame({
            "Metric": [
                "Distinct PRMs in >1 month",
                "Distinct PRMs crossing midnight",
                "Distinct PRMs crossing midnight AND crossing months",
            ],
            "Value": [0, 0, 0],
        })
        if excel_out:
            write_once_then_update(excel_out, "PRM Debug Summary", summary, anchor="A2", include_header=True)
        return {"multi_month": pd.DataFrame(), "cross_midnight": pd.DataFrame(),
                "cross_midnight_across_months": pd.DataFrame(), "summary": summary}

    df = prm_df.copy()
    df["Operation Date"] = pd.to_datetime(df["Operation Date"], errors="coerce")

    # ==========================================
    # 1) PRMs in multiple months
    # ==========================================
    df["YearMonth_Period"] = df["Operation Date"].dt.to_period("M")
    month_counts = df.groupby("Passenger ID")["YearMonth_Period"].nunique()
    multi_month_ids = month_counts[month_counts > 1].index

    multi_month_rows = (
        df[df["Passenger ID"].isin(multi_month_ids)]
        .sort_values(["Passenger ID", "Operation Date"])
        .drop(columns=["YearMonth_Period"])
        .copy()
    )

    # ==========================================
    # 2) Cross-midnight PRMs
    # ==========================================
    dx = df.sort_values(["Passenger ID", "Operation Date"]).copy()
    dx["OpDateOnly"] = dx["Operation Date"].dt.normalize()

    dx["Prev Operation Date"] = dx.groupby("Passenger ID")["Operation Date"].shift(1)
    dx["Prev OpDateOnly"]     = dx.groupby("Passenger ID")["OpDateOnly"].shift(1)
    dx["Day Diff"]            = (dx["OpDateOnly"] - dx["Prev OpDateOnly"]).dt.days

    consecutive_flags = dx.groupby("Passenger ID")["Day Diff"].apply(lambda s: (s == 1).any())
    cross_midnight_ids = consecutive_flags[consecutive_flags].index

    cross_midnight_rows = dx[dx["Passenger ID"].isin(cross_midnight_ids)].copy()
    cross_midnight_rows = cross_midnight_rows.drop(columns=["YearMonth_Period"], errors="ignore")

    # ==========================================
    # 3) Cross-midnight *and* crossing into a new month
    # ==========================================
    dx["Prev Month"] = dx["Prev OpDateOnly"].dt.month
    dx["Curr Month"] = dx["OpDateOnly"].dt.month
    dx["Month Changed"] = dx["Prev Month"] != dx["Curr Month"]

    cross_month_mask = (dx["Day Diff"] == 1) & (dx["Month Changed"])
    cross_midnight_month_ids = dx.loc[cross_month_mask, "Passenger ID"].unique()

    cross_midnight_across_months = (
        dx[dx["Passenger ID"].isin(cross_midnight_month_ids)]
        .copy()
        .drop(columns=["YearMonth_Period"], errors="ignore")
    )

    # ==========================================
    # 4) Summary table
    # ==========================================
    summary = pd.DataFrame({
        "Metric": [
            "Distinct PRMs in >1 month",
            "Distinct PRMs crossing midnight",
            "Distinct PRMs crossing midnight AND crossing months",
        ],
        "Value": [
            int(len(multi_month_ids)),
            int(len(cross_midnight_ids)),
            int(len(cross_midnight_month_ids)),
        ],
    })

    # ==========================================
    # 5) Write Excel tabs
    # ==========================================
    if excel_out:
        write_once_then_update(excel_out, "PRMs in Multiple Months",
                               multi_month_rows, anchor="A2", include_header=True)

        write_once_then_update(excel_out, "Cross-Midnight PRMs",
                               cross_midnight_rows, anchor="A2", include_header=True)

        write_once_then_update(excel_out, "Cross-Midnight Across Months",
                               cross_midnight_across_months, anchor="A2", include_header=True)

        write_once_then_update(excel_out, "PRM Debug Summary",
                               summary, anchor="A2", include_header=True)

    return {
        "multi_month": multi_month_rows,
        "cross_midnight": cross_midnight_rows,
        "cross_midnight_across_months": cross_midnight_across_months,
        "summary": summary,
    }



#---------------------------------------------------------------
# Main
#---------------------------------------------------------------

def main(start: str, end: str, budget_path: str, excel_out: str | None):

    
    print("\nPRM REPORT")
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
    monthly = build_monthly(prm_df, pax_df, budget_df)
    daily   = build_daily(prm_df)
    t4 = step(t3, "Monthly & Daily computed.")

    # 5) Yearly
    print("[5/8] Building Yearly summary…")
    yearly = build_yearly(monthly, daily, budget_df)
    t5 = step(t4, "Yearly summary built.")

    print("[6/8] Calculating PRM Growth…")
    prm_demand_growth = period_growth(
        loader_fn=load_prm_data,
        start=start,
        end=end,
        years_back=3,
        id_col="Passenger ID"
    )
    t6 = step(t5, "PRM Growth Calculated.")

    print("[7/8] Calculating SSR Code and Booking Type Breakdown…")
    breakdowns = build_breakdowns(prm_df)

    t7 = step(t6, "SSR Code and Booking Type Breakdown Calculated.")

    # 6) Excel output
    if excel_out:
        print("[8/8] Writing Excel output…")
        write_once_then_update(excel_out, "Monthly_PRMs",   monthly, anchor="A2", include_header=True)
        write_once_then_update(excel_out, "Yearly_Summary", yearly,  anchor="A2", include_header=True)
        write_once_then_update(excel_out, "3 Year PRM Demand Growth", prm_demand_growth)
        write_once_then_update(excel_out, "PRM Breakdown by SSR",     breakdowns["by_ssr"],     anchor="A2", include_header=True)
        write_once_then_update(excel_out, "PRM Breakdown by Booking", breakdowns["by_booking"], anchor="A2", include_header=True)

        
    # --- TEMP debug tabs ---
        print("     • Adding debug tabs: multi-month / cross-midnight …")
        debug_prm_spanning_months(prm_df, excel_out)


        step(t7, f"Excel updated → {excel_out}")

    print("\n✔ PRM report complete.\n")


# ================================================================
# ENTRYPOINT
# ================================================================

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="PRM Report (Refactored, query-based)")
    p.add_argument("--start",  required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end",    required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--budget", required=False, help="Path to PRM Budget Excel (optional: uses default if omitted")
    p.add_argument("--out",    default=None, help="Output Excel path")
    args = p.parse_args()

    main(args.start, args.end, args.budget, args.out)
