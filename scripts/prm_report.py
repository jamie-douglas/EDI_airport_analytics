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
from modules.domain.prm.demand import group_prm_by_time, group_pax_by_time, merge_pax, add_budget_comparison, growth_unique_passengers, compute_complaints_rolling_window, compute_ecac_yearly_means
from modules.domain.prm.ambulift import group_ambulift_by_time

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
            "VehicleTypeName AS [Vehicle Type]",
        ],
        where = ["BillingPRM = 1",
                 "Operation_DateID_Local >= :start_op",
                 "Operation_DateID_Local < :end_op",
        ],
        params= {"start_op": start_op, "end_op": end_op},
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
# Main
#---------------------------------------------------------------

def main(start: str, end: str, budget_path: str, excel_out: str | None):

    
    print("\nPRM REPORT")
    print(f"Window : {start} → {end}\n")

    t0 = time.perf_counter()

    # 1) PRM
    print("[1/7] Loading PRM jobs…")
    prm_df = load_prm_data(start, end)
    t1 = step(t0, f"Loaded PRM rows: {len(prm_df):,}")
    if prm_df.empty:
        print("No PRM jobs found. Exiting.")
        return

    # 2) Pax
    print("[2/7] Loading Pax…")
    pax_df = load_passenger_data(start, end)
    t2 = step(t1, f"Loaded Pax rows: {len(pax_df):,}")

    # 3) Budget
    print("[3/7] Loading Budget Excel…")
    budget_df = load_prm_budget_data(budget_path)
    t3 = step(t2, "Budget loaded.")

    # 4) Monthly + Daily
    print("[4/7] Building Monthly & Daily…")
    monthly = build_monthly(prm_df, pax_df, budget_df)
    daily   = build_daily(prm_df)
    t4 = step(t3, "Monthly & Daily computed.")

    # 5) Yearly
    print("[5/7] Building Yearly summary…")
    yearly = build_yearly(monthly, daily, budget_df)
    t5 = step(t4, "Yearly summary built.")

    print("[6/7] Calculating PRM Growth…")
    prm_demand_growth = period_growth(
        loader_fn=load_prm_data,
        start=start,
        end=end,
        years_back=3,
        id_col="Passenger ID"
    )
    t6 = step(t5, "PRM Growth Calculated.")

    
    # 1) Window‑distinct (should match period_growth current-year 'Count')
    prm_window = load_prm_data(start, end)
    window_distinct = prm_window["Passenger ID"].nunique()
    print("Window‑distinct Passenger IDs:", window_distinct)

    # 2) Sum of monthly uniques (should match Yearly summary for the year)
    prm_monthly = group_prm_by_time(prm_window, "Operation Date", "M", out_col="Month")
    sum_of_monthly = int(prm_monthly["Unique Count"].sum())
    print("Sum of monthly uniques:", sum_of_monthly)

    
    prm = load_prm_data(start, end).copy()
    prm["Month"] = prm["Operation Date"].dt.to_period("M")

    # Count distinct months per Passenger ID
    months_per_id = (
        prm.groupby("Passenger ID")["Month"].nunique()
        .sort_values(ascending=False)
    )

    # Offenders that appear in >1 month
    repeat_ids = months_per_id[months_per_id > 1]
    print("Passengers in >1 month:", len(repeat_ids))
    print("Extra monthly appearances (should equal 78):", int((repeat_ids - 1).sum()))
    print(repeat_ids.head(20))


    # 6) Excel output
    if excel_out:
        print("[7/7] Writing Excel output…")
        write_once_then_update(excel_out, "Monthly_PRMs",   monthly, anchor="A2", include_header=True)
        write_once_then_update(excel_out, "Yearly_Summary", yearly,  anchor="A2", include_header=True)
        write_once_then_update(excel_out, "3 Year PRM Demand Growth", prm_demand_growth)
        step(t6, f"Excel updated → {excel_out}")

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
