
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# -----------------------------
# Project imports (repo root)
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.utils.query import query

# -----------------------------
# Config
# -----------------------------
CSV_PATH = r"C:\Users\jamie_douglas\Edinburgh Airport Limited\Shared Files - Business Planning\Forecasting\Forecast prep automation_test\Historic forecasts\23022026_sch_output.csv"

# If you want to extend the end date beyond what’s in Excel, set this (YYYY-MM-DD) else None
END_DATE_OVERRIDE = None  # e.g. "2026-10-25"

# Remove known non-flight placeholders if present
EXCLUDE_AIRLINES = {"99"}
EXCLUDE_AIRPORTS = {"STAFF"}

# -----------------------------
# Helpers
# -----------------------------
def _clean_str(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.strip()
         .str.upper()
         .replace({"": np.nan, "NAN": np.nan, "NONE": np.nan})
    )

def load_future_flights(start: str, end: str) -> pd.DataFrame:
    """
    Load future flight schedule from EAL.FlightPerformance_FutureFlights.
    """
    df = query(
        table="EAL.FlightPerformance_FutureFlights",
        columns=[
            "FlightID",
            "ScheduledDateTime_Local",
            "ArrDeptureCode",
            "FlightNumber",
            "AirlineCode_IATA",
            "AirportCode_IATA",        # <- airport code you want
            "Domestic_International",  # <- domestic vs international lives here
            "Sector",                  # <- NOT used for matching; kept for debugging
        ],
        where=[
            "ScheduledDateTime_Local >= :start",
            "ScheduledDateTime_Local < :end",
            "IsPassengerFlight = 1",
        ],
        params={"start": start, "end": end},
        query_option="OPTION (RECOMPILE)",
    )
    return df

# -----------------------------
# Load CSV (Excel extract)
# -----------------------------
df_excel = pd.read_csv(CSV_PATH)
df_excel.columns = [c.strip() for c in df_excel.columns]

# Parse scheduled datetime from Date + Time
df_excel["ScheduledDateTime_Local"] = pd.to_datetime(
    df_excel["Date"].astype(str).str.strip() + " " + df_excel["Time"].astype(str).str.strip(),
    dayfirst=True,
    errors="coerce"
)
df_excel = df_excel.dropna(subset=["ScheduledDateTime_Local"])

# Align column names to match DB naming
df_excel = df_excel.rename(columns={
    "AD": "ArrDeptureCode",
    "Airline": "AirlineCode_IATA",
    "Airport": "AirportCode_IATA",
})

# Clean join fields
df_excel["ArrDeptureCode"] = _clean_str(df_excel["ArrDeptureCode"])
df_excel["AirlineCode_IATA"] = _clean_str(df_excel["AirlineCode_IATA"])
df_excel["AirportCode_IATA"] = _clean_str(df_excel["AirportCode_IATA"])

# Remove placeholders / junk rows
df_excel = df_excel[~df_excel["AirlineCode_IATA"].isin(EXCLUDE_AIRLINES)]
df_excel = df_excel[~df_excel["AirportCode_IATA"].isin(EXCLUDE_AIRPORTS)]

# Build Date key (per-day comparison)
df_excel["SchedDate"] = df_excel["ScheduledDateTime_Local"].dt.normalize()

excel_keys = df_excel[["ArrDeptureCode", "SchedDate", "AirlineCode_IATA", "AirportCode_IATA"]].dropna()

if excel_keys.empty:
    raise ValueError("No valid Excel rows after parsing/cleaning. Check columns and placeholders.")

# -----------------------------
# Window: tomorrow -> end_date
# -----------------------------
tomorrow = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)

# End date defaults to last date present in the CSV (but can be overridden)
end_date = excel_keys["SchedDate"].max()
if END_DATE_OVERRIDE is not None:
    end_date = pd.to_datetime(END_DATE_OVERRIDE).normalize()

# Filter Excel to tomorrow onwards (as you requested)
excel_keys = excel_keys[excel_keys["SchedDate"] >= tomorrow]

if excel_keys.empty:
    raise ValueError(
        f"No Excel flights found from tomorrow ({tomorrow.date()}) onward. "
        f"Max date in file is {df_excel['SchedDate'].max().date()}."
    )

# Use the same end date for the DB pull (end is exclusive; add 1 day)
start_date = tomorrow
db_end_exclusive = (end_date + pd.Timedelta(days=1))

# -----------------------------
# Load FutureFlights
# -----------------------------
df_future = load_future_flights(
    start=start_date.strftime("%Y-%m-%d"),
    end=db_end_exclusive.strftime("%Y-%m-%d"),
)

if df_future is None or len(df_future) == 0:
    raise ValueError(f"FutureFlights returned 0 rows for {start_date.date()} → {end_date.date()}.")

df_future["ScheduledDateTime_Local"] = pd.to_datetime(df_future["ScheduledDateTime_Local"], errors="coerce")
df_future = df_future.dropna(subset=["ScheduledDateTime_Local"])

df_future["ArrDeptureCode"] = _clean_str(df_future["ArrDeptureCode"])
df_future["AirlineCode_IATA"] = _clean_str(df_future["AirlineCode_IATA"])
df_future["AirportCode_IATA"] = _clean_str(df_future["AirportCode_IATA"])

df_future["SchedDate"] = df_future["ScheduledDateTime_Local"].dt.normalize()

future_keys = df_future[["ArrDeptureCode", "SchedDate", "AirlineCode_IATA", "AirportCode_IATA"]].dropna()

# -----------------------------
# COUNT reconciliation
# -----------------------------
group_cols = ["ArrDeptureCode", "SchedDate", "AirlineCode_IATA", "AirportCode_IATA"]

excel_counts = (
    excel_keys.groupby(group_cols)
              .size()
              .rename("excel_count")
              .reset_index()
)

future_counts = (
    future_keys.groupby(group_cols)
               .size()
               .rename("future_count")
               .reset_index()
)

recon = excel_counts.merge(future_counts, on=group_cols, how="outer")
recon["excel_count"] = recon["excel_count"].fillna(0).astype(int)
recon["future_count"] = recon["future_count"].fillna(0).astype(int)

# Missing means Excel has more flights than FutureFlights for that key
recon["missing_count"] = recon["excel_count"] - recon["future_count"]

missing = recon[recon["missing_count"] > 0].sort_values(
    ["SchedDate", "ArrDeptureCode", "AirlineCode_IATA", "AirportCode_IATA"]
)

# Optional: show also “extra in future” if you care
extra_in_future = recon[recon["missing_count"] < 0].copy()
extra_in_future["extra_count"] = -extra_in_future["missing_count"]
extra_in_future = extra_in_future.sort_values(
    ["SchedDate", "ArrDeptureCode", "AirlineCode_IATA", "AirportCode_IATA"]
)

# -----------------------------
# Print summary + save outputs
# -----------------------------
print("---- Window ----")
print("Start:", start_date)
print("End  :", end_date)
print("Excel rows:", len(excel_keys))
print("Future rows:", len(future_keys))

print("\n---- Reconciliation summary ----")
print("Keys in Excel:", len(excel_counts))
print("Keys in Future:", len(future_counts))
print("Keys missing (excel>future):", len(missing))
print("Total missing flights:", int(missing["missing_count"].sum()))

print("\n---- Missing (first 50 keys) ----")
print(missing.head(50).to_string(index=False))


# -----------------------------------
# Aggregate missing flights
# by A/D, Airline, Airport
# -----------------------------------
summary_missing = (
    missing
        .groupby(
            ["ArrDeptureCode", "AirlineCode_IATA", "AirportCode_IATA"],
            as_index=False
        )["missing_count"]
        .sum()
        .rename(columns={"missing_count": "total_missing_flights"})
        .sort_values("total_missing_flights", ascending=False)
)

print("\n---- Missing flights summary (A/D × Airline × Airport) ----")
print(summary_missing.to_string(index=False))


output_path = Path("outputs") / "schedule_analysis.xlsx"
output_path.parent.mkdir(parents=True, exist_ok=True)

with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
    missing.to_excel(writer, sheet_name="missing_flights_detail", index=False)
    summary_missing.to_excel(writer, sheet_name="missing_flights_summary", index=False)

print(f"Saved Excel output to: {output_path}")