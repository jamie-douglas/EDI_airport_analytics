# %% [markdown]
# # PRM Penetration Rate Backtest
#
# Purpose:
# Pull PRM and Pax data from the database and compare different penetration-rate methods:
#
# 1. Country + Month
# 2. Airline + Country
# 3. Airline + Country + Month
# 4. Overall monthly airport uplift
#
# The script backtests year-on-year performance for the past 3 years where data exists.
#
# It answers:
# - Is penetration better forecast by country?
# - By airline + country?
# - By airline + country + month?
# - Does month-based uplift improve accuracy?
# - What uplift should be applied?
#
# Output:
# - Excel workbook with summary tabs
# - CSV extracts for review

# %%
import pyodbc
import pandas as pd
import numpy as np
import sys
from datetime import datetime
from pathlib import Path
from tabulate import tabulate

# %% [markdown]
# # User Configuration

# %%
email = "jamie_douglas@edinburghairport.com"
dsn = "AzureConnection"

# Pull enough history for YoY testing
# Example: 2023, 2024, 2025, and current available 2026 if present
data_start_date = "2023-01-01"
prm_sql_start = "202301"
prm_sql_end = "202612"

# Minimum volume thresholds to avoid tiny-route noise
MIN_PAX_GROUP = 500
MIN_PRM_GROUP = 1

# Output folder
output_dir = Path(
    r"C:\Users\jamie_douglas\OneDrive - Edinburgh Airport Limited\Documents\GitHub\EDI_airport_analytics\output\PRM"
)

output_dir.mkdir(parents=True, exist_ok=True)

output_excel_path = output_dir / "penetration_rate_backtest.xlsx"
raw_prm_csv = output_dir / "raw_prm.csv"
raw_pax_csv = output_dir / "raw_pax.csv"
merged_flight_csv = output_dir / "merged_flight.csv"

# %% [markdown]
# # Connect to Database

# %%
try:
    conn = pyodbc.connect("DSN=" + dsn + ";UID=" + email)
    print("Database connection successful.")
except pyodbc.Error as ex:
    print(f"Error connecting to database: {ex}")
    print("Check VPN / DSN / credentials.")
    sys.exit(1)
except pyodbc.InterfaceError as ex:
    print(f"ODBC interface error: {ex}")
    sys.exit(1)

# %% [markdown]
# # Pull PRM Data
#
# This mirrors Yini's logic:
# - Source: AvTech.CompletedServicesByJob
# - PRM definition: BillingPRM = 1
# - PRM count: distinct Passenger ID by flight

# %%
prm_sql = f"""
SELECT
    FlightID,
    LEFT(CAST(Operation_DateID_Local AS VARCHAR(8)), 6) AS YearMonth,
    AirlineCode_IATA AS [Airline Code],
    AirportCode_IATA AS Airport,
    passengerID AS [Passenger ID],
    BillingPRM
FROM AvTech.CompletedServicesByJob
WHERE
    BillingPRM = 1
    AND LEFT(CAST(Operation_DateID_Local AS VARCHAR(8)), 6) >= '{prm_sql_start}'
    AND LEFT(CAST(Operation_DateID_Local AS VARCHAR(8)), 6) <= '{prm_sql_end}'
"""

df_prm_raw = pd.read_sql(prm_sql, conn)

df_prm_raw["Year"] = df_prm_raw["YearMonth"].str[:4].astype(int)
df_prm_raw["Month"] = df_prm_raw["YearMonth"].str[4:6].astype(int)

df_prm_raw.to_csv(raw_prm_csv, index=False)

print("PRM rows pulled:", len(df_prm_raw))
print(tabulate(df_prm_raw.head(), headers="keys", tablefmt="psql", showindex=False))

# %% [markdown]
# # Pull Pax Data
#
# This mirrors Yini's logic:
# - Source: EAL.FlightPerformance
# - Pax field: Pax_MostConfident
# - Country field: CountryName

# %%
pax_sql = f"""
SELECT
    FlightID,
    DATEPART(YEAR, ActualDateTime_Local) AS Year,
    DATEPART(MONTH, ActualDateTime_Local) AS Month,
    AirlineCode_IATA AS [Airline Code],
    AirportCode_IATA AS Airport,
    CountryName,
    Pax_MostConfident AS Pax
FROM EAL.FlightPerformance
WHERE
    ActualDateTime_Local >= '{data_start_date}'
    AND IsPassengerFlight = 1
ORDER BY
    Year,
    Month,
    AirlineCode_IATA,
    AirportCode_IATA
"""

df_pax_raw = pd.read_sql(pax_sql, conn)

df_pax_raw.to_csv(raw_pax_csv, index=False)

print("Pax rows pulled:", len(df_pax_raw))
print(tabulate(df_pax_raw.head(), headers="keys", tablefmt="psql", showindex=False))

# %% [markdown]
# # Prepare Flight-Level Dataset
#
# Important:
# Pax is flight-level.
# PRM is passenger-level.
#
# So we first aggregate PRM to FlightID, then merge onto flight-level pax.
# This avoids duplicating Pax by passenger-level PRM rows.

# %%
df_prm_flight = (
    df_prm_raw
    .groupby(["FlightID"], dropna=False)
    .agg(
        prm_count=("Passenger ID", "nunique"),
        prm_airline=("Airline Code", "first"),
        prm_airport=("Airport", "first"),
        prm_year=("Year", "first"),
        prm_month=("Month", "first"),
    )
    .reset_index()
)

merged_flight = df_pax_raw.merge(
    df_prm_flight,
    on="FlightID",
    how="left"
)

merged_flight["prm_count"] = merged_flight["prm_count"].fillna(0)

# Use Pax-side airline/country as the main reference for penetration denominators
merged_flight["Pax"] = pd.to_numeric(merged_flight["Pax"], errors="coerce").fillna(0)

merged_flight.to_csv(merged_flight_csv, index=False)

print("Merged flight rows:", len(merged_flight))
print(tabulate(merged_flight.head(), headers="keys", tablefmt="psql", showindex=False))

# %% [markdown]
# # Helper Functions

# %%
def safe_divide(n, d):
    return np.where(d > 0, n / d, 0.0)


def weighted_mape(actual, predicted):
    """
    Weighted absolute percentage error.
    Better than normal MAPE because it weights high-volume groups more heavily.
    """
    actual = np.asarray(actual)
    predicted = np.asarray(predicted)

    denominator = actual.sum()

    if denominator == 0:
        return np.nan

    return np.abs(actual - predicted).sum() / denominator


def rmse(actual, predicted):
    actual = np.asarray(actual)
    predicted = np.asarray(predicted)
    return np.sqrt(np.mean((actual - predicted) ** 2))


def mae(actual, predicted):
    actual = np.asarray(actual)
    predicted = np.asarray(predicted)
    return np.mean(np.abs(actual - predicted))


def make_penetration_table(df, group_cols):
    """
    Creates penetration table:
        group_cols + Year + Month if provided in group_cols
        Pax
        PRM
        penetration
    """
    out = (
        df
        .groupby(group_cols, dropna=False)
        .agg(
            pax=("Pax", "sum"),
            prm=("prm_count", "sum"),
            flights=("FlightID", "nunique"),
        )
        .reset_index()
    )

    out["penetration"] = safe_divide(out["prm"], out["pax"])

    return out


def add_yoy_columns(df, join_cols):
    """
    Adds previous-year penetration and YoY uplift for a given grouping.
    """
    current = df.copy()

    previous = df.copy()
    previous["Year"] = previous["Year"] + 1

    previous = previous.rename(
        columns={
            "penetration": "previous_year_penetration",
            "pax": "previous_year_pax",
            "prm": "previous_year_prm",
            "flights": "previous_year_flights",
        }
    )

    keep_cols = (
        join_cols
        + [
            "previous_year_penetration",
            "previous_year_pax",
            "previous_year_prm",
            "previous_year_flights",
        ]
    )

    current = current.merge(
        previous[keep_cols],
        on=join_cols,
        how="left",
    )

    current["yoy_uplift"] = safe_divide(
        current["penetration"] - current["previous_year_penetration"],
        current["previous_year_penetration"],
    )

    return current


def summarise_backtest(results, method_name):
    """
    Summarise prediction accuracy.
    """
    valid = results[
        results["actual_prm"].notnull()
        & results["predicted_prm"].notnull()
        & (results["actual_pax"] > 0)
    ].copy()

    if valid.empty:
        return {
            "method": method_name,
            "rows": 0,
            "actual_prm": np.nan,
            "predicted_prm": np.nan,
            "bias_prm": np.nan,
            "bias_pct": np.nan,
            "wape": np.nan,
            "mae": np.nan,
            "rmse": np.nan,
        }

    actual_total = valid["actual_prm"].sum()
    predicted_total = valid["predicted_prm"].sum()
    bias = predicted_total - actual_total

    return {
        "method": method_name,
        "rows": len(valid),
        "actual_prm": actual_total,
        "predicted_prm": predicted_total,
        "bias_prm": bias,
        "bias_pct": safe_divide(np.array([bias]), np.array([actual_total]))[0],
        "wape": weighted_mape(valid["actual_prm"], valid["predicted_prm"]),
        "mae": mae(valid["actual_prm"], valid["predicted_prm"]),
        "rmse": rmse(valid["actual_prm"], valid["predicted_prm"]),
    }


# %% [markdown]
# # Build Actual Penetration Tables

# %%
# Overall airport by year/month
actual_airport_month = make_penetration_table(
    merged_flight,
    ["Year", "Month"],
)

# Country + month
actual_country_month = make_penetration_table(
    merged_flight,
    ["Year", "Month", "CountryName"],
)

# Airline + country
actual_airline_country = make_penetration_table(
    merged_flight,
    ["Year", "Airline Code", "CountryName"],
)

# Airline + country + month
actual_airline_country_month = make_penetration_table(
    merged_flight,
    ["Year", "Month", "Airline Code", "CountryName"],
)

# Country only by year
actual_country = make_penetration_table(
    merged_flight,
    ["Year", "CountryName"],
)

# Airline only by year
actual_airline = make_penetration_table(
    merged_flight,
    ["Year", "Airline Code"],
)

print("Airport-month penetration:")
print(tabulate(actual_airport_month.head(20), headers="keys", tablefmt="psql", showindex=False))

# %% [markdown]
# # Calculate YoY Uplifts

# %%
airport_month_yoy = add_yoy_columns(
    actual_airport_month,
    ["Year", "Month"],
)

country_month_yoy = add_yoy_columns(
    actual_country_month,
    ["Year", "Month", "CountryName"],
)

airline_country_yoy = add_yoy_columns(
    actual_airline_country,
    ["Year", "Airline Code", "CountryName"],
)

airline_country_month_yoy = add_yoy_columns(
    actual_airline_country_month,
    ["Year", "Month", "Airline Code", "CountryName"],
)

country_yoy = add_yoy_columns(
    actual_country,
    ["Year", "CountryName"],
)

airline_yoy = add_yoy_columns(
    actual_airline,
    ["Year", "Airline Code"],
)

# %% [markdown]
# # Build Backtest Predictions
#
# For each actual year, predict that year using the previous year's penetration.
#
# Method examples:
#
# - Country + Month:
#   Use previous year's same country/month penetration.
#
# - Airline + Country:
#   Use previous year's same airline/country penetration.
#
# - Airline + Country + Month:
#   Use previous year's same airline/country/month penetration.
#
# - Airport Monthly Uplift:
#   Use previous year's same group penetration, then apply airport-level monthly YoY uplift.
#
# This lets us see whether month / airline / country adds useful predictive power.

# %%
actual_target = actual_airline_country_month.rename(
    columns={
        "pax": "actual_pax",
        "prm": "actual_prm",
        "penetration": "actual_penetration",
        "flights": "actual_flights",
    }
)

# Only evaluate groups with enough volume
actual_target = actual_target[
    (actual_target["actual_pax"] >= MIN_PAX_GROUP)
].copy()

# %% [markdown]
# ## Method 1: Country + Month

# %%
base_country_month = actual_country_month.copy()
base_country_month["Year"] = base_country_month["Year"] + 1

base_country_month = base_country_month.rename(
    columns={
        "penetration": "base_penetration",
        "pax": "base_pax",
        "prm": "base_prm",
    }
)

m1 = actual_target.merge(
    base_country_month[
        ["Year", "Month", "CountryName", "base_penetration", "base_pax", "base_prm"]
    ],
    on=["Year", "Month", "CountryName"],
    how="left",
)

m1["predicted_penetration"] = m1["base_penetration"]
m1["predicted_prm"] = m1["actual_pax"] * m1["predicted_penetration"]
m1["method"] = "Country + Month"

# %% [markdown]
# ## Method 2: Airline + Country

# %%
base_airline_country = actual_airline_country.copy()
base_airline_country["Year"] = base_airline_country["Year"] + 1

base_airline_country = base_airline_country.rename(
    columns={
        "penetration": "base_penetration",
        "pax": "base_pax",
        "prm": "base_prm",
    }
)

m2 = actual_target.merge(
    base_airline_country[
        ["Year", "Airline Code", "CountryName", "base_penetration", "base_pax", "base_prm"]
    ],
    on=["Year", "Airline Code", "CountryName"],
    how="left",
)

m2["predicted_penetration"] = m2["base_penetration"]
m2["predicted_prm"] = m2["actual_pax"] * m2["predicted_penetration"]
m2["method"] = "Airline + Country"

# %% [markdown]
# ## Method 3: Airline + Country + Month

# %%
base_airline_country_month = actual_airline_country_month.copy()
base_airline_country_month["Year"] = base_airline_country_month["Year"] + 1

base_airline_country_month = base_airline_country_month.rename(
    columns={
        "penetration": "base_penetration",
        "pax": "base_pax",
        "prm": "base_prm",
    }
)

m3 = actual_target.merge(
    base_airline_country_month[
        [
            "Year",
            "Month",
            "Airline Code",
            "CountryName",
            "base_penetration",
            "base_pax",
            "base_prm",
        ]
    ],
    on=["Year", "Month", "Airline Code", "CountryName"],
    how="left",
)

m3["predicted_penetration"] = m3["base_penetration"]
m3["predicted_prm"] = m3["actual_pax"] * m3["predicted_penetration"]
m3["method"] = "Airline + Country + Month"

# %% [markdown]
# ## Method 4: Airline + Country Base with Airport Monthly Uplift
#
# This is effectively:
#
# historical airline/country penetration
# x
# airport-wide monthly YoY uplift
#
# This is probably the best bridge between your model and Yini's method.

# %%
airport_month_uplift = airport_month_yoy[
    ["Year", "Month", "yoy_uplift"]
].rename(
    columns={"yoy_uplift": "airport_month_yoy_uplift"}
)

m4 = m2.copy()

m4 = m4.merge(
    airport_month_uplift,
    on=["Year", "Month"],
    how="left",
)

m4["airport_month_yoy_uplift"] = m4["airport_month_yoy_uplift"].fillna(0)

m4["predicted_penetration"] = (
    m4["base_penetration"]
    * (1 + m4["airport_month_yoy_uplift"])
)

m4["predicted_prm"] = m4["actual_pax"] * m4["predicted_penetration"]
m4["method"] = "Airline + Country + Airport Monthly Uplift"

# %% [markdown]
# ## Method 5: Airline + Country + Month with Airport Monthly Uplift
#
# This is the most granular seasonal method.
#
# Warning:
# It may overfit if some airline/country/month combinations are small.

# %%
m5 = m3.copy()

m5 = m5.merge(
    airport_month_uplift,
    on=["Year", "Month"],
    how="left",
)

m5["airport_month_yoy_uplift"] = m5["airport_month_yoy_uplift"].fillna(0)

m5["predicted_penetration"] = (
    m5["base_penetration"]
    * (1 + m5["airport_month_yoy_uplift"])
)

m5["predicted_prm"] = m5["actual_pax"] * m5["predicted_penetration"]
m5["method"] = "Airline + Country + Month + Airport Monthly Uplift"

# %% [markdown]
# ## Method 6: Yini-Style Country + Month with Airport Monthly Uplift + Additional Needs
#
# This approximates the existing accepted approach:
#
# country/month historical penetration
# x
# airport monthly YoY uplift
# +
# 0.02 percentage points

# %%
ADDITIONAL_NEEDS = 0.0002

m6 = m1.copy()

m6 = m6.merge(
    airport_month_uplift,
    on=["Year", "Month"],
    how="left",
)

m6["airport_month_yoy_uplift"] = m6["airport_month_yoy_uplift"].fillna(0)

m6["predicted_penetration"] = (
    m6["base_penetration"]
    * (1 + m6["airport_month_yoy_uplift"])
    + ADDITIONAL_NEEDS
)

m6["predicted_prm"] = m6["actual_pax"] * m6["predicted_penetration"]
m6["method"] = "Country + Month + Airport Monthly Uplift + 0.02pp"

# %% [markdown]
# # Combine Backtests and Summarise Accuracy

# %%
all_methods = pd.concat(
    [m1, m2, m3, m4, m5, m6],
    ignore_index=True,
)

# Keep only rows where we had a base penetration
all_methods_valid = all_methods[
    all_methods["base_penetration"].notnull()
].copy()

summary_rows = []

for method_name, method_df in all_methods_valid.groupby("method"):
    summary_rows.append(summarise_backtest(method_df, method_name))

accuracy_summary = pd.DataFrame(summary_rows)

accuracy_summary = accuracy_summary.sort_values(
    by=["wape", "abs_bias_pct" if "abs_bias_pct" in accuracy_summary.columns else "wape"],
    ascending=True,
)

accuracy_summary["abs_bias_pct"] = accuracy_summary["bias_pct"].abs()

accuracy_summary = accuracy_summary[
    [
        "method",
        "rows",
        "actual_prm",
        "predicted_prm",
        "bias_prm",
        "bias_pct",
        "abs_bias_pct",
        "wape",
        "mae",
        "rmse",
    ]
].sort_values("wape")

print("Accuracy summary:")
print(tabulate(accuracy_summary, headers="keys", tablefmt="psql", showindex=False))

# %% [markdown]
# # Monthly Accuracy by Method

# %%
monthly_accuracy = (
    all_methods_valid
    .groupby(["method", "Year", "Month"])
    .agg(
        actual_prm=("actual_prm", "sum"),
        predicted_prm=("predicted_prm", "sum"),
        actual_pax=("actual_pax", "sum"),
    )
    .reset_index()
)

monthly_accuracy["bias_prm"] = monthly_accuracy["predicted_prm"] - monthly_accuracy["actual_prm"]
monthly_accuracy["bias_pct"] = safe_divide(
    monthly_accuracy["bias_prm"],
    monthly_accuracy["actual_prm"],
)
monthly_accuracy["abs_error_prm"] = monthly_accuracy["bias_prm"].abs()
monthly_accuracy["abs_error_pct"] = safe_divide(
    monthly_accuracy["abs_error_prm"],
    monthly_accuracy["actual_prm"],
)

monthly_accuracy = monthly_accuracy.sort_values(
    ["method", "Year", "Month"]
)

print("Monthly accuracy sample:")
print(tabulate(monthly_accuracy.head(30), headers="keys", tablefmt="psql", showindex=False))

# %% [markdown]
# # Uplift Analysis
#
# This section calculates observed YoY uplift at different levels:
#
# - Airport + Month
# - Country + Month
# - Airline + Country
# - Airline + Country + Month
#
# This shows whether the uplift should be:
# - one global number
# - monthly
# - country-specific
# - airline-specific
# - too noisy at granular levels

# %%
def summarise_uplift(df, label, min_prev_pax=MIN_PAX_GROUP):
    valid = df[
        df["previous_year_penetration"].notnull()
        & (df["previous_year_pax"] >= min_prev_pax)
        & (df["previous_year_prm"] >= MIN_PRM_GROUP)
    ].copy()

    return {
        "level": label,
        "rows": len(valid),
        "weighted_avg_yoy_uplift": np.average(
            valid["yoy_uplift"],
            weights=valid["previous_year_pax"],
        ) if len(valid) > 0 else np.nan,
        "median_yoy_uplift": valid["yoy_uplift"].median() if len(valid) > 0 else np.nan,
        "p10_yoy_uplift": valid["yoy_uplift"].quantile(0.10) if len(valid) > 0 else np.nan,
        "p90_yoy_uplift": valid["yoy_uplift"].quantile(0.90) if len(valid) > 0 else np.nan,
        "std_yoy_uplift": valid["yoy_uplift"].std() if len(valid) > 0 else np.nan,
    }


uplift_summary = pd.DataFrame(
    [
        summarise_uplift(airport_month_yoy, "Airport + Month"),
        summarise_uplift(country_month_yoy, "Country + Month"),
        summarise_uplift(airline_country_yoy, "Airline + Country"),
        summarise_uplift(airline_country_month_yoy, "Airline + Country + Month"),
        summarise_uplift(country_yoy, "Country"),
        summarise_uplift(airline_yoy, "Airline"),
    ]
)

uplift_summary = uplift_summary.sort_values("std_yoy_uplift")

print("Uplift summary:")
print(tabulate(uplift_summary, headers="keys", tablefmt="psql", showindex=False))

# %% [markdown]
# # Monthly Uplift Recommendation
#
# This gives a Yini-style uplift by month, based on airport-wide YoY movement.
#
# It is usually more defensible than airline/country-specific uplift because it is less noisy.

# %%
monthly_uplift_recommendation = (
    airport_month_yoy[
        airport_month_yoy["previous_year_penetration"].notnull()
    ]
    .groupby("Month")
    .agg(
        avg_yoy_uplift=("yoy_uplift", "mean"),
        median_yoy_uplift=("yoy_uplift", "median"),
        min_yoy_uplift=("yoy_uplift", "min"),
        max_yoy_uplift=("yoy_uplift", "max"),
        observations=("yoy_uplift", "count"),
    )
    .reset_index()
)

monthly_uplift_recommendation["recommended_uplift"] = monthly_uplift_recommendation[
    "median_yoy_uplift"
]

print("Monthly uplift recommendation:")
print(tabulate(monthly_uplift_recommendation, headers="keys", tablefmt="psql", showindex=False))

# %% [markdown]
# # Granularity Coverage
#
# This checks how often each method can actually find last year's matching group.
#
# A super granular method might be accurate where it matches, but unusable if lots of groups are missing.

# %%
coverage_summary = (
    all_methods
    .assign(has_base=lambda x: x["base_penetration"].notnull())
    .groupby("method")
    .agg(
        total_rows=("FlightID", "count") if "FlightID" in all_methods.columns else ("actual_pax", "count"),
        rows_with_base=("has_base", "sum"),
        actual_pax_total=("actual_pax", "sum"),
        actual_prm_total=("actual_prm", "sum"),
    )
    .reset_index()
)

coverage_summary["coverage_pct"] = safe_divide(
    coverage_summary["rows_with_base"],
    coverage_summary["total_rows"],
)

print("Coverage summary:")
print(tabulate(coverage_summary, headers="keys", tablefmt="psql", showindex=False))

# %% [markdown]
# # Export Results

# %%
with pd.ExcelWriter(output_excel_path, engine="openpyxl") as writer:
    accuracy_summary.to_excel(writer, sheet_name="accuracy_summary", index=False)
    monthly_accuracy.to_excel(writer, sheet_name="monthly_accuracy", index=False)
    uplift_summary.to_excel(writer, sheet_name="uplift_summary", index=False)
    monthly_uplift_recommendation.to_excel(writer, sheet_name="monthly_uplift", index=False)
    coverage_summary.to_excel(writer, sheet_name="coverage_summary", index=False)

    airport_month_yoy.to_excel(writer, sheet_name="airport_month_yoy", index=False)
    country_month_yoy.to_excel(writer, sheet_name="country_month_yoy", index=False)
    airline_country_yoy.to_excel(writer, sheet_name="airline_country_yoy", index=False)
    airline_country_month_yoy.to_excel(writer, sheet_name="airline_country_month_yoy", index=False)

    all_methods_valid.to_excel(writer, sheet_name="all_backtest_rows", index=False)

print(f"Saved results to: {output_excel_path}")

# %% [markdown]
# # Suggested Interpretation
#
# Use the output sheets as follows:
#
# 1. accuracy_summary
#    - Main answer.
#    - Lowest WAPE is the best predictive method.
#
# 2. monthly_accuracy
#    - Shows whether performance differs by month.
#    - If some months are consistently under-forecast, monthly uplift is justified.
#
# 3. uplift_summary
#    - Shows how noisy each uplift level is.
#    - If Airline + Country + Month has a very high standard deviation, it may be too granular.
#
# 4. monthly_uplift
#    - Gives a clean monthly uplift assumption you can defend.
#
# 5. coverage_summary
#    - Shows whether the method has enough prior-year matches.
#    - A method with low error but poor coverage may not be operationally useful.