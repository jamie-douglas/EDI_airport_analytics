# scripts/monthly_prm_stand_report.py

import sys
import pathlib
from pathlib import Path
import argparse
import time

import pandas as pd
import numpy as np

# =====================================================================
# Project imports
# =====================================================================

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from modules.utils.query import query
from modules.utils.dates import to_datetime
from modules.utils.progress import step


# =====================================================================
# Report configuration
# =====================================================================

FOCUS_STANDS = ("8", "9", "10")
WHEELCHAIR_SSR_CODES = ("WCHC", "WCHS")
AMBULIFT_LABEL = "Ambulift"


# =====================================================================
# Generic helper functions
# =====================================================================

def print_section(title):
    print("")
    print("=" * 100)
    print(title)
    print("=" * 100)


def print_preview(name, df, rows=20):
    print_section(name)
    print("Rows: {:,}".format(len(df)))
    print("Columns: {:,}".format(len(df.columns)))

    if df.empty:
        print("No rows returned.")
    else:
        print(df.head(rows).to_string(index=False))


def normalise_text(value):
    if pd.isna(value):
        return ""

    return str(value).strip().upper()


def normalise_stand(value):
    if pd.isna(value):
        return ""

    s = str(value).strip().upper()

    if s.endswith(".0"):
        s = s[:-2]

    return s


def add_month_fields(df, date_col, month_col="Year Month"):
    x = df.copy()

    x[date_col] = pd.to_datetime(x[date_col], errors="coerce")
    x = x.dropna(subset=[date_col]).copy()

    x["Year"] = x[date_col].dt.year
    x["Month Number"] = x[date_col].dt.month
    x["Month Name"] = x[date_col].dt.strftime("%b")
    x[month_col] = x[date_col].dt.to_period("M").astype(str)

    return x


def month_pivot(df, index_cols, month_col, value_col, fill_value=0, add_total_col=True):
    if df.empty:
        out = pd.DataFrame(columns=index_cols)
        if add_total_col:
            out["Total"] = []
        return out

    out = (
        df.pivot_table(
            index=index_cols,
            columns=month_col,
            values=value_col,
            aggfunc="sum",
            fill_value=fill_value,
        )
        .reset_index()
    )

    out.columns.name = None

    month_cols = sorted([c for c in out.columns if c not in index_cols])
    out = out[index_cols + month_cols]

    if add_total_col and len(month_cols) > 0:
        out["Total"] = out[month_cols].sum(axis=1)

    return out


def save_outputs(outputs, excel_out, csv_dir=None):
    excel_path = Path(excel_out)
    excel_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for sheet_name, df in outputs.items():
            clean_sheet_name = sheet_name[:31]
            df.to_excel(writer, sheet_name=clean_sheet_name, index=False)

    print("")
    print("Excel output saved to: {}".format(excel_path))

    if csv_dir:
        csv_path = Path(csv_dir)
        csv_path.mkdir(parents=True, exist_ok=True)

        for name, df in outputs.items():
            clean_file_name = (
                name.lower()
                .replace(" ", "_")
                .replace("/", "_")
                .replace("\\", "_")
            )
            out_file = csv_path / "{}.csv".format(clean_file_name)
            df.to_csv(out_file, index=False)

        print("CSV outputs saved to: {}".format(csv_path))


# =====================================================================
# Data loaders
# =====================================================================

def load_prm_data(start, end):
    start_op = start.replace("-", "")
    end_op = end.replace("-", "")

    df = query(
        table="PRM.CompletedServicesByJob",
        columns=[
            "RequestID AS [Job ID]",
            "PassengerID AS [Passenger ID]",
            "FlightID AS [Flight ID]",
            "AirlineCode_IATA AS [Airline Code]",
            "FlightNumber AS [Flight Number]",
            "ArrDep AS [A/D]",
            "currentSSRCode AS [SSR Code]",
            "startService_DateTime_Local AS [Job Start Time]",
            "finishService_DateTime_Local AS [Job End Time]",
            "ActualDateTime_Local AS [Actual Date Time]",
            "ScheduledDateTime_Local AS [Scheduled DateTime]",
            "disregardCode AS [Disregard Code]",
            "EmployeeName AS [Employee]",
            "VehicleShortName AS [Vehicle Model]",
            "VehicleTypeName AS [Vehicle Type]",
            "StandCode AS [Stand]",
        ],
        where=[
            "BillingPRM = 1",
            "Operation_DateID_Local >= :start_op",
            "Operation_DateID_Local < :end_op",
        ],
        params={
            "start_op": start_op,
            "end_op": end_op,
        },
        query_option="OPTION (RECOMPILE)",
    )

    df = to_datetime(
        df,
        [
            "Job Start Time",
            "Job End Time",
            "Actual Date Time",
            "Scheduled DateTime",
        ],
    )

    df = add_month_fields(df, date_col="Scheduled DateTime", month_col="Year Month")

    df["Flight Date"] = pd.to_datetime(df["Scheduled DateTime"], errors="coerce").dt.date

    df["Flight Number"] = (
        df["Flight Number"]
        .astype(str)
        .str.strip()
        .str.lstrip("0")
    )

    df["Stand"] = df["Stand"].map(normalise_stand)
    df["SSR Code"] = df["SSR Code"].map(normalise_text)
    df["Vehicle Type"] = df["Vehicle Type"].astype(str).str.strip()

    return df


def load_flight_data(start, end):
    df = query(
        table="EAL.FlightPerformance",
        columns=[
            "FlightID AS [Flight ID]",
            "ScheduledDateTime_Local AS [Scheduled DateTime]",
            "ActualDateTime_Local AS [Actual Date Time]",
            "FlightNumber AS [Flight Number]",
            "AirlineCode_IATA AS [Airline Code]",
            "ArrDeptureCode AS [A/D]",
            "Sector",
            "StandCode AS [Flight Stand]",
            "DepartureGate AS [Departure Gate]",
            "RemoteStand AS [Remote Stand]",
            "IsPassengerFlight",
            "Passengers AS [Passengers]",
        ],
        where=[
            "ScheduledDateTime_Local >= :start",
            "ScheduledDateTime_Local < :end",
        ],
        params={
            "start": start,
            "end": end,
        },
        query_option="OPTION (RECOMPILE)",
    )

    df = to_datetime(
        df,
        [
            "Scheduled DateTime",
            "Actual Date Time",
        ],
    )

    df = add_month_fields(df, date_col="Scheduled DateTime", month_col="Year Month")

    df["Flight Date"] = pd.to_datetime(df["Scheduled DateTime"], errors="coerce").dt.date

    df["Flight Number"] = (
        df["Flight Number"]
        .astype(str)
        .str.strip()
        .str.lstrip("0")
    )

    df["Flight Stand"] = df["Flight Stand"].map(normalise_stand)

    return df


# =====================================================================
# Preparation / merge
# =====================================================================

def prepare_prm_job_level_dataset(prm_df, flights_df, passenger_flights_only=True):
    prm = prm_df.copy()
    flt = flights_df.copy()

    flight_cols = [
        "Flight ID",
        "Flight Date",
        "Sector",
        "Flight Stand",
        "Departure Gate",
        "Remote Stand",
        "IsPassengerFlight",
        "Passengers",
    ]

    flight_cols = [c for c in flight_cols if c in flt.columns]

    flt = flt[flight_cols].drop_duplicates(["Flight ID", "Flight Date"])

    merged = prm.merge(
        flt,
        on=["Flight ID", "Flight Date"],
        how="left",
    )

    if "Flight Stand" in merged.columns:
        merged["Stand"] = merged["Stand"].replace("", np.nan)
        merged["Stand"] = merged["Stand"].fillna(merged["Flight Stand"])
        merged["Stand"] = merged["Stand"].map(normalise_stand)

    if passenger_flights_only and "IsPassengerFlight" in merged.columns:
        keep_mask = (
            merged["IsPassengerFlight"].isna()
            | merged["IsPassengerFlight"].eq(1)
            | merged["IsPassengerFlight"].eq(True)
        )
        merged = merged.loc[keep_mask].copy()

    return merged


# =====================================================================
# Metric 1: Monthly Ambulift passengers by stand
# =====================================================================

def monthly_ambulift_by_stand_long(
    prm_jobs,
    passenger_col="Passenger ID",
    stand_col="Stand",
    vehicle_col="Vehicle Type",
    month_col="Year Month",
    ambulift_label=AMBULIFT_LABEL,
):
    x = prm_jobs.copy()

    required_cols = [passenger_col, stand_col, vehicle_col, month_col]
    missing = [c for c in required_cols if c not in x.columns]

    if missing:
        raise KeyError("Missing required columns for Ambulift metric: {}".format(missing))

    x["Vehicle Type Clean"] = x[vehicle_col].astype(str).str.strip().str.upper()
    ambulift_clean = ambulift_label.strip().upper()

    x["Stand"] = x[stand_col].map(normalise_stand)

    x = x.loc[x["Vehicle Type Clean"].eq(ambulift_clean)].copy()

    x = x.dropna(subset=[passenger_col])
    x = x.loc[x["Stand"].ne("")].copy()

    dedup = x.drop_duplicates(["Stand", month_col, passenger_col]).copy()
    dedup["Unique Ambulift Passenger"] = 1

    out = (
        dedup
        .groupby(["Stand", month_col], dropna=False)["Unique Ambulift Passenger"]
        .sum()
        .reset_index(name="Ambulift Jobs")
        .sort_values(["Stand", month_col])
    )

    return out


def monthly_ambulift_by_stand_pivot(prm_jobs):
    long_df = monthly_ambulift_by_stand_long(prm_jobs)

    pivot_df = month_pivot(
        df=long_df,
        index_cols=["Stand"],
        month_col="Year Month",
        value_col="Ambulift Jobs",
        fill_value=0,
        add_total_col=True,
    )

    return pivot_df


# =====================================================================
# Metric 2: Monthly WCHC/WCHS passengers by stand group
# =====================================================================

def monthly_wchc_wchs_by_stand_group_long(
    prm_jobs,
    passenger_col="Passenger ID",
    stand_col="Stand",
    ssr_col="SSR Code",
    month_col="Year Month",
    ssr_codes=WHEELCHAIR_SSR_CODES,
    focus_stands=FOCUS_STANDS,
    include_ssr_breakdown=False,
):
    x = prm_jobs.copy()

    required_cols = [passenger_col, stand_col, ssr_col, month_col]
    missing = [c for c in required_cols if c not in x.columns]

    if missing:
        raise KeyError("Missing required columns for WCHC/WCHS metric: {}".format(missing))

    ssr_set = set([normalise_text(s) for s in ssr_codes])
    focus_set = set([normalise_stand(s) for s in focus_stands])

    x["SSR Code Clean"] = x[ssr_col].map(normalise_text)
    x["Stand"] = x[stand_col].map(normalise_stand)

    ssr_mask = x["SSR Code Clean"].isin(ssr_set)
    stand_mask = x["Stand"].ne("")
    x = x.loc[ssr_mask & stand_mask].copy()

    x["Stand Group"] = "Excluding stands 8/9/10"
    x.loc[x["Stand"].isin(focus_set), "Stand Group"] = "Stands 8/9/10 only"

    if include_ssr_breakdown:
        dedup_cols = [month_col, "Stand Group", "SSR Code Clean", passenger_col]
        group_cols = ["Stand Group", "SSR Code Clean", month_col]
    else:
        dedup_cols = [month_col, "Stand Group", passenger_col]
        group_cols = ["Stand Group", month_col]

    dedup = x.drop_duplicates(dedup_cols).copy()
    dedup["Unique Passenger"] = 1

    out = (
        dedup
        .groupby(group_cols, dropna=False)["Unique Passenger"]
        .sum()
        .reset_index(name="Passengers")
    )

    if include_ssr_breakdown:
        out = out.rename(columns={"SSR Code Clean": "SSR Code"})
        out = out.sort_values(["Stand Group", "SSR Code", month_col])
    else:
        out = out.sort_values(["Stand Group", month_col])

    return out


def monthly_wchc_wchs_by_stand_group_pivot(prm_jobs, include_ssr_breakdown=False):
    long_df = monthly_wchc_wchs_by_stand_group_long(
        prm_jobs=prm_jobs,
        include_ssr_breakdown=include_ssr_breakdown,
    )

    if include_ssr_breakdown:
        index_cols = ["Stand Group", "SSR Code"]
    else:
        index_cols = ["Stand Group"]

    pivot_df = month_pivot(
        df=long_df,
        index_cols=index_cols,
        month_col="Year Month",
        value_col="Passengers",
        fill_value=0,
        add_total_col=True,
    )

    return pivot_df

# =====================================================================
# Metric 3: WCHC/WCHS passengers on stands 8/9/10 by Ambulift usage
# =====================================================================

def wchc_wchs_focus_stands_ambulift_usage_passenger_detail(
    prm_jobs,
    passenger_col="Passenger ID",
    flight_col="Flight ID",
    flight_date_col="Flight Date",
    stand_col="Stand",
    ssr_col="SSR Code",
    vehicle_col="Vehicle Type",
    month_col="Year Month",
    ssr_codes=WHEELCHAIR_SSR_CODES,
    focus_stands=FOCUS_STANDS,
    ambulift_label=AMBULIFT_LABEL,
):
    """
    Detailed passenger-level QA output.

    One row = one unique Passenger ID.

    Logic:
    - Passenger ID is treated as the unique passenger journey identifier.
    - First, identify Passenger IDs where:
        SSR Code is WCHC/WCHS
        and Stand is one of 8/9/10.
    - Then look across all job rows for those Passenger IDs to check whether
      any associated job row has Vehicle Type = Ambulift.

    This avoids double counting because each Passenger ID may have multiple
    job lines across the journey, including duplicate vehicle/staff rows.
    """

    x = prm_jobs.copy()

    required_cols = [
        passenger_col,
        stand_col,
        ssr_col,
        vehicle_col,
        month_col,
    ]

    missing = [c for c in required_cols if c not in x.columns]

    if missing:
        raise KeyError(
            "Missing required columns for WCHC/WCHS focus stand Ambulift usage detail: {}".format(
                missing
            )
        )

    ssr_set = set([normalise_text(s) for s in ssr_codes])
    focus_set = set([normalise_stand(s) for s in focus_stands])
    ambulift_clean = ambulift_label.strip().upper()

    x["SSR Code Clean"] = x[ssr_col].map(normalise_text)
    x["Stand Clean"] = x[stand_col].map(normalise_stand)
    x["Vehicle Type Clean"] = x[vehicle_col].astype(str).str.strip().str.upper()

    x = x.dropna(subset=[passenger_col]).copy()

    qualifying_rows = x.loc[
        x["SSR Code Clean"].isin(ssr_set)
        & x["Stand Clean"].isin(focus_set)
    ].copy()

    if qualifying_rows.empty:
        return pd.DataFrame(
            columns=[
                "Passenger ID",
                "Year Month",
                "Flight ID",
                "Flight Date",
                "Airline Code",
                "Flight Number",
                "A/D",
                "Scheduled DateTime",
                "Focus Stand List",
                "SSR Code List",
                "Vehicle Type List",
                "Raw Job Rows",
                "Ambulift Job Rows",
                "Ambulift Usage",
            ]
        )

    qualifying_passengers = qualifying_rows[passenger_col].dropna().unique()

    passenger_rows = x.loc[x[passenger_col].isin(qualifying_passengers)].copy()

    passenger_rows["Is Ambulift Job"] = passenger_rows["Vehicle Type Clean"].eq(
        ambulift_clean
    )

    qualifying_summary = (
        qualifying_rows
        .groupby(passenger_col, dropna=False)
        .agg(
            Year_Month=(month_col, "first"),
            Focus_Stand_List=(
                "Stand Clean",
                lambda s: ", ".join(sorted(set([v for v in s if v != ""]))),
            ),
            SSR_Code_List=(
                "SSR Code Clean",
                lambda s: ", ".join(sorted(set([v for v in s if v != ""]))),
            ),
        )
        .reset_index()
    )

    optional_first_cols = [
        flight_col,
        flight_date_col,
        "Airline Code",
        "Flight Number",
        "A/D",
        "Scheduled DateTime",
    ]

    agg_dict = {
        "Vehicle Type List": (
            "Vehicle Type Clean",
            lambda s: ", ".join(sorted(set([v for v in s if v != ""]))),
        ),
        "Raw Job Rows": (passenger_col, "size"),
        "Ambulift Job Rows": ("Is Ambulift Job", "sum"),
        "Has Ambulift Job": ("Is Ambulift Job", "max"),
    }

    for col in optional_first_cols:
        if col in passenger_rows.columns:
            agg_dict[col] = (col, "first")

    passenger_summary = (
        passenger_rows
        .groupby(passenger_col, dropna=False)
        .agg(**agg_dict)
        .reset_index()
    )

    detail = qualifying_summary.merge(
        passenger_summary,
        on=passenger_col,
        how="left",
    )

    detail["Ambulift Usage"] = np.where(
        detail["Has Ambulift Job"],
        "Had Ambulift job",
        "No Ambulift job / assumed Jetbridge",
    )

    detail = detail.drop(columns=["Has Ambulift Job"])

    detail = detail.rename(
        columns={
            passenger_col: "Passenger ID",
            "Year_Month": "Year Month",
            "Focus_Stand_List": "Focus Stand List",
            "SSR_Code_List": "SSR Code List",
            flight_col: "Flight ID",
            flight_date_col: "Flight Date",
        }
    )

    preferred_cols = [
        "Passenger ID",
        "Year Month",
        "Flight ID",
        "Flight Date",
        "Airline Code",
        "Flight Number",
        "A/D",
        "Scheduled DateTime",
        "Focus Stand List",
        "SSR Code List",
        "Vehicle Type List",
        "Raw Job Rows",
        "Ambulift Job Rows",
        "Ambulift Usage",
    ]

    preferred_cols = [c for c in preferred_cols if c in detail.columns]

    detail = detail[preferred_cols]

    sort_cols = [
        c for c in [
            "Year Month",
            "Ambulift Usage",
            "Flight Date",
            "Flight ID",
            "Passenger ID",
        ]
        if c in detail.columns
    ]

    if sort_cols:
        detail = detail.sort_values(sort_cols)

    return detail


def monthly_wchc_wchs_focus_stands_ambulift_usage_long(prm_jobs):
    """
    Monthly passenger-level summary.

    Counts each Passenger ID once.
    """

    detail = wchc_wchs_focus_stands_ambulift_usage_passenger_detail(prm_jobs)

    if detail.empty:
        return pd.DataFrame(
            columns=[
                "Year Month",
                "Ambulift Usage",
                "Passengers",
            ]
        )

    out = (
        detail
        .groupby(["Year Month", "Ambulift Usage"], dropna=False)
        .size()
        .reset_index(name="Passengers")
        .sort_values(["Year Month", "Ambulift Usage"])
    )

    return out


def monthly_wchc_wchs_focus_stands_ambulift_usage_pivot(prm_jobs):
    long_df = monthly_wchc_wchs_focus_stands_ambulift_usage_long(prm_jobs)

    pivot_df = month_pivot(
        df=long_df,
        index_cols=["Ambulift Usage"],
        month_col="Year Month",
        value_col="Passengers",
        fill_value=0,
        add_total_col=True,
    )

    return pivot_df


# =====================================================================
# Additional QA outputs
# =====================================================================

def vehicle_type_summary(prm_jobs):
    if "Vehicle Type" not in prm_jobs.columns:
        return pd.DataFrame(columns=["Vehicle Type", "Raw Job Rows"])

    x = prm_jobs.copy()
    x["Vehicle Type"] = x["Vehicle Type"].astype(str).str.strip()

    out = (
        x
        .groupby("Vehicle Type", dropna=False)
        .size()
        .reset_index(name="Raw Job Rows")
        .sort_values("Raw Job Rows", ascending=False)
    )

    return out


def ssr_summary(prm_jobs):
    if "SSR Code" not in prm_jobs.columns:
        return pd.DataFrame(columns=["SSR Code", "Raw Job Rows", "Unique Passengers"])

    x = prm_jobs.copy()
    x["SSR Code"] = x["SSR Code"].map(normalise_text)

    raw_rows = (
        x
        .groupby("SSR Code", dropna=False)
        .size()
        .reset_index(name="Raw Job Rows")
    )

    unique_pax = (
        x
        .groupby("SSR Code", dropna=False)["Passenger ID"]
        .nunique()
        .reset_index(name="Unique Passengers")
    )

    out = raw_rows.merge(unique_pax, on="SSR Code", how="left")
    out = out.sort_values("Raw Job Rows", ascending=False)

    return out


def stand_summary(prm_jobs):
    if "Stand" not in prm_jobs.columns:
        return pd.DataFrame(columns=["Stand", "Raw Job Rows", "Unique Passengers"])

    x = prm_jobs.copy()
    x["Stand"] = x["Stand"].map(normalise_stand)

    raw_rows = (
        x
        .groupby("Stand", dropna=False)
        .size()
        .reset_index(name="Raw Job Rows")
    )

    unique_pax = (
        x
        .groupby("Stand", dropna=False)["Passenger ID"]
        .nunique()
        .reset_index(name="Unique Passengers")
    )

    out = raw_rows.merge(unique_pax, on="Stand", how="left")
    out = out.sort_values("Raw Job Rows", ascending=False)

    return out


def build_data_quality_summary(prm_raw, flights_raw, prm_jobs):
    rows = []

    rows.append({
        "Check": "Raw PRM job rows loaded",
        "Value": len(prm_raw),
    })

    rows.append({
        "Check": "Flight rows loaded",
        "Value": len(flights_raw),
    })

    rows.append({
        "Check": "PRM job rows after merge/filter",
        "Value": len(prm_jobs),
    })

    if "Passenger ID" in prm_raw.columns:
        raw_unique_pax = prm_raw["Passenger ID"].nunique()
    else:
        raw_unique_pax = np.nan

    rows.append({
        "Check": "Unique PRM passengers in raw PRM data",
        "Value": raw_unique_pax,
    })

    if "Passenger ID" in prm_jobs.columns:
        merged_unique_pax = prm_jobs["Passenger ID"].nunique()
    else:
        merged_unique_pax = np.nan

    rows.append({
        "Check": "Unique PRM passengers after merge/filter",
        "Value": merged_unique_pax,
    })

    if "Flight ID" in prm_raw.columns:
        raw_unique_flights = prm_raw["Flight ID"].nunique()
    else:
        raw_unique_flights = np.nan

    rows.append({
        "Check": "Unique Flight IDs in raw PRM data",
        "Value": raw_unique_flights,
    })

    if "Flight ID" in prm_jobs.columns:
        merged_unique_flights = prm_jobs["Flight ID"].nunique()
    else:
        merged_unique_flights = np.nan

    rows.append({
        "Check": "Unique Flight IDs after merge/filter",
        "Value": merged_unique_flights,
    })

    if "Passenger ID" in prm_jobs.columns:
        missing_passenger_id = int(prm_jobs["Passenger ID"].isna().sum())
    else:
        missing_passenger_id = np.nan

    rows.append({
        "Check": "Rows missing Passenger ID after merge/filter",
        "Value": missing_passenger_id,
    })

    if "Stand" in prm_jobs.columns:
        missing_stand = int(prm_jobs["Stand"].map(normalise_stand).eq("").sum())
    else:
        missing_stand = np.nan

    rows.append({
        "Check": "Rows missing Stand after merge/filter",
        "Value": missing_stand,
    })

    if "Vehicle Type" in prm_jobs.columns:
        raw_ambulift_rows = int(
            prm_jobs["Vehicle Type"]
            .astype(str)
            .str.strip()
            .str.upper()
            .eq(AMBULIFT_LABEL.upper())
            .sum()
        )
    else:
        raw_ambulift_rows = np.nan

    rows.append({
        "Check": "Raw Ambulift job rows after merge/filter",
        "Value": raw_ambulift_rows,
    })

    if "SSR Code" in prm_jobs.columns:
        raw_wchc_wchs_rows = int(
            prm_jobs["SSR Code"]
            .map(normalise_text)
            .isin(set(WHEELCHAIR_SSR_CODES))
            .sum()
        )
    else:
        raw_wchc_wchs_rows = np.nan

    rows.append({
        "Check": "Raw WCHC/WCHS job rows after merge/filter",
        "Value": raw_wchc_wchs_rows,
    })

    return pd.DataFrame(rows)


def build_run_parameters(start, end, excel_out, csv_dir, passenger_flights_only):
    rows = [
        {
            "Parameter": "Start date inclusive",
            "Value": start,
        },
        {
            "Parameter": "End date exclusive",
            "Value": end,
        },
        {
            "Parameter": "Month field used",
            "Value": "Scheduled DateTime converted to YYYY-MM",
        },
        {
            "Parameter": "Ambulift metric",
            "Value": "Vehicle Type = Ambulift; deduplicated by Stand + Year Month + Passenger ID",
        },
        {
            "Parameter": "WCHC/WCHS metric",
            "Value": "SSR Code in WCHC/WCHS; deduplicated by Year Month + Stand Group + Passenger ID",
        },
        {
            "Parameter": "WCHC/WCHS focus stand Ambulift usage metric",
            "Value": (
                "For WCHC/WCHS passengers on stands 8/9/10, Passenger ID is treated as "
                "the unique passenger journey identifier. Qualifying Passenger IDs are "
                "identified using SSR Code in WCHC/WCHS and Stand in 8/9/10. All associated "
                "job rows for those Passenger IDs are then checked. If any associated row "
                "has Vehicle Type = Ambulift, the passenger is classified as Had Ambulift job; "
                "otherwise No Ambulift job / assumed Jetbridge."
            ),
        },
        {
            "Parameter": "Focus stands",
            "Value": ", ".join(FOCUS_STANDS),
        },
        {
            "Parameter": "Other stand group",
            "Value": "All stands excluding 8, 9 and 10",
        },
        {
            "Parameter": "Passenger flights only",
            "Value": passenger_flights_only,
        },
        {
            "Parameter": "Excel output",
            "Value": excel_out,
        },
        {
            "Parameter": "CSV output directory",
            "Value": csv_dir if csv_dir else "",
        },
    ]

    return pd.DataFrame(rows)


# =====================================================================
# Main report builder
# =====================================================================

def build_monthly_prm_stand_report(
    start,
    end,
    excel_out,
    csv_dir=None,
    passenger_flights_only=True,
):
    total_t0 = time.perf_counter()

    print_section("MONTHLY PRM STAND REPORT")
    print("Start date inclusive: {}".format(start))
    print("End date exclusive:   {}".format(end))
    print("Excel output:         {}".format(excel_out))
    print("CSV output folder:    {}".format(csv_dir if csv_dir else "Not requested"))
    print("Passenger flights only after merge: {}".format(passenger_flights_only))

    print("")
    print("[1/9] Loading raw PRM job-level data...")
    t0 = time.perf_counter()
    prm_raw = load_prm_data(start=start, end=end)
    t1 = step(t0, "Loaded raw PRM job rows: {:,}".format(len(prm_raw)))

    print("")
    print("[2/9] Loading flight data...")
    flights_raw = load_flight_data(start=start, end=end)
    t2 = step(t1, "Loaded flight rows: {:,}".format(len(flights_raw)))

    print("")
    print("[3/9] Merging flight fields onto PRM job-level data...")
    prm_jobs = prepare_prm_job_level_dataset(
        prm_df=prm_raw,
        flights_df=flights_raw,
        passenger_flights_only=passenger_flights_only,
    )
    t3 = step(t2, "Merged PRM job rows ready: {:,}".format(len(prm_jobs)))

    print("")
    print("[4/9] Building monthly Ambulift unique passenger count by stand...")
    ambulift_by_stand_long_df = monthly_ambulift_by_stand_long(prm_jobs)
    ambulift_by_stand_pivot_df = monthly_ambulift_by_stand_pivot(prm_jobs)
    t4 = step(t3, "Monthly Ambulift by stand complete.")

    print("")
    print("[5/9] Building monthly WCHC/WCHS passenger count by stand group...")
    wchc_wchs_group_long_df = monthly_wchc_wchs_by_stand_group_long(
        prm_jobs=prm_jobs,
        include_ssr_breakdown=False,
    )

    wchc_wchs_group_pivot_df = monthly_wchc_wchs_by_stand_group_pivot(
        prm_jobs=prm_jobs,
        include_ssr_breakdown=False,
    )

    t5 = step(t4, "Monthly WCHC/WCHS by stand group complete.")

    print("")
    print("[6/9] Building monthly WCHC/WCHS by stand group and SSR breakdown...")
    wchc_wchs_by_ssr_long_df = monthly_wchc_wchs_by_stand_group_long(
        prm_jobs=prm_jobs,
        include_ssr_breakdown=True,
    )

    wchc_wchs_by_ssr_pivot_df = monthly_wchc_wchs_by_stand_group_pivot(
        prm_jobs=prm_jobs,
        include_ssr_breakdown=True,
    )

    t6 = step(t5, "Monthly WCHC/WCHS by stand group and SSR complete.")

    print("")
    print("[7/9] Building WCHC/WCHS stands 8/9/10 Ambulift vs assumed Jetbridge metric...")

    wchc_wchs_focus_ambulift_usage_long_df = (
        monthly_wchc_wchs_focus_stands_ambulift_usage_long(prm_jobs)
    )

    wchc_wchs_focus_ambulift_usage_pivot_df = (
        monthly_wchc_wchs_focus_stands_ambulift_usage_pivot(prm_jobs)
    )

    wchc_wchs_focus_ambulift_usage_detail_df = (
        wchc_wchs_focus_stands_ambulift_usage_passenger_detail(prm_jobs)
    )

    t7 = step(
        t6,
        "WCHC/WCHS stands 8/9/10 Ambulift vs assumed Jetbridge metric complete.",
    )

    print("")
    print("[8/9] Building QA and supporting outputs...")

    qa_summary_df = build_data_quality_summary(
        prm_raw=prm_raw,
        flights_raw=flights_raw,
        prm_jobs=prm_jobs,
    )

    run_parameters_df = build_run_parameters(
        start=start,
        end=end,
        excel_out=excel_out,
        csv_dir=csv_dir,
        passenger_flights_only=passenger_flights_only,
    )

    vehicle_summary_df = vehicle_type_summary(prm_jobs)
    ssr_summary_df = ssr_summary(prm_jobs)
    stand_summary_df = stand_summary(prm_jobs)

    t8 = step(t7, "QA and supporting outputs complete.")

    print("")
    print("[9/9] Saving outputs...")

    outputs = {
        "Run_Parameters": run_parameters_df,
        "QA_Summary": qa_summary_df,

        "Ambulift_By_Stand": ambulift_by_stand_pivot_df,
        "WCHC_WCHS_Stand_Group": wchc_wchs_group_pivot_df,

        "Ambulift_By_Stand_Long": ambulift_by_stand_long_df,
        "WCHC_WCHS_Group_Long": wchc_wchs_group_long_df,

        "WCHC_WCHS_By_SSR": wchc_wchs_by_ssr_pivot_df,
        "WCHC_WCHS_By_SSR_Long": wchc_wchs_by_ssr_long_df,

        "Focus_Stands_Amb_Use": wchc_wchs_focus_ambulift_usage_pivot_df,
        "Focus_Stands_Amb_Use_Long": wchc_wchs_focus_ambulift_usage_long_df,
        "Focus_Stands_Amb_Use_Detail": wchc_wchs_focus_ambulift_usage_detail_df,

        "Vehicle_Type_Summary": vehicle_summary_df,
        "SSR_Summary": ssr_summary_df,
        "Stand_Summary": stand_summary_df,
    }

    save_outputs(
        outputs=outputs,
        excel_out=excel_out,
        csv_dir=csv_dir,
    )

    t9 = step(t8, "Outputs saved.")

    print_preview("RUN PARAMETERS", run_parameters_df)
    print_preview("QA SUMMARY", qa_summary_df)

    print_preview(
        "MONTHLY AMBULIFT UNIQUE PASSENGERS BY STAND",
        ambulift_by_stand_pivot_df,
        rows=30,
    )

    print_preview(
        "MONTHLY WCHC/WCHS PASSENGERS BY STAND GROUP",
        wchc_wchs_group_pivot_df,
        rows=20,
    )

    print_preview(
        "MONTHLY WCHC/WCHS PASSENGERS BY STAND GROUP AND SSR",
        wchc_wchs_by_ssr_pivot_df,
        rows=20,
    )

    print_preview(
        "MONTHLY WCHC/WCHS STANDS 8/9/10 - AMBULIFT VS ASSUMED JETBRIDGE",
        wchc_wchs_focus_ambulift_usage_pivot_df,
        rows=20,
    )

    print_preview(
        "WCHC/WCHS STANDS 8/9/10 - PASSENGER DETAIL",
        wchc_wchs_focus_ambulift_usage_detail_df,
        rows=30,
    )

    total_elapsed = time.perf_counter() - total_t0

    print_section("REPORT COMPLETE")
    print("Workbook saved to: {}".format(excel_out))

    if csv_dir:
        print("CSV outputs saved to: {}".format(csv_dir))

    print("Total elapsed seconds: {:,.2f}".format(total_elapsed))

    return outputs


# =====================================================================
# CLI entry point
# =====================================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Monthly PRM stand report. "
            "Outputs Ambulift by stand and WCHC/WCHS by stand group."
        )
    )

    parser.add_argument(
        "--start",
        required=False,
        default="2025-01-01",
        help="Start date inclusive, YYYY-MM-DD. Default: 2025-01-01",
    )

    parser.add_argument(
        "--end",
        required=False,
        default="2027-01-01",
        help="End date exclusive, YYYY-MM-DD. Default: 2027-01-01",
    )

    parser.add_argument(
        "--out",
        required=False,
        default="outputs/monthly_prm_stand_report.xlsx",
        help="Output Excel path. Default: outputs/monthly_prm_stand_report.xlsx",
    )

    parser.add_argument(
        "--csv-dir",
        required=False,
        default="outputs/monthly_prm_stand_report_csv",
        help=(
            "CSV output directory. "
            "Default: outputs/monthly_prm_stand_report_csv. "
            "Use --csv-dir '' to skip CSV output."
        ),
    )

    parser.add_argument(
        "--include-non-passenger-flights",
        action="store_true",
        help=(
            "If supplied, the report will not filter to passenger flights only "
            "after merging with flight data."
        ),
    )

    args = parser.parse_args()

    csv_dir = args.csv_dir
    if csv_dir == "":
        csv_dir = None

    build_monthly_prm_stand_report(
        start=args.start,
        end=args.end,
        excel_out=args.out,
        csv_dir=csv_dir,
        passenger_flights_only=not args.include_non_passenger_flights,
    )


if __name__ == "__main__":
    main()