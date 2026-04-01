
import pandas as pd
from modules.domain.prm.efficiency import get_prm_count_per_flight, get_employee_count_per_flight, get_wch_counts_per_flight, get_disregard_counts_per_flight, get_vehicle_count
from modules.analytics.grouping import group_unique


def prepare_prm_flight_summary(prm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert PRM job-level dataset into a flight-level PRM summary.
    Includes:
        • PRM Count
        • Employee Count
        • WCHC Count / WCHS Count
        • Disregard Codes
        • Vehicle counts per flight (Ambulift, Minibus, No Vehicle, Total)
        • PassengerType counts per flight (Ambulift Only, Mini Bus Only, Both, No Vehicle)
    
    Important: passenger_level_flags(prm_df) must be applied BEFORE this function, so that PassengerType exists in prm_df
        
    """

    df = prm_df.copy()

    flight_cols = ["Airline Code", "Flight Number", "Day"]


    prm_counts = get_prm_count_per_flight(df, flight_cols)
    emp_counts = get_employee_count_per_flight(df, flight_cols)
    wch_counts = get_wch_counts_per_flight(df, flight_cols)
    disregard = get_disregard_counts_per_flight(df, flight_cols)
    vc_long = get_vehicle_count(df, flight_cols)
    # vc_long = 
    #   Airline | Flight Number | Day | Vehicle Type | Vehicle Count

    # Pivot to wide format: one row per flight
    vc_pivot = (
        vc_long
        .pivot_table(
            index=flight_cols,
            columns="Vehicle Type",
            values="Vehicle Count",
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    # Add total vehicles per flight
    vc_pivot["Total Vehicle Count"] = (
        vc_pivot.get("Ambulift", 0)
        + vc_pivot.get("Mini Bus", 0)
        + vc_pivot.get("No Vehicle", 0)
    )

    
    # --- 3. PassengerType PRM breakdown (if exists) ---
    
    if "PassengerType" in df.columns:
        pax_type_long = group_unique(
            df,
            by_cols=flight_cols + ["PassengerType"],
            id_col="Passenger ID"
        ).rename(columns={"Unique Count": "PassengerType Count"})

        pax_type_pivot = (
            pax_type_long
            .pivot_table(
                index=flight_cols,
                columns="PassengerType",
                values="PassengerType Count",
                aggfunc="sum",
                fill_value=0
            )
            .reset_index()
        )

        # Ensure all expected PassengerType columns exist
        expected_cols = ["Ambulift Only", "Mini Bus Only", "Both", "No Vehicle"]
        for col in expected_cols:
            if col not in pax_type_pivot.columns:
                pax_type_pivot[col] = 0

    else:
        pax_type_pivot = None


    # --- Merge all summaries ---
    summary = (
        prm_counts
        .merge(emp_counts, on=flight_cols, how="left")
        .merge(wch_counts, on=flight_cols, how="left")
        .merge(disregard, on=flight_cols, how="left")
        .merge(vc_pivot, on=flight_cols, how="left")
    )

    if pax_type_pivot is not None:
        summary = summary.merge(pax_type_pivot, on=flight_cols, how="left")

    return summary



def merge_prm_and_flights(prm_summary: pd.DataFrame,
                          flight_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge the flight-level PRM summary with the flight schedule.

    Both inputs must already be flight-level (1 row per flight):
      • prm_summary from prepare_prm_flight_summary()
      • flight_df from load_flight_data()

    Merge Keys:
      Airline Code
      Flight Number (zero-stripped)
      Day (ScheduledDateTime.date)
    """

    prm = prm_summary.copy()
    fl = flight_df.copy()

    # Clean merge fields
    prm["Flight Number"] = prm["Flight Number"].astype(str).str.lstrip("0")
    fl["Flight Number"] = fl["Flight Number"].astype(str).str.lstrip("0")

    # Ensure Day exists in flight schedule
    if "Day" not in fl.columns:
        fl["Day"] = pd.to_datetime(fl["Scheduled DateTime"]).dt.date

    merge_keys = ["Airline Code", "Flight Number", "Day"]

    merged = prm.merge(
        fl,
        on=merge_keys,
        how="left",
        suffixes=("", "_flight")
    )

    return merged



def challenge_summary(df_merged: pd.DataFrame,
                      forecast_df: pd.DataFrame | None,
                      challenge_mask: pd.Series) -> dict:
    """
    Summarise a challenge using FLIGHT-LEVEL merged data.

    If forecast_df is provided and not empty → include forecast metrics.
    If forecast_df is None or empty → only return historical metrics.

    Parameters
    ----------
    df_merged : flight-level PRM+flight merged DataFrame
    forecast_df : forecast flight DataFrame or None
    challenge_mask : boolean mask applied to df_merged

    Returns
    -------
    dict : challenge summary
    """

    
    affected = df_merged.loc[challenge_mask]

    # ------------------------------------------------------------------
    # Flight counts
    # ------------------------------------------------------------------
    total_hist = len(df_merged)
    affected_hist = len(affected)
    pct_hist = affected_hist / total_hist if total_hist else 0.0

    # ------------------------------------------------------------------
    # Historical averages
    # ------------------------------------------------------------------
    avg_prm = affected["PRM Count"].mean()
    avg_staff = affected["Employee Count"].mean()
    avg_wchc = affected["WCHC Count"].mean()
    avg_wchs = affected["WCHS Count"].mean()
    avg_wchr = affected["WCHR Count"].mean()

    # ------------------------------------------------------------------
    # PassengerType breakdown (columns, not rows)
    # ------------------------------------------------------------------
    pax_cols = [
        c for c in ["Ambulift Only", "Mini Bus Only", "Both", "No Vehicle"]
        if c in df_merged.columns
    ]

    if pax_cols:
        pax_breakdown = (
            affected[pax_cols]
            .mean()
            .round(2)
            .to_dict()
        )
    else:
        pax_breakdown = {}

    # ------------------------------------------------------------------
    # Base (historic) summary
    # ------------------------------------------------------------------
    summary = {
        "Historical Flights": int(total_hist),
        "Affected Flights": int(affected_hist),
        "Affected %": round(pct_hist * 100, 2),
        "Avg PRMs per affected flight": round(avg_prm, 2),
        "Avg Employees per affected flight": round(avg_staff, 2),
        "Avg WCHC per flight": round(avg_wchc, 2),
        "Avg WCHS per flight": round(avg_wchs, 2),
        "Avg WCHR per flight": round(avg_wchr, 2),
        "PassengerType Breakdown": pax_breakdown,
    }

    # ------------------------------------------------------------------
    # Forecast metrics (optional)
    # ------------------------------------------------------------------
    if forecast_df is not None and len(forecast_df) > 0:
        total_forecast = len(forecast_df)
        expected_forecast = pct_hist * total_forecast

        summary.update({
            "Forecast Flights": int(total_forecast),
            "Forecast Expected Affected %": round(pct_hist * 100, 2),
            "Forecast Expected Affected Flights": int(round(expected_forecast)),
        })

    return summary

