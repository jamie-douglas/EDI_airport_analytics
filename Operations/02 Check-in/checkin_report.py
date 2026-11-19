#scripts/checkin_report.py
import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))


import argparse
import time
import pandas as pd

from modules.utils.query import query
from modules.utils.excel import write_once_then_update
from modules.utils.progress import step
from modules.domain.checkin import compute_peak_rolling_hour, compute_durations, compute_penetration, compute_apr_sept_sidecheck

# ====================================================================================
# DATA LOADING
# ====================================================================================

def load_cupps(start: str, end: str) -> pd.DataFrame:
    """
    Loads CUPPs (desk) check-in transactions for the specified period. 

    Parameters
    ----------
    start: str
        Start date (inclusive) in 'YYYY-MM-DD' format
    end: str
        End date (exclusive) in 'YYYY-MM-DD' format
    
    
    Returns
    ----------
    pandas.DataFrame
        DataFrame containing CUPPs check-in transactions with columns:
        - 'Flight ID
        - 'Airline IATA Code'
        - 'Flight Number'
        - 'Scheduled DateTime'
        - 'Unique BP Prints'
        - 'Flight Cancelled'
        - 'BT Prints'
        - 'CIP At EGate'
        - 'FirstPNRAction'
        - 'NextPNRAction'
        - 'Buffer'
    """

    
    return query(
        table="Reporting.CUPPS_CheckInHallTransactions_SourceData",
        columns=[
            "FlightID AS [Flight ID]",
            "AirlineCode_IATA AS [Airline IATA Code]",
            "FlightNumber AS [Flight Number]",
            "ScheduledDateTime_Local AS [Scheduled DateTime]",
            "UniqueCheckInBPPrints AS [Unique BP Prints]",
            "FlightCancelled AS [Flight Cancelled]",
            "BagTagPrints AS [BT Prints]",
            "CIPAtEGate AS [CIP At EGate]",
            "FirstPNRAction",
            "NextPNRAction",
            "Buffer_BetweenPax AS [Buffer]"
        ],
        where=["FlightCancelled = 0"],
        date_column="ScheduledDateTime_Local",
        start=start,
        end=end
    )


def load_kiosk(start: str, end: str) -> pd.DataFrame:
    """
    Loads SITA kiosk bag-drop events (error-free) for the specified period.

    Parameters
    ----------
    start: str
        Start date (inclusive) in 'YYYY-MM-DD' format
    end: str
        End date (exclusive) in 'YYYY-MM-DD' format

    Returns
    -------
    pandas.DataFrame
        DataFrame containing kiosk events with columns:
        - 'Flight ID'
        - 'Airline'
        - 'Flight Number'
        - 'Start DateTime'
        - 'End DateTime'
        - 'Process Duration'
        - 'Error Message'
        - 'Transaction Success'
        - 'IsTwilightBag'
        - 'Zone'
    """
    return query(
        table="SITA.v_DropOffEvent",
        columns=[
            "flightID AS [Flight ID]",
            "airline AS [Airline]",
            "flight_number AS [Flight Number]",
            "start_datetime_local AS [Start DateTime]",
            "end_datetime_local AS [End DateTime]",
            "process_duration_seconds AS [Process Duration]",
            "error_message AS [Error Message]",
            "transaction_success AS [Transaction Success]",
            "IsTwilightBag AS [IsTwilightBag]",
            "Zone AS [Zone]"
        ],
        where=["error_message IS NULL"],
        date_column="start_datetime_local",
        end_column="end_datetime_local",
        start=start,
        end=end
        # overlap=False → sessions fully contained in [start, end)
    )



def load_flights_denominator(start: str, end: str) -> pd.DataFrame:
    """
    Loads departure flights for penetration denominator (passenger flights, not canceled).

    Parameters
    ----------
    start: str
        Start date (inclusive) in 'YYYY-MM-DD' format
    end: str
        End date (exclusive) in 'YYYY-MM-DD' format

    Returns
    -------
    pandas.DataFrame
        DataFrame containing flight rows with columns:
        - 'Flight ID'
        - 'Scheduled DateTime'
        - 'A/D'
        - 'Flight Number'
        - 'Airline IATA Code'
        - 'Pax'
        - 'Flight Cancelled'
    """
    return query(
        table="EAL.FlightPerformance",
        columns=[
            "FlightID AS [Flight ID]",
            "ScheduledDateTime_Local AS [Scheduled DateTime]",
            "ArrDeptureCode AS [A/D]",
            "FlightNumber AS [Flight Number]",
            "AirlineCode_IATA AS [Airline IATA Code]",
            "Passengers AS [Pax]",
            "FlightCancelled AS [Flight Cancelled]"
        ],
        where=[
            "ArrDeptureCode = 'D'",
            "FlightCancelled = 0",
            "IsPassengerFlight = 1"
        ],
        date_column="ScheduledDateTime_Local",
        start=start,
        end=end
    )

# ====================================================================================
# CHECK IN REPORT - MAIN
# ====================================================================================

def main(start: str, end: str, excel_out: str | None) -> None:
    """
    Executes the check-in report for the given period.

    Parameters
    ----------
    start : str
        Start datetime (inclusive), ISO format.
    end : str
        End datetime (exclusive), ISO format.
    excel_out : str or None
        Optional path to an Excel workbook to be updated in place.

    Returns
    -------
    None
    """

    print("\nCHECK-IN REPORT")
    print(f"Window: {start} -> {end}\n")

    t0 = time.perf_counter()

    # [1/6] Load CUPPs
    print("[1/6] Loading CUPPs...")
    cupps  = load_cupps(start, end)
    t1 = step(t0, f" Loaded CUPPs ({len(cupps):,} rows)")

    # [2/6] Load kiosk
    print("[2/6] Loading Kiosk...")
    kiosk  = load_kiosk(start, end)
    t2 = step(t1, f" Loaded Kiosk ({len(kiosk):,} rows)")

    # [3/6] Load flights (denominator)
    print("[3/6] Loading flights (denominator)…")
    flights = load_flights_denominator(start, end)
    t3 = step(t2, f" Loaded flights ({len(flights):,} rows)")

    if cupps.empty and kiosk.empty:
        print("No Check-In data for this window")

    # [4/6] Analytics
    print("[4/6] Computing analytics…")
    #Peak rolling hour and window
    roll, peak_val, win_start, win_end = compute_peak_rolling_hour(cupps, kiosk)
    #Average durations
    desk_mean, kiosk_mean = compute_durations(cupps, kiosk)
    #Penetration overll
    rate_all, pen_all = compute_penetration(cupps, kiosk, flights)
    #Penetration Apr-Sep (using start-year)
    year_start = pd.Timestamp(start).year
    rate_apr_sep, pen_apr_sep = compute_apr_sept_sidecheck(cupps, kiosk, flights, year_start)
    t4 = step(t3, "Analytics computed")

    # [5/6]Console summary
    print("[5/6] Console summary...")
    print("\n--- CHECK-IN: PEAK ROLLING HOUR ---")
    print(f"Peak rolling hour pax: {int(peak_val)}")
    print(f"Window: {win_start:%Y-%m-%d %H:%M} → {win_end:%Y-%m-%d %H:%M}")

    print("\n--- DURATIONS (secs) ---")
    print(f"Avg Desk per pax: {desk_mean:.2f}")
    print(f"Avg Kiosk:        {kiosk_mean:.2f}")

    print("\n--- PENETRATION (overall) ---")
    print(pen_all)

    print("\n--- PENETRATION (Apr–Sep) ---")
    print(pen_apr_sep)
    t5 = step(t4, "Console summary printed")

    # [6/6] Optional Excel output
    if excel_out:
        print("[6/6] Writing Excel...")

        #Rolling hour
        write_once_then_update(
            path=excel_out, sheet="Rolling_Hour",
            df=roll[["Bucket","CUPPS_Pax","SITA_Pax","Total_Pax","Rolling1h"]],
            anchor="A2", include_header=True
        )
        #Penetration overall
        write_once_then_update(
            path=excel_out, sheet="Penetration_All",
            df=pen_all, anchor="A2", include_header=True
        )
        #Penetration Apr -Sep
        write_once_then_update(
            path=excel_out, sheet="Penetration_Apr_Sep",
            df=pen_apr_sep, anchor="A2", include_header=True
        )
        #Durations
        durations_df = pd.DataFrame({
            "Metric": ["Avg Desk per pax (s)", "Avg Kiosk (s)"],
            "Value":  [desk_mean, kiosk_mean]
        })
        write_once_then_update(
            path=excel_out, sheet="Durations",
            df=durations_df, anchor="A2", include_header=True
        )
    print("\n✔ Check-in report complete.\n")


if __name__ == "__main__":
    # CLI mode (arguments present)
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser()
        parser.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
        parser.add_argument("--end",   required=True, help="YYYY-MM-DD (exclusive)")
        parser.add_argument("--out",   default=None,  help="Optional Excel output path")
        args = parser.parse_args()
        main(args.start, args.end, args.out)
    # VS Code run mode (no args): defaults
    else:
        DEFAULT_START = "2025-01-01"
        DEFAULT_END   = "2026-01-01"
        DEFAULT_OUT   = None  # e.g., "output/excel/checkin.xlsx"
        print("\nRunning checkin_report.py with defaults (no CLI args).")
        print(f"Using: {DEFAULT_START} → {DEFAULT_END}\n")
        main(DEFAULT_START, DEFAULT_END, DEFAULT_OUT)
