
import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import argparse
import time
import pandas as pd
import os

from modules.utils.query import query
from modules.utils.excel import write_once_then_update
from modules.utils.progress import step
from modules.viz.fastpark import plot_distribution
from modules.analytics.growth import period_growth

from modules.domain.fastpark import (
    monthly_movements_and_validations,
    peak_days_table,
    entry_exit_diffs_stats,
    entry_exit_histogram,
    checkin_duration_validation,
    length_of_stay,
    flight_info,
)


def load_fastpark(start: str, end: str, overlap: bool = False, or_events: bool = True) -> pd.DataFrame:
    """
    Load FastPark stays using correct interval:
      StayStart = CheckInStarted
      StayEnd   = ActualCheckedOutDate

    Parameters
    ----------
    start: str
        Start of window (ISO format)
    end: str
        End of window (ISO format)
    overlap: bool
        If true, returns rows where the stay overlaps the window (StayStart < end AND StayEnd >= start).
        If false, returns rows where the stay is fully contained within the window (StayStart >= start AND StayEnd < end).
        Default is True (overlapping).
    
    Returns
    ----------
    pd.DataFrame
        DataFrame of FastPark entries/exits in the window, with columns:
        ['BookingReference', 'CheckInStarted', 'CheckInEnded', 'CheckInDurationSecs',
         'ExpectedArrivalDate', 'ExpectedReturnDate', 'ReturnFlight', 'ActualCheckedOutDate']
    """
    return query(
        table="FastPark.v_EntryAndExits",
        columns=[
            "BookingReference",
            "CheckInStarted",
            "CheckInEnded",
            "CheckInDurationSecs",
            "ExpectedArrivalDate",
            "ExpectedReturnDate",
            "ReturnFlight",
            "ActualCheckedOutDate",
        ],
        date_column="CheckInStarted",        # interval start
        end_column="ActualCheckedOutDate",   # interval end
        start=start,
        end=end,
        or_events=True,
    )


def load_flights(start: str, end: str) -> pd.DataFrame:
    """
    Load flights for airline/sector attribution.

    Parameters
    ----------
    start: str
        Start of window (ISO format)
    end: str
        End of window (ISO format)
    
    Returns
    ----------
    pd.DataFrame
        DataFrame of flights in the window, with columns:
        ['Scheduled DateTime', 'AirlineCode_IATA', 'Airline_Description', 'FlightNumber',
        'AirportDescription', 'Sector', 'Passengers']
    """
    df = query(
        table="EAL.FlightPerformance",
        columns=[
            "ScheduledDateTime_Local AS [Scheduled DateTime]",
            "AirlineCode_IATA",
            "Airline_Description",
            "FlightNumber",
            "AirportDescription",
            "Sector",
            "Passengers",
        ],
        date_column="ScheduledDateTime_Local",
        start=start,
        end=end
    )

    # Normalise to a combined code
    df["Combined Flight Code"] = df["AirlineCode_IATA"] + df["FlightNumber"].astype(str).str.lstrip("0")
    return df


def _date_filter_mode_line(or_events: bool, overlap: bool,
                           date_col: str = "CheckInStarted",
                           end_col: str = "ActualCheckedOutDate") -> str:
    """
    Build a one-line description of the date filtering logic.

    - or_events=True: (date_col ∈ [start,end)) OR (end_col ∈ [start,end))
    - or_events=False and end_col present:
        * overlap=True : (date_col < end) AND (end_col >= start)
        * overlap=False: (date_col >= start) AND (end_col < end)
    - If only date_col is used: (date_col ∈ [start,end))
    """
    if or_events:
        return (f"Mode   : OR events  — ({date_col} OR {end_col} in [start, end))")
    # interval mode
    if end_col:
        return (f"Mode   : Interval   — "
                f"{'Overlap' if overlap else 'Contained'} "
                f"({date_col} {'<' if overlap else '>='} end and "
                f"{end_col} {'>=' if overlap else '<'} start)")
    # single-timestamp fallback (not used in FastPark)
    return f"Mode   : Single ts  — ({date_col} in [start, end))"



def main(start: str, end: str, excel_out: str | None, overlap: bool, plots: bool, or_events: bool = True):
    """
    Run the FastPark report for a date window.

    Parameters
    ----------
    start: str
        Start of window (ISO format)
    end: str
        End of window (ISO format)
    excel_out: str or None
        If not None, path to Excel file to write outputs to (will be created if doesn't exist).
    overlap: bool
        If true, includes FastPark stays that overlap the window (StayStart < end AND StayEnd >= start).
        If false, includes only stays fully contained within the window (StayStart >= start AND StayEnd < end).
    plots: bool
        If true, saves entry/exit distribution charts as PNG files.
    
    Returns
    ----------
        None (prints report and optionally saves Excel/charts)
    """
    
    if pd.Timestamp(end) <= pd.Timestamp(start):
        raise ValueError("--end must be after --start")

    print("\nFASTPARK REPORT")
    print(f"Window : {start} → {end}")
    print(_date_filter_mode_line(or_events, overlap))
    print(f"Plots  : {plots}\n")


    t0 = time.perf_counter()

    # 1) Load FastPark
    print("[1/8] Loading FastPark…")
    fp = load_fastpark(start, end, or_events)
    t1 = step(t0, f"Loaded FastPark ({len(fp):,} rows)")

    if fp.empty:
        print("No FastPark rows in this window.")
        return

    # 2) Load flights
    print("[2/8] Loading flight data…")
    fl = load_flights(start, end)
    t2 = step(t1, f"Loaded flights ({len(fl):,} rows)")

    # 3) Monthly movements
    print("[3/8] Monthly movements & validation…")
    monthly_df, base_summary = monthly_movements_and_validations(fp, start, end)
    t3 = step(t2, "Monthly movements computed")

    # 4) Duration validation
    print("[4/8] Check-in duration validation…")
    dur = checkin_duration_validation(fp)
    t4 = step(t3, "Duration validation complete")

    # 5) Peaks + diffs + histogram
    print("[5/8] Peaks + entry/exit stats…")
    peaks = peak_days_table(fp, start, end)
    central, desc, avg_e, med_e, avg_x, med_x = entry_exit_diffs_stats(fp)
    hist = entry_exit_histogram(fp, avg_e, med_e, avg_x, med_x)
    t5 = step(t4, "Peaks/stats/hist computed")

    
    # Optional charts
    if plots:
        print("      • Saving charts…")

        import os  # ensure available for folder creation

        # Define where plots should be saved (inside your repo output folder)
        plot_folder = "output/fastpark/plots/"
        os.makedirs(plot_folder, exist_ok=True)  # create if missing

        # Save PNGs into output folder
        plot_distribution(hist, "entry", plot_folder + "fastpark_entry_distribution.png")
        plot_distribution(hist, "exit",  plot_folder + "fastpark_exit_distribution.png")

        t5 = step(t5, f"Charts saved → {plot_folder}")


    # 6) LOS + flights
    print("[6/8] LOS + flight info…")
    
    avg_los, top3, bot3, los_bins = length_of_stay(fp, start, end)
    top_airlines, sectors = flight_info(fl, fp)

    # Rename for consistency
    top_airlines = top_airlines.rename(columns={"Airline_Description": "Name"})
    sectors = sectors.rename(columns={"Sector": "Name"})

    # Tag categories
    top_airlines["Category"] = "Airline"
    sectors["Category"] = "Sector"

    # Include Percent in output
    flight_info_df = pd.concat(
        [top_airlines, sectors],
        ignore_index=True
    )[["Category", "Name", "Count", "Percent"]]

    t6 = step(t5, "LOS + flight info complete")

    
    # ---- Transaction Growth (multi-year) ----
    print("[7/8] Computing transaction growth...")
    growth_df = period_growth(
        loader_fn=load_fastpark,
        start=start,
        end=end,
        years_back=3,
        id_col="BookingReference",
        loader_kwargs={
            "overlap": overlap,
            "or_events": or_events
        }
    )
    t7 = step(t6, "Transaction growth computed")



    # Combined summary
    summary_full = pd.concat([base_summary, dur, peaks, central, desc], ignore_index=True)

    # 7) Excel outputs
    if excel_out:
        print("[8/8] Writing Excel…")

        write_once_then_update(
            excel_out, "Monthly Movements", monthly_df, anchor="A2", include_header=True
        )
        write_once_then_update(
            excel_out, "Summary", summary_full, anchor="A2", include_header=True
        )
        write_once_then_update(
            excel_out, "Entry Exit Distribution", hist, anchor="A2", include_header=True
        )

        # LOS combined helper table
        los_combined = pd.concat([
            top3.assign(Kind="Top 3"),
            bot3.assign(Kind="Bottom 3"),
            pd.DataFrame({
                "BookingReference": ["Average LOS"],
                "Length of Stay Days": [avg_los["Value"].iloc[0]],
                "Kind": ["Average"]
            }),
            los_bins.rename(columns={"Bin": "BookingReference", "Count": "Length of Stay Days"}).assign(Kind="Bins")
        ], ignore_index=True)

        write_once_then_update(
            excel_out, "Length of Stay", los_combined, anchor="A2", include_header=True
        )
        write_once_then_update(
            excel_out, "Flight Info", flight_info_df, anchor="A2", include_header=True
        )

        write_once_then_update(
            excel_out, "Transaction Growth", growth_df, anchor="A2", include_header=True
        )

        step(t7, f"Excel updated → {excel_out}")

    print("\n✔ FastPark report complete.\n")



if __name__ == "__main__":
    if len(sys.argv) > 1:
        p = argparse.ArgumentParser()
        p.add_argument("--start", required=True)
        p.add_argument("--end", required=True)
        p.add_argument("--out", default=None, help ="output Excel File path(optional)")
        p.add_argument("--no-overlap", action="store_true",
                       help="When in interval mode, use fully-contained instead of overlap.")
        p.add_argument("--plots", action="store_true")
        p.add_argument("--or-events", action="store_true",
                       help="Use legacy OR-events windowing (either start OR end in [start,end)).")
        p.add_argument("--no-or-events", action="store_true",
                       help="Disable OR-events; use interval mode with --no-overlap to control containment.")
        args = p.parse_args()

        # Resolve flags:
        # default True unless user explicitly passes --no-or-events
        use_or_events = args.or_events or not args.no_or_events
        use_overlap   = not args.no_overlap

        main(args.start, args.end, args.out,
             overlap=use_overlap,
             plots=args.plots,
             or_events=use_or_events)
    else:
        main("2025-01-01", "2025-01-05", None, True, False, or_events=True)
