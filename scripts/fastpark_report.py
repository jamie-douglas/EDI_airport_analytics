
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

from modules.domain.fastpark import (
    monthly_movements_and_validations,
    peak_days_table,
    entry_exit_diffs_stats,
    entry_exit_histogram,
    checkin_duration_validation,
    length_of_stay,
    flight_info,
)


def load_fastpark(start: str, end: str, overlap: bool = True) -> pd.DataFrame:
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
        overlap=overlap
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


def main(start: str, end: str, excel_out: str | None, overlap: bool, plots: bool):
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
    print(f"Overlap: {overlap}")
    print(f"Plots  : {plots}\n")

    t0 = time.perf_counter()

    # 1) Load FastPark
    print("[1/7] Loading FastPark…")
    fp = load_fastpark(start, end, overlap)
    t1 = step(t0, f"Loaded FastPark ({len(fp):,} rows)")

    if fp.empty:
        print("No FastPark rows in this window.")
        return

    # 2) Load flights
    print("[2/7] Loading flight data…")
    fl = load_flights(start, end)
    t2 = step(t1, f"Loaded flights ({len(fl):,} rows)")

    # 3) Monthly movements
    print("[3/7] Monthly movements & validation…")
    monthly_df, base_summary = monthly_movements_and_validations(fp, start, end)
    t3 = step(t2, "Monthly movements computed")

    # 4) Duration validation
    print("[4/7] Check-in duration validation…")
    dur = checkin_duration_validation(fp)
    t4 = step(t3, "Duration validation complete")

    # 5) Peaks + diffs + histogram
    print("[5/7] Peaks + entry/exit stats…")
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
    print("[6/7] LOS + flight info…")
    avg_los, top3, bot3, los_bins = length_of_stay(fp, start, end)
    top_airlines, sectors = flight_info(fl, fp)

    # Tidy flight info tables
    top_airlines = top_airlines.rename(columns={"Airline_Description": "Name"})
    sectors = sectors.rename(columns={"Sector": "Name"})
    top_airlines["Category"] = "Airline"
    sectors["Category"] = "Sector"
    flight_info_df = pd.concat([top_airlines, sectors], ignore_index=True)[["Category", "Name", "Count"]]
    t6 = step(t5, "LOS + flight info complete")

    # Combined summary
    summary_full = pd.concat([base_summary, dur, peaks, central, desc], ignore_index=True)

    # 7) Excel outputs
    if excel_out:
        print("[7/7] Writing Excel…")

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

        step(t6, f"Excel updated → {excel_out}")

    print("\n✔ FastPark report complete.\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        p = argparse.ArgumentParser()
        p.add_argument("--start", required=True)
        p.add_argument("--end", required=True)
        p.add_argument("--out", default=None)
        p.add_argument("--no-overlap", action="store_true")
        p.add_argument("--plots", action="store_true")
        args = p.parse_args()

        main(
            args.start, args.end, args.out,
            overlap=not args.no_overlap,
            plots=args.plots
        )

    else:
        main("2025-01-01", "2025-01-05", None, True, False)
