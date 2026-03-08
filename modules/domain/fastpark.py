#modules/domain/fastpark.py

import pandas as pd
from typing import Tuple, Optional, Sequence

from modules.analytics.peaks import peak_day
from modules.analytics.bins import histogram_counts

def monthly_movements_and_validations(fp_df: pd.DataFrame, start: str, end: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Computes monthtly entry/exit counts and a validation summary
    
    Paramaters
    ----------
    fp_df: pd.DataFrame
        Input DataFrame
    start: str
        Start of window (ISO format)
    end: str
        End of window (ISO format)

    Returns
    ----------
    monthly_df: pandas.DataFrame
        Columns ['Month', 'Entries', 'Exits']
    summary_df: pandas.DataFrame
        Summary metrics and validation flags
        
    """

    df = fp_df.copy()
    start_ts, end_ts = pd.to_datetime(start), pd.to_datetime(end)

    #Filer entries within window using CheckInEnded
    entries = df.dropna(subset=["CheckInEnded"])
    entries = entries[
        (pd.to_datetime(entries["CheckInEnded"]) >= start_ts) &
        (pd.to_datetime(entries["CheckInEnded"]) < end_ts)
    ]

    #Filer exits within window using ActualCheckedOutDate
    exits = df.dropna(subset=["ActualCheckedOutDate"]).copy()
    exits = exits[
        (pd.to_datetime(exits["ActualCheckedOutDate"]) >= start_ts) &
        (pd.to_datetime(exits["ActualCheckedOutDate"]) < end_ts)
    ]

    #Monthly buckets using Period YYY-MM (no timestamp in Excel)
    if not entries.empty:
        entries["Month"]= pd.to_datetime(entries["CheckInEnded"]).dt.to_period("M")
        monthly_entries = entries.groupby("Month")["BookingReference"].nunique()
    else:
        monthly_entries = pd.Series(dtype = "float64")

    if not exits.empty:
        exits["Month"]= pd.to_datetime(exits["ActualCheckedOutDate"]).dt.to_period("M")
        monthly_exits = exits.groupby("Month")["BookingReference"].nunique()
    else:
        monthly_exits = pd.Series(dtype = "float64")

    #Ensure both series have the same index
    idx = pd.Index(sorted(set(monthly_entries.index).union(monthly_exits.index)),
                    name = "Month")
    
    #final monthly table
    monthly_df = pd.DataFrame({
        "Entries": monthly_entries.reindex(idx, fill_value = 0),
        "Exits": monthly_exits.reindex(idx, fill_value = 0)
    }).reset_index()

    
    # Convert Month to "YYYY-MM"
    monthly_df["Month"] = monthly_df["Month"].astype(str)

    #Total distinct transactions: A transaction is a booking that had an entry or exit in the window
    valid_refs = pd.unique(pd.concat([entries["BookingReference"], exits["BookingReference"]], ignore_index=True))
    total_transactions = df[df["BookingReference"].isin(valid_refs)]["BookingReference"].nunique()
    total_entries = entries["BookingReference"].nunique()
    total_exits = exits["BookingReference"].nunique()

    summary_df = pd.DataFrame({
        "Metric": [
            "Total Transactions (Distinct Bookings)",
            "Total Entries",
            "Total Exits",
            "Entries Validation Passed",
            "Exits Validation Passed"
        ],
        "Value": [
            total_transactions,
            total_entries,
            total_exits,
            monthly_df["Entries"].sum() == total_entries,
            monthly_df["Exits"].sum() == total_exits
            ]
    })

    return monthly_df, summary_df

def peak_days_table(fp_df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """
    Identify peak days for entries, exits and total movements
    
    Parameters
    ----------
    fp_df: pd.DataFrame
        Input DataFrame
    start: str
        Start of window (ISO format)
    end: str
        End of window (ISO format)

    Returns
    ----------
    pd.DataFrame
        Columns ['Peak Type', 'Date', 'Total']
        Rows: Peak Entries Day, Peak Exits Day, Peak Movements Day
    """

    df = fp_df.copy()
    start_ts, end_ts = pd.to_datetime(start), pd.to_datetime(end)

    #Filter entries and exits within the window
    entries = df.dropna(subset=["CheckInEnded"])
    entries = entries[
        (pd.to_datetime(entries["CheckInEnded"]) >= start_ts) &
        (pd.to_datetime(entries["CheckInEnded"]) < end_ts)
    ]

    exits = df.dropna(subset=["ActualCheckedOutDate"])
    exits = exits[
        (pd.to_datetime(exits["ActualCheckedOutDate"]) >= start_ts) &
        (pd.to_datetime(exits["ActualCheckedOutDate"]) < end_ts)
    ]


    #Daily distint counts per group
    daily_entries = entries.groupby(pd.to_datetime(entries["CheckInEnded"]).dt.date)["BookingReference"].nunique()
    daily_exits = exits.groupby(pd.to_datetime(exits["ActualCheckedOutDate"]).dt.date)["BookingReference"].nunique()
    daily_movements = daily_entries.add(daily_exits, fill_value = 0)

    #build one row for each peak type
    def build(label, s):
        if s.empty:
            return {"Peak Type": label, "Date": "n/a", "Total":0}
        d, v = peak_day(s)
        return {"Peak Type": label, "Date": d.strftime("%d %b %Y"), "Total": v}
    
    return pd.DataFrame([
        build("Peak Entries Day", daily_entries),
        build("Peak Exits Day", daily_exits),
        build("Peak Movements Day", daily_movements),
    ])

def entry_exit_diffs_stats(fp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute entry/ exit time-difference statistics in minutes
    
    Parameters
    ----------
    fp_df: pd.DataFrame
        Input DataFrame
        
    Returns
    ----------
    pd.DataFrame
        DataFrame with metrics: average/median entry/exit time differences in HH:MM format
    
    """
    df = fp_df.copy()

    #Parse datetimes
    for c in ["CheckInStarted", "ExpectedArrivalDate", "ActualCheckedOutDate", "ExpectedReturnDate"]:
        df[c] = pd.to_datetime(df[c], errors = "coerce")

    #Differences in minutes
    df["EntryDiffMin"] = (df["CheckInStarted"] - df["ExpectedArrivalDate"]).dt.total_seconds() / 60
    df["ExitDiffMin"] = (df["ActualCheckedOutDate"] - df["ExpectedReturnDate"]).dt.total_seconds() / 60

    #Drop invalids per series

    e = df["EntryDiffMin"].dropna()
    x = df["ExitDiffMin"].dropna()

    #Averages/Medians
    avg_e, med_e = e.mean(), e.median()
    avg_x, med_x = x.mean(), x.median()

    #HH:MM formatter
    def hhmm(v):
        sign = "-" if v < 0 else ""
        v = abs(v)
        return f"{sign}{int(v//60):02d}:{int(v%60):02d}"
    
    central_df = pd.DataFrame({
        "Metric": [
            "Average Entry Difference (HH:MM)",
            "Median Entry Difference (HH:MM)",
            "Average Exit Difference (HH:MM)",
            "Median Exit Difference (HH:MM)",
        ],
        "Value": [
            hhmm(avg_e), hhmm(med_e), hhmm(avg_x), hhmm(med_x)
        ]
    })
    
    # Percentiles
    e_desc, x_desc = e.describe(), x.describe()
    describe_df = pd.DataFrame({
        "Metric": [
            "Entry Min (mins)", "Entry 25th Percentile (mins)",
            "Entry 75th Percentile (mins)", "Entry Max (mins)",
            "Exit Min (mins)", "Exit 25th Percentile (mins)",
            "Exit 75th Percentile (mins)", "Exit Max (mins)",
        ],
        "Value": [
            e_desc.get("min"), e_desc.get("25%"), e_desc.get("75%"), e_desc.get("max"),
            x_desc.get("min"), x_desc.get("25%"), x_desc.get("75%"), x_desc.get("max")
        ]
    })

    return central_df, describe_df, float(avg_e), float(med_e), float(avg_x), float(med_x)


def entry_exit_histogram(
    fp_df: pd.DataFrame,
    avg_entry: float,
    med_entry: float,
    avg_exit: float,
    med_exit: float,
    bins: Sequence[float] = (-99999, -180, -120, -60, -30, 0, 30, 60, 120, 180, 99999)
) -> pd.DataFrame:
    """
    Build histogram of entry/exit time differences with overlay columns.

    Parameters
    ----------
    fp_df: pd.DataFrame
        Input DataFrame
    avg_entry: float
        Average entry time difference in minutes
    med_entry: float
        Median entry time difference in minutes
    avg_exit: float
        Average exit time difference in minutes
    med_exit: float
        Median exit time difference in minutes
    bins: Sequence[float]
        Bin edges for histogram (default covers wide range with focus around 0)

    Returns
    ----------
    pd.DataFrame
        DataFrame with columns ['Bin Start', 'Bin End', 'Bin Midpoint', 'Entry Count', 'Exit Count', 'Zero Line', 'Avg Entry Line', 'Median Entry Line', 'Avg Exit Line', 'Median Exit Line']

    """
    df = fp_df.copy()

    # Parse datetimes if present
    for c in ["CheckInStarted", "ExpectedArrivalDate", "ActualCheckedOutDate", "ExpectedReturnDate"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # Compute differences if missing
    if "EntryDiffMin" not in df:
        df["EntryDiffMin"] = (df["CheckInStarted"] - df["ExpectedArrivalDate"]).dt.total_seconds()/60
    if "ExitDiffMin" not in df:
        df["ExitDiffMin"] = (df["ActualCheckedOutDate"] - df["ExpectedReturnDate"]).dt.total_seconds()/60

    # Hist counts
    h_e = histogram_counts(df["EntryDiffMin"], bins=bins)
    h_x = histogram_counts(df["ExitDiffMin"], bins=bins)

    # Merge entry/exit histograms
    out = h_e.merge(
        h_x, on=["Bin Start", "Bin End", "Bin Midpoint"], how="outer",
        suffixes=("_Entry", "_Exit")
    ).fillna(0)

    # Standard column names + overlays
    out = out.rename(columns={"Count_Entry": "Entry Count", "Count_Exit": "Exit Count"})
    out["Zero Line"] = 0
    out["Avg Entry Line"] = avg_entry
    out["Median Entry Line"] = med_entry
    out["Avg Exit Line"] = avg_exit
    out["Median Exit Line"] = med_exit

    return out


def checkin_duration_validation(fp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate recorded kiosk duration against computed duration.

    Parameters
    ----------
    fp_df: pd.DataFrame
        Input DataFrame with columns 'CheckInStarted', 'CheckInEnded', 'CheckInDurationSecs'

    Returns
    ----------
    pd.DataFrame
        Kiosk duration summary

    """
    for c in ["CheckInStarted", "CheckInEnded"]:
        fp_df[c] = pd.to_datetime(fp_df[c], errors="coerce")

    #Compute actual duration in seconds
    fp_df["ActualDurationSecs"] = (fp_df["CheckInEnded"] - fp_df["CheckInStarted"]).dt.total_seconds()
    fp_df["DurationMatch"] = fp_df["ActualDurationSecs"] == fp_df["CheckInDurationSecs"]

    return pd.DataFrame({
        "Metric": [
            "Average Kiosk Transaction Time (secs)",
            "Invaild Duration Count",
            "Total Transactions"
        ],
        "Value": [
            float(fp_df["ActualDurationSecs"].mean()) if len(fp_df) > 0 else 0.0,
            int((~fp_df["DurationMatch"]).sum()),
            int(len(fp_df))
        ]
    })

def length_of_stay(fp_df: pd.DataFrame, start: str, end: str, bins: Optional[Sequence[int]] = None, max_days: int = 90):
    """
    Compute LOS (length of stay) metrics and distribution.

    Parameters
    ----------
    fp_df: pd.DataFrame
        Input DataFrame with columns 'CheckInEnded' and 'ActualCheckedOutDate'
    start: str
        Start of window (ISO format)
    end: str
        End of window (ISO format)
    bins: Optional[Sequence[int]]
        Bin edges for LOS distribution (in days). If None, defaults to [1,2,3,7,10,14,21,30,60,90]
    max_days: int
        Maximum LOS in days to include in analysis (default 90). Records with LOS above this will be filtered out as likely data quality issues.

    Returns
    ----------
    avg_df: pd.DataFrame
        DataFrame with average LOS
    top3: pd.DataFrame
        Top 3 longest LOS records with columns ['BookingReference', 'Length of Stay Days']
    bottom3: pd.DataFrame
        Bottom 3 shortest LOS records with columns ['BookingReference', 'Length of Stay Days']
    bins_df: pd.DataFrame
        LOS distribution with columns ['Bin', 'Count'] where 'Bin' is a human-friendly label for the LOS range (e.g. "0-1", "1-2", "2-3", "3-6", etc.)
    """
    df = fp_df.copy()
    start_ts, end_ts = pd.to_datetime(start), pd.to_datetime(end)

    # Parse datetimes
    for c in ["CheckInEnded", "ActualCheckedOutDate"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")

    # Collapse to one row per booking with min start/max end
    stays = df.groupby("BookingReference").agg({
        "CheckInEnded": "min",
        "ActualCheckedOutDate": "max"
    }).reset_index()

    
    # Include only stays whose checkout is inside window
    stays = stays[
        (stays["ActualCheckedOutDate"] >= start_ts) &
        (stays["ActualCheckedOutDate"] <  end_ts)
    ]


    # LOS in days
    stays["Length of Stay Days"] = (
        stays["ActualCheckedOutDate"] - stays["CheckInEnded"]
    ).dt.total_seconds() / 86400

    # Basic data quality filtering
    stays = stays[
        (stays["Length of Stay Days"] >= 0) &
        (stays["Length of Stay Days"] <= max_days)
    ]

    # Average LOS
    avg_df = pd.DataFrame({
        "Metric": ["Average Length of Stay (Days)"],
        "Value": [stays["Length of Stay Days"].mean()]
    })

    # Extremes
    top3 = stays.nlargest(3, "Length of Stay Days")[["BookingReference", "Length of Stay Days"]]
    bottom3 = stays.nsmallest(3, "Length of Stay Days")[["BookingReference", "Length of Stay Days"]]

    # Default bins if none provided
    if bins is None:
        bins = [1,2,3,7,10,14,21,30,60,90]

    # Bin + count
    stays["Bin"] = pd.cut(stays["Length of Stay Days"], bins=bins, right=False)
    bins_df = stays.groupby("Bin")["BookingReference"].nunique().reset_index()
    bins_df = bins_df.rename(columns={"BookingReference": "Count"})

    # Human-friendly labels
    labels = []
    for i in range(len(bins) - 1):
        a, b = bins[i], bins[i + 1] - 1
        labels.append(f"{a}" if a == b else f"{a}-{b}")
    bins_df["Bin"] = labels

    return avg_df, top3, bottom3, bins_df


def flight_info(flight_df: pd.DataFrame, fp_df: pd.DataFrame):
    """
    Join FastPark bookings with flight schedule and compute:
        - top 3 airlines
        - sector distribution

    Parameters
    ----------
    flight_df: pd.DataFrame
        Flight schedule DataFrame with columns 'Combined Flight Code', 'Scheduled DateTime', 'Airline_Description', 'Sector'
    fp_df: pd.DataFrame
        FastPark DataFrame with columns 'BookingReference', 'ExpectedReturnDate', 'ReturnFlight'
    
    Returns
    ----------
    top_airlines: pd.DataFrame
        Columns ['Airline Description', 'Count'] with top 3 airlines by unique bookings
    sector_counts: pd.DataFrame
        Columns ['Sector', 'Count'] with unique booking counts per sector
    
    """
    fp_unique = fp_df.drop_duplicates(subset="BookingReference").copy()
    f = flight_df.copy()

    # Keys for flight schedule
    f["Flight Date"] = pd.to_datetime(f["Scheduled DateTime"]).dt.date
    f["Flight Key"] = f["Combined Flight Code"] + f["Flight Date"].astype(str)

    # Keys for FastPark records
    fp_unique["Flight Date"] = pd.to_datetime(fp_unique["ExpectedReturnDate"]).dt.date
    fp_unique["Flight Key"] = fp_unique["ReturnFlight"] + fp_unique["Flight Date"].astype(str)

    merged = fp_unique.merge(f, on="Flight Key", how="left")

    # Top 3 airlines
    top_airlines = (
        merged.groupby("Airline_Description")["BookingReference"]
        .nunique().sort_values(ascending=False)
        .reset_index().rename(columns={"BookingReference":"Count"})
        .head(3)
    )

    # Sector counts
    sector_counts = (
        merged.groupby("Sector")["BookingReference"]
        .nunique().sort_values(ascending=False)
        .reset_index().rename(columns={"BookingReference":"Count"})
    )

    return top_airlines, sector_counts
