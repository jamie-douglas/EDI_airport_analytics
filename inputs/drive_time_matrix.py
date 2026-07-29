import os
import re
import pandas as pd
import numpy as np

# ============================================================
# CONFIG
# ============================================================

INPUT_FILE = "inputs/movements_july25_26.csv"

OUTPUT_EXCEL = "inputs/driving_time_matrices.xlsx"

MAX_DRIVING_TIME_MINUTES = 90

# ============================================================
# LOAD
# ============================================================

print("Loading file...")

df = pd.read_csv(
    INPUT_FILE,
    low_memory=False
)

df.columns = df.columns.str.strip().str.lower()

print(f"Rows loaded: {len(df):,}")

# ============================================================
# LOCATION NORMALISATION
# ============================================================

def get_location(value):

    if pd.isna(value):
        return np.nan

    value = str(value).split("/")[0].strip()

    value_lower = value.lower()

    if value_lower == "customer":
        return "Drop-off"

    if value_lower.startswith("returns"):
        return "Returns"

    match = re.match(
        r"^block\s*([A-Za-z])",
        value,
        flags=re.IGNORECASE
    )

    if match:
        return f"Block {match.group(1).upper()}"

    return np.nan


df["from_location"] = df["from"].apply(get_location)
df["to_location"] = df["to"].apply(get_location)

print("Location normalisation complete")

# ============================================================
# PARSE ONLY TIME RANGES
# ============================================================

times = df["time"].astype(str).str.extract(
    r"(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})"
)

df["start_time"] = times[0]
df["end_time"] = times[1]

print(
    f"Rows with usable ranges: "
    f"{df['start_time'].notna().sum():,}"
)

# ============================================================
# DATETIMES
# ============================================================

df = df[df["start_time"].notna()].copy()

df["start_dt"] = pd.to_datetime(
    df["date"].astype(str)
    + " "
    + df["start_time"],
    dayfirst=True,
    errors="coerce"
)

df["end_dt"] = pd.to_datetime(
    df["date"].astype(str)
    + " "
    + df["end_time"],
    dayfirst=True,
    errors="coerce"
)

df.loc[
    df["end_dt"] < df["start_dt"],
    "end_dt"
] += pd.Timedelta(days=1)

df["duration_minutes"] = (
    (df["end_dt"] - df["start_dt"])
    .dt.total_seconds()
    / 60
)

# ============================================================
# FILTER VALID MOVEMENTS
# ============================================================

df = df[
    df["from_location"].notna()
].copy()

df = df[
    df["to_location"].notna()
].copy()

df = df[
    df["from_location"]
    !=
    df["to_location"]
].copy()

df = df[
    df["duration_minutes"] > 0
].copy()

df = df[
    df["duration_minutes"]
    <= MAX_DRIVING_TIME_MINUTES
].copy()

print(f"Rows used in matrix: {len(df):,}")

# ============================================================
# UNDIRECTED PAIRS
# ============================================================

pairs = np.sort(
    df[["from_location", "to_location"]],
    axis=1
)

df["location_1"] = pairs[:, 0]
df["location_2"] = pairs[:, 1]

# ============================================================
# PAIR SUMMARY
# ============================================================

pair_summary = (
    df
    .groupby(
        ["location_1", "location_2"]
    )
    .agg(
        count=("duration_minutes", "count"),
        average_minutes=("duration_minutes", "mean"),
        median_minutes=("duration_minutes", "median"),
        min_minutes=("duration_minutes", "min"),
        max_minutes=("duration_minutes", "max"),
        p95_minutes=(
            "duration_minutes",
            lambda x: np.percentile(x, 95)
        )
    )
    .reset_index()
)

for col in [
    "average_minutes",
    "median_minutes",
    "min_minutes",
    "max_minutes",
    "p95_minutes"
]:
    pair_summary[col] = pair_summary[col].round(2)

print(pair_summary)

# ============================================================
# MATRIX BUILD
# ============================================================

locations = sorted(
    list(
        set(pair_summary["location_1"])
        |
        set(pair_summary["location_2"])
    )
)

average_matrix = pd.DataFrame(
    np.nan,
    index=locations,
    columns=locations
)

median_matrix = pd.DataFrame(
    np.nan,
    index=locations,
    columns=locations
)

max_matrix = pd.DataFrame(
    np.nan,
    index=locations,
    columns=locations
)

count_matrix = pd.DataFrame(
    np.nan,
    index=locations,
    columns=locations
)

for _, row in pair_summary.iterrows():

    a = row["location_1"]
    b = row["location_2"]

    average_matrix.loc[a, b] = row["average_minutes"]
    average_matrix.loc[b, a] = row["average_minutes"]

    median_matrix.loc[a, b] = row["median_minutes"]
    median_matrix.loc[b, a] = row["median_minutes"]

    max_matrix.loc[a, b] = row["max_minutes"]
    max_matrix.loc[b, a] = row["max_minutes"]

    count_matrix.loc[a, b] = row["count"]
    count_matrix.loc[b, a] = row["count"]

for loc in locations:

    average_matrix.loc[loc, loc] = 0
    median_matrix.loc[loc, loc] = 0
    max_matrix.loc[loc, loc] = 0
    count_matrix.loc[loc, loc] = 0

# ============================================================
# SAVE
# ============================================================

print("Writing Excel...")

with pd.ExcelWriter(
    OUTPUT_EXCEL,
    engine="openpyxl"
) as writer:

    average_matrix.reset_index().rename(
        columns={"index": "Location"}
    ).to_excel(
        writer,
        sheet_name="Average Matrix",
        index=False
    )

    median_matrix.reset_index().rename(
        columns={"index": "Location"}
    ).to_excel(
        writer,
        sheet_name="Median Matrix",
        index=False
    )

    max_matrix.reset_index().rename(
        columns={"index": "Location"}
    ).to_excel(
        writer,
        sheet_name="Max Matrix",
        index=False
    )

    count_matrix.reset_index().rename(
        columns={"index": "Location"}
    ).to_excel(
        writer,
        sheet_name="Count Matrix",
        index=False
    )

    pair_summary.to_excel(
        writer,
        sheet_name="Pair Summary",
        index=False
    )

print("")
print("================================================")
print("COMPLETE")
print("================================================")
print(f"Saved to: {os.path.abspath(OUTPUT_EXCEL)}")
print("================================================")