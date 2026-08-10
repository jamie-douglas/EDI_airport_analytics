import os
import re
import pandas as pd
import numpy as np

# ============================================================
# CONFIG
# ============================================================

INPUT_FILE = "inputs/movements_july25_26.csv"

OUTPUT_EXCEL = "inputs/driving_time_matrices_v2.xlsx"

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
        r"^block\s*([A-Za-z]+)",
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
# DISTRIBUTION SHAPE AND BEST-FIT TEST
# ============================================================
#
# Tests whether each location pair's positive movement times
# resemble:
#   - Normal
#   - Lognormal
#   - Gamma
#   - Weibull
#
# AIC interpretation:
#   The distribution with the lowest AIC is the best fit among
#   the candidate distributions tested.
#
# Important:
#   "Best fit" does not mean the data perfectly follows that
#   theoretical distribution. Always inspect the histogram too.
# ============================================================

from scipy import stats
import matplotlib.pyplot as plt

MIN_SAMPLE_FOR_FIT = 30

distribution_results = []

for (location_1, location_2), group in df.groupby(
    ["location_1", "location_2"]
):

    movement_times = (
        group["duration_minutes"]
        .dropna()
        .astype(float)
    )

    movement_times = movement_times[
        movement_times > 0
    ]

    sample_size = len(movement_times)

    if sample_size < MIN_SAMPLE_FOR_FIT:
        distribution_results.append({
            "location_1": location_1,
            "location_2": location_2,
            "count": sample_size,
            "mean_minutes": movement_times.mean(),
            "median_minutes": movement_times.median(),
            "skewness": movement_times.skew(),
            "best_fit_distribution": "Insufficient sample",
            "normal_aic": np.nan,
            "lognormal_aic": np.nan,
            "gamma_aic": np.nan,
            "weibull_aic": np.nan
        })
        continue

    aic_values = {}

    candidate_distributions = {
        "Normal": stats.norm,
        "Lognormal": stats.lognorm,
        "Gamma": stats.gamma,
        "Weibull": stats.weibull_min
    }

    for distribution_name, distribution in candidate_distributions.items():

        try:

            if distribution_name == "Normal":
                parameters = distribution.fit(movement_times)
            else:
                # Movement times cannot be negative, so fix
                # the distribution location parameter at zero.
                parameters = distribution.fit(
                    movement_times,
                    floc=0
                )

            log_likelihood = np.sum(
                distribution.logpdf(
                    movement_times,
                    *parameters
                )
            )

            number_of_parameters = len(parameters)

            aic = (
                2 * number_of_parameters
                - 2 * log_likelihood
            )

            aic_values[distribution_name] = aic

        except Exception:
            aic_values[distribution_name] = np.nan

    valid_aic_values = {
        name: value
        for name, value in aic_values.items()
        if pd.notna(value)
    }

    if valid_aic_values:
        best_fit = min(
            valid_aic_values,
            key=valid_aic_values.get
        )
    else:
        best_fit = "Unable to fit"

    distribution_results.append({
        "location_1": location_1,
        "location_2": location_2,
        "count": sample_size,
        "mean_minutes": movement_times.mean(),
        "median_minutes": movement_times.median(),
        "standard_deviation_minutes": movement_times.std(),
        "skewness": movement_times.skew(),
        "best_fit_distribution": best_fit,
        "normal_aic": aic_values.get("Normal"),
        "lognormal_aic": aic_values.get("Lognormal"),
        "gamma_aic": aic_values.get("Gamma"),
        "weibull_aic": aic_values.get("Weibull")
    })


distribution_fit_summary = pd.DataFrame(
    distribution_results
)

numeric_columns = [
    "mean_minutes",
    "median_minutes",
    "standard_deviation_minutes",
    "skewness",
    "normal_aic",
    "lognormal_aic",
    "gamma_aic",
    "weibull_aic"
]

distribution_fit_summary[numeric_columns] = (
    distribution_fit_summary[numeric_columns]
    .round(2)
)

print("")
print("================================================")
print("DISTRIBUTION FIT BY LOCATION PAIR")
print("================================================")
print(distribution_fit_summary.to_string(index=False))

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
# MOVEMENT TIME DISTRIBUTION
# ============================================================
#
# Purpose:
#   Shows how movement times are distributed for each location
#   pair, rather than relying only on the average or maximum.
#
# Important:
#   This uses the same valid movement records used in the matrices:
#     - valid time ranges only
#     - recognised locations only
#     - different origin and destination
#     - duration above 0 minutes
#     - duration no greater than 90 minutes
# ============================================================


# ------------------------------------------------------------
# 1. OVERALL DISTRIBUTION SUMMARY
# ------------------------------------------------------------

overall_distribution = pd.DataFrame({
    "metric": [
        "Movement count",
        "Average minutes",
        "Median minutes",
        "Standard deviation",
        "P10 minutes",
        "P25 minutes",
        "P75 minutes",
        "P90 minutes",
        "P95 minutes",
        "P99 minutes",
        "Minimum minutes",
        "Maximum minutes"
    ],
    "value": [
        len(df),
        df["duration_minutes"].mean(),
        df["duration_minutes"].median(),
        df["duration_minutes"].std(),
        df["duration_minutes"].quantile(0.10),
        df["duration_minutes"].quantile(0.25),
        df["duration_minutes"].quantile(0.75),
        df["duration_minutes"].quantile(0.90),
        df["duration_minutes"].quantile(0.95),
        df["duration_minutes"].quantile(0.99),
        df["duration_minutes"].min(),
        df["duration_minutes"].max()
    ]
})

overall_distribution["value"] = (
    overall_distribution["value"].round(2)
)

print("")
print("================================================")
print("OVERALL MOVEMENT TIME DISTRIBUTION")
print("================================================")
print(overall_distribution)


# ------------------------------------------------------------
# 2. DISTRIBUTION SUMMARY BY LOCATION PAIR
# ------------------------------------------------------------

pair_distribution = (
    df
    .groupby(["location_1", "location_2"])
    .agg(
        count=("duration_minutes", "count"),
        average_minutes=("duration_minutes", "mean"),
        standard_deviation_minutes=("duration_minutes", "std"),
        p10_minutes=(
            "duration_minutes",
            lambda x: x.quantile(0.10)
        ),
        p25_minutes=(
            "duration_minutes",
            lambda x: x.quantile(0.25)
        ),
        median_minutes=("duration_minutes", "median"),
        p75_minutes=(
            "duration_minutes",
            lambda x: x.quantile(0.75)
        ),
        p90_minutes=(
            "duration_minutes",
            lambda x: x.quantile(0.90)
        ),
        p95_minutes=(
            "duration_minutes",
            lambda x: x.quantile(0.95)
        ),
        p99_minutes=(
            "duration_minutes",
            lambda x: x.quantile(0.99)
        ),
        minimum_minutes=("duration_minutes", "min"),
        maximum_minutes=("duration_minutes", "max")
    )
    .reset_index()
)

pair_distribution["interquartile_range_minutes"] = (
    pair_distribution["p75_minutes"]
    - pair_distribution["p25_minutes"]
)

numeric_distribution_columns = [
    "average_minutes",
    "standard_deviation_minutes",
    "p10_minutes",
    "p25_minutes",
    "median_minutes",
    "p75_minutes",
    "p90_minutes",
    "p95_minutes",
    "p99_minutes",
    "minimum_minutes",
    "maximum_minutes",
    "interquartile_range_minutes"
]

pair_distribution[numeric_distribution_columns] = (
    pair_distribution[numeric_distribution_columns].round(2)
)

print("")
print("================================================")
print("MOVEMENT TIME DISTRIBUTION BY LOCATION PAIR")
print("================================================")
print(pair_distribution.to_string(index=False))


# ------------------------------------------------------------
# 3. MOVEMENT TIME BANDS
# ------------------------------------------------------------
#
# These bands provide an operationally simple view of the shape
# of the movement-time distribution.
# ------------------------------------------------------------

duration_bins = [
    0,
    2,
    5,
    10,
    15,
    20,
    30,
    45,
    60,
    MAX_DRIVING_TIME_MINUTES
]

duration_labels = [
    ">0 to 2 mins",
    ">2 to 5 mins",
    ">5 to 10 mins",
    ">10 to 15 mins",
    ">15 to 20 mins",
    ">20 to 30 mins",
    ">30 to 45 mins",
    ">45 to 60 mins",
    f">60 to {MAX_DRIVING_TIME_MINUTES} mins"
]

df["duration_band"] = pd.cut(
    df["duration_minutes"],
    bins=duration_bins,
    labels=duration_labels,
    right=True,
    include_lowest=False
)

overall_duration_bands = (
    df
    .groupby("duration_band", observed=False)
    .size()
    .reset_index(name="movement_count")
)

overall_duration_bands["percentage"] = np.where(
    overall_duration_bands["movement_count"].sum() > 0,
    (
        overall_duration_bands["movement_count"]
        / overall_duration_bands["movement_count"].sum()
        * 100
    ),
    np.nan
)

overall_duration_bands["percentage"] = (
    overall_duration_bands["percentage"].round(1)
)

print("")
print("================================================")
print("OVERALL MOVEMENT TIME BANDS")
print("================================================")
print(overall_duration_bands)


# ------------------------------------------------------------
# 4. MOVEMENT TIME BANDS BY LOCATION PAIR
# ------------------------------------------------------------

pair_duration_bands = (
    df
    .groupby(
        [
            "location_1",
            "location_2",
            "duration_band"
        ],
        observed=False
    )
    .size()
    .reset_index(name="movement_count")
)

pair_duration_bands["pair_total"] = (
    pair_duration_bands
    .groupby(
        ["location_1", "location_2"]
    )["movement_count"]
    .transform("sum")
)

pair_duration_bands["percentage_of_pair"] = np.where(
    pair_duration_bands["pair_total"] > 0,
    (
        pair_duration_bands["movement_count"]
        / pair_duration_bands["pair_total"]
        * 100
    ),
    np.nan
)

pair_duration_bands["percentage_of_pair"] = (
    pair_duration_bands["percentage_of_pair"].round(1)
)

print("")
print("================================================")
print("MOVEMENT TIME BANDS BY LOCATION PAIR")
print("================================================")
print(pair_duration_bands.to_string(index=False))

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

    overall_distribution.to_excel(
        writer,
        sheet_name="Overall Distribution",
        index=False
    )

    pair_distribution.to_excel(
        writer,
        sheet_name="Pair Distribution",
        index=False
    )

    overall_duration_bands.to_excel(
        writer,
        sheet_name="Overall Time Bands",
        index=False
    )

    pair_duration_bands.to_excel(
        writer,
        sheet_name="Pair Time Bands",
        index=False
    )

    distribution_fit_summary.to_excel(
        writer,
        sheet_name="Distribution Fit",
        index=False
    )

print("")
print("================================================")
print("COMPLETE")
print("================================================")
print(f"Saved to: {os.path.abspath(OUTPUT_EXCEL)}")
print("================================================")