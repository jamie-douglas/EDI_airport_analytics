import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))  # repo root, so `modules` imports
from modules.paths import FASTPARK_MOVEMENTS_DIR
import numpy as np
import pandas as pd


INPUT_CSV = FASTPARK_MOVEMENTS_DIR / "movements_YTD_26.csv"
OUTPUT_DIR = pathlib.Path(__file__).resolve().parent / "outputs" / "movement_transaction_analysis"
OUTPUT_XLSX = OUTPUT_DIR / "movement_transaction_productivity_summary.xlsx"


def extract_location(value):
    if pd.isna(value):
        return np.nan
    return str(value).split("/")[0].strip()


def normalise_time_string(value):
    if pd.isna(value):
        return np.nan

    value = str(value).strip()
    if ":" not in value:
        return np.nan

    parts = value.split(":")
    if len(parts) != 2:
        return np.nan

    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return np.nan

    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return np.nan

    return f"{hour:02d}:{minute:02d}"


def build_base_dataframe(csv_path):
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip().str.lower()

    required = ["date", "time", "driver", "booking ref", "from", "to"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["from_clean"] = df["from"].apply(extract_location)
    df["to_clean"] = df["to"].apply(extract_location)

    from_norm = df["from_clean"].astype(str).str.strip().str.lower()
    to_norm = df["to_clean"].astype(str).str.strip().str.lower()
    df["from_norm"] = from_norm
    df["to_norm"] = to_norm

    df["same_location"] = from_norm == to_norm
    df["is_return_to_return"] = (
        from_norm.str.startswith("returns") & to_norm.str.startswith("returns")
    )

    time_text = df["time"].astype(str).str.strip()

    range_times = time_text.str.extract(r"(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})")
    single_times = time_text.str.extract(r"^(\d{1,2}:\d{2})$")[0]

    df["start_time"] = range_times[0].fillna(single_times).apply(normalise_time_string)
    df["end_time"] = range_times[1].fillna(single_times).apply(normalise_time_string)

    df["start_dt"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["start_time"],
        dayfirst=True,
        errors="coerce",
    )
    df["end_dt"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["end_time"],
        dayfirst=True,
        errors="coerce",
    )

    overnight = df["end_dt"] < df["start_dt"]
    df.loc[overnight, "end_dt"] = df.loc[overnight, "end_dt"] + pd.Timedelta(days=1)

    df["has_valid_datetime"] = df["start_dt"].notna() & df["end_dt"].notna()
    df["has_from_to"] = df["from_clean"].notna() & df["to_clean"].notna()

    # "Clearly not movements": missing times, missing locations, or same location.
    df["is_clear_movement"] = (
        df["has_valid_datetime"]
        & df["has_from_to"]
        & (~df["same_location"])
    )

    df["booking_ref_clean"] = (
        df["booking ref"].astype(str).str.strip().replace({"": np.nan, "nan": np.nan})
    )

    df["date_only"] = df["start_dt"].dt.date
    df["hour"] = df["start_dt"].dt.hour
    df["year_month"] = df["start_dt"].dt.to_period("M").astype(str)

    return df


def summarise_transactions_and_movements(frame, include_return_to_return):
    if include_return_to_return:
        use = frame[frame["is_clear_movement"]].copy()
        scenario = "including_return_to_return"
    else:
        use = frame[(frame["is_clear_movement"]) & (~frame["is_return_to_return"])].copy()
        scenario = "excluding_return_to_return"

    total_movements = len(use)
    unique_transactions = use["booking_ref_clean"].nunique(dropna=True)

    movements_per_transaction = (
        total_movements / unique_transactions if unique_transactions else np.nan
    )

    overall = pd.DataFrame(
        [
            {
                "scenario": scenario,
                "movements": total_movements,
                "unique_booking_refs": unique_transactions,
                "avg_movements_per_booking_ref": round(movements_per_transaction, 4)
                if pd.notna(movements_per_transaction)
                else np.nan,
            }
        ]
    )

    monthly = (
        use.groupby("year_month", dropna=True)
        .agg(
            movements=("booking_ref_clean", "size"),
            unique_booking_refs=("booking_ref_clean", "nunique"),
        )
        .reset_index()
    )

    monthly["avg_movements_per_booking_ref"] = np.where(
        monthly["unique_booking_refs"] > 0,
        monthly["movements"] / monthly["unique_booking_refs"],
        np.nan,
    )
    monthly["scenario"] = scenario

    return scenario, use, overall, monthly


def build_movements_per_hour(use, scenario):
    driver_hour_detail = (
        use.groupby(["driver", "date_only", "year_month", "hour"], dropna=True)
        .size()
        .reset_index(name="movements")
    )
    driver_hour_detail["scenario"] = scenario

    overall_hourly = (
        driver_hour_detail.groupby("hour", dropna=True)
        .agg(
            avg_movements_per_driver_hour=("movements", "mean"),
            median_movements_per_driver_hour=("movements", "median"),
            active_driver_hours=("movements", "size"),
        )
        .reset_index()
    )
    overall_hourly["scenario"] = scenario

    monthly_hourly = (
        driver_hour_detail.groupby(["year_month", "hour"], dropna=True)
        .agg(
            avg_movements_per_driver_hour=("movements", "mean"),
            median_movements_per_driver_hour=("movements", "median"),
            active_driver_hours=("movements", "size"),
        )
        .reset_index()
    )
    monthly_hourly["scenario"] = scenario

    monthly_summary = (
        driver_hour_detail.groupby("year_month", dropna=True)
        .agg(
            total_movements=("movements", "sum"),
            active_driver_hours=("movements", "size"),
        )
        .reset_index()
    )
    monthly_summary["system_movements_per_driver_hour"] = np.where(
        monthly_summary["active_driver_hours"] > 0,
        monthly_summary["total_movements"] / monthly_summary["active_driver_hours"],
        np.nan,
    )
    monthly_summary["scenario"] = scenario

    return driver_hour_detail, overall_hourly, monthly_hourly, monthly_summary


def build_return_ev_summary(use, scenario):
    return_ev_flag = (
        use["from_norm"].str.startswith("returns ev")
        | use["to_norm"].str.startswith("returns ev")
    )

    return_ev_rows = use[return_ev_flag].copy()
    return_ev_booking_refs = return_ev_rows["booking_ref_clean"].dropna().unique()

    movements_for_return_ev_bookings = use[
        use["booking_ref_clean"].isin(return_ev_booking_refs)
    ].copy()

    unique_return_ev_refs = len(return_ev_booking_refs)
    return_ev_movements = len(return_ev_rows)
    total_movements_for_refs = len(movements_for_return_ev_bookings)

    overall = pd.DataFrame(
        [
            {
                "scenario": scenario,
                "unique_return_ev_booking_refs": unique_return_ev_refs,
                "return_ev_movements": return_ev_movements,
                "avg_return_ev_movements_per_booking_ref": (
                    round(return_ev_movements / unique_return_ev_refs, 4)
                    if unique_return_ev_refs
                    else np.nan
                ),
                "total_movements_for_return_ev_booking_refs": total_movements_for_refs,
                "avg_total_movements_per_return_ev_booking_ref": (
                    round(total_movements_for_refs / unique_return_ev_refs, 4)
                    if unique_return_ev_refs
                    else np.nan
                ),
            }
        ]
    )

    def _monthly_metrics(group):
        monthly_flag = (
            group["from_norm"].str.startswith("returns ev")
            | group["to_norm"].str.startswith("returns ev")
        )
        monthly_return_ev_rows = group[monthly_flag]
        monthly_refs = monthly_return_ev_rows["booking_ref_clean"].dropna().unique()
        monthly_total_for_refs = group[group["booking_ref_clean"].isin(monthly_refs)]

        unique_refs = len(monthly_refs)
        return pd.Series(
            {
                "unique_return_ev_booking_refs": unique_refs,
                "return_ev_movements": len(monthly_return_ev_rows),
                "avg_return_ev_movements_per_booking_ref": (
                    len(monthly_return_ev_rows) / unique_refs if unique_refs else np.nan
                ),
                "total_movements_for_return_ev_booking_refs": len(monthly_total_for_refs),
                "avg_total_movements_per_return_ev_booking_ref": (
                    len(monthly_total_for_refs) / unique_refs if unique_refs else np.nan
                ),
            }
        )

    monthly = (
        use.groupby("year_month", dropna=True)
        .apply(_monthly_metrics, include_groups=False)
        .reset_index()
    )
    monthly["scenario"] = scenario

    for col in [
        "avg_return_ev_movements_per_booking_ref",
        "avg_total_movements_per_return_ev_booking_ref",
    ]:
        monthly[col] = monthly[col].round(4)

    return overall, monthly


def build_key_headlines(overall_comparison, quality, monthly_system):
    incl = overall_comparison[
        overall_comparison["scenario"] == "including_return_to_return"
    ].iloc[0]
    excl = overall_comparison[
        overall_comparison["scenario"] == "excluding_return_to_return"
    ].iloc[0]

    latest_month = monthly_system["year_month"].max()
    latest_month_rows = monthly_system[monthly_system["year_month"] == latest_month]

    latest_incl = latest_month_rows[
        latest_month_rows["scenario"] == "including_return_to_return"
    ].iloc[0]
    latest_excl = latest_month_rows[
        latest_month_rows["scenario"] == "excluding_return_to_return"
    ].iloc[0]

    quality_map = dict(zip(quality["metric"], quality["value"]))

    return pd.DataFrame(
        [
            {
                "headline": "Movements (including return->return)",
                "value": int(incl["movements"]),
            },
            {
                "headline": "Movements (excluding return->return)",
                "value": int(excl["movements"]),
            },
            {
                "headline": "Unique booking refs (including return->return)",
                "value": int(incl["unique_booking_refs"]),
            },
            {
                "headline": "Unique booking refs (excluding return->return)",
                "value": int(excl["unique_booking_refs"]),
            },
            {
                "headline": "Avg movements per booking (including return->return)",
                "value": round(float(incl["avg_movements_per_booking_ref"]), 4),
            },
            {
                "headline": "Avg movements per booking (excluding return->return)",
                "value": round(float(excl["avg_movements_per_booking_ref"]), 4),
            },
            {
                "headline": "Rows removed for same-location",
                "value": int(quality_map.get("rows_same_location", 0)),
            },
            {
                "headline": f"Latest month system mph incl ({latest_month})",
                "value": round(float(latest_incl["system_movements_per_driver_hour"]), 4),
            },
            {
                "headline": f"Latest month system mph excl ({latest_month})",
                "value": round(float(latest_excl["system_movements_per_driver_hour"]), 4),
            },
        ]
    )


def print_key_headlines(headlines):
    print("\n=== KEY HEADLINES ===")
    for _, row in headlines.iterrows():
        print(f"- {row['headline']}: {row['value']}")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for old_csv in OUTPUT_DIR.glob("*.csv"):
        old_csv.unlink(missing_ok=True)

    df = build_base_dataframe(INPUT_CSV)

    quality = pd.DataFrame(
        [
            {"metric": "raw_rows", "value": len(df)},
            {"metric": "rows_with_valid_datetime", "value": int(df["has_valid_datetime"].sum())},
            {"metric": "rows_same_location", "value": int(df["same_location"].sum())},
            {"metric": "rows_return_to_return", "value": int(df["is_return_to_return"].sum())},
            {"metric": "rows_clear_movements", "value": int(df["is_clear_movement"].sum())},
        ]
    )

    outputs_overall = []
    outputs_monthly = []

    driver_hour_detail_parts = []
    overall_hourly_parts = []
    monthly_hourly_parts = []
    monthly_system_parts = []
    return_ev_overall_parts = []
    return_ev_monthly_parts = []

    scenario_frames = {}

    for include_r2r in [True, False]:
        scenario, use, overall, monthly = summarise_transactions_and_movements(
            frame=df,
            include_return_to_return=include_r2r,
        )
        outputs_overall.append(overall)
        outputs_monthly.append(monthly)
        scenario_frames[scenario] = use

        (
            detail,
            overall_hourly,
            monthly_hourly,
            monthly_system,
        ) = build_movements_per_hour(use, scenario)

        return_ev_overall, return_ev_monthly = build_return_ev_summary(use, scenario)

        driver_hour_detail_parts.append(detail)
        overall_hourly_parts.append(overall_hourly)
        monthly_hourly_parts.append(monthly_hourly)
        monthly_system_parts.append(monthly_system)
        return_ev_overall_parts.append(return_ev_overall)
        return_ev_monthly_parts.append(return_ev_monthly)

    overall_comparison = pd.concat(outputs_overall, ignore_index=True)
    monthly_comparison = pd.concat(outputs_monthly, ignore_index=True)

    monthly_comparison["avg_movements_per_booking_ref"] = (
        monthly_comparison["avg_movements_per_booking_ref"].round(4)
    )

    driver_hour_detail_all = pd.concat(driver_hour_detail_parts, ignore_index=True)
    overall_hourly_all = pd.concat(overall_hourly_parts, ignore_index=True).sort_values(
        ["scenario", "hour"]
    )
    monthly_hourly_all = pd.concat(monthly_hourly_parts, ignore_index=True).sort_values(
        ["scenario", "year_month", "hour"]
    )
    monthly_system_all = pd.concat(monthly_system_parts, ignore_index=True).sort_values(
        ["scenario", "year_month"]
    )
    return_ev_overall_all = pd.concat(return_ev_overall_parts, ignore_index=True)
    return_ev_monthly_all = pd.concat(return_ev_monthly_parts, ignore_index=True).sort_values(
        ["scenario", "year_month"]
    )

    booking_counts = []
    for scenario_name, scenario_df in scenario_frames.items():
        per_booking = (
            scenario_df.dropna(subset=["booking_ref_clean"])
            .groupby(["year_month", "booking_ref_clean"], dropna=True)
            .size()
            .reset_index(name="movement_count")
        )
        per_booking["scenario"] = scenario_name
        booking_counts.append(per_booking)

    per_booking_all = pd.concat(booking_counts, ignore_index=True).sort_values(
        ["scenario", "year_month", "movement_count"], ascending=[True, True, False]
    )

    key_headlines = build_key_headlines(
        overall_comparison=overall_comparison,
        quality=quality,
        monthly_system=monthly_system_all,
    )

    for scenario_name in ["including_return_to_return", "excluding_return_to_return"]:
        row = return_ev_overall_all[return_ev_overall_all["scenario"] == scenario_name].iloc[0]
        key_headlines = pd.concat(
            [
                key_headlines,
                pd.DataFrame(
                    [
                        {
                            "headline": f"RETURN EV unique booking refs ({scenario_name})",
                            "value": int(row["unique_return_ev_booking_refs"]),
                        },
                        {
                            "headline": f"RETURN EV avg total movements per booking ({scenario_name})",
                            "value": round(
                                float(row["avg_total_movements_per_return_ev_booking_ref"]), 4
                            )
                            if pd.notna(row["avg_total_movements_per_return_ev_booking_ref"])
                            else np.nan,
                        },
                    ]
                ),
            ],
            ignore_index=True,
        )

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        key_headlines.to_excel(writer, sheet_name="Key Headlines", index=False)
        quality.to_excel(writer, sheet_name="Data Quality", index=False)
        overall_comparison.to_excel(writer, sheet_name="Overall Txn vs Move", index=False)
        monthly_comparison.to_excel(writer, sheet_name="Monthly Txn vs Move", index=False)
        monthly_system_all.to_excel(
            writer,
            sheet_name="Monthly System MPH",
            index=False,
        )
        overall_hourly_all.to_excel(writer, sheet_name="Overall MPH by Hour", index=False)
        monthly_hourly_all.to_excel(writer, sheet_name="Monthly MPH by Hour", index=False)
        driver_hour_detail_all.to_excel(writer, sheet_name="Driver Hour Detail", index=False)
        per_booking_all.to_excel(writer, sheet_name="Movements per Booking", index=False)
        return_ev_overall_all.to_excel(writer, sheet_name="RETURN EV Overall", index=False)
        return_ev_monthly_all.to_excel(writer, sheet_name="RETURN EV Monthly", index=False)

    print_key_headlines(key_headlines)

    print("Saved workbook:", OUTPUT_XLSX)


if __name__ == "__main__":
    main()
