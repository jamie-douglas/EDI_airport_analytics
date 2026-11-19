# 02 Reports

## Purpose

Operational reporting on FastPark activity, plus data extracts built for specific requests.

---

## Scripts

### `fastpark_report.py`: monthly FastPark report

Covers:

- monthly movements and validations
- peak days
- entry-to-exit time differences
- 5-minute arrival distribution
- check-in duration validation
- length of stay
- flight and airline attribution
- growth against the prior period

Run:

```powershell
python "FastPark/02 Reports/fastpark_report.py" --start 2026-01-01 --end 2026-11-01 --out "FastPark/02 Reports/outputs/FastPark_Report.xlsx" --plots
```

Other options: `--no-overlap` (only stays fully inside the window), `--or-events`.

- **Inputs:** `FastPark.v_EntryAndExits`, `EAL.FlightPerformance`
- **Outputs:** the `--out` workbook (updated in place, with a `__bak_` copy) and PNG charts in `outputs/plots/`

### `bookings.py`: bookings extract

A one-off pull of FastPark bookings for a fixed date window (edit the dates at the top).

- **Inputs:** `AirportX.v_Bookings`
- **Outputs:** sheet "FastPark Bookings" in `outputs/bookings.xlsx`

### `fp_data.py`: pseudo-dated dataset

Builds a calendar-shifted FastPark dataset (bookings, actuals, flights) aligned by weekday into target months, plus hourly block and lane occupancy from one week of lane movements.

- **Inputs:** `AirportX.v_Bookings`, `FastPark.v_EntryAndExits`, `EAL.FlightPerformance`, and `02 Restricted Data/FastPark/Movements/lane_movements_week.xlsx`
- **Outputs:** `outputs/fastpark_msc_vol3.xlsx`
- **Known issue:** the "clean" and "debug" exports write to the same file, so the debug version overwrites the clean one.

### `onsite_due_in_data_clean.py`: anonymise onsite / due-in exports

Combines the daily "Onsite" and "Due In" exports, scrambles booking refs and vehicle registrations, and shifts dates.

- **Inputs:** `02 Restricted Data/FastPark/Onsite and Due In/*.xlsx` (file list at the top of the script)
- **Outputs:** `outputs/combined_anonymised_parking_data.xlsx`
- **Warning:** `KEEP_ORIGINAL_*_IN_OUTPUT = True` by default, so the output still contains the original refs and registrations plus the mapping sheets. **Treat the output as personal data** unless you switch those flags off.

---

## Status

`fastpark_report.py` is the maintained report. The other three are one-off extracts.
