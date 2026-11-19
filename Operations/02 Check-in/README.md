# 02 Check-in

## Purpose

Check-in hall performance: desk and kiosk activity, peaks, transaction durations and penetration against departing flights.

---

## Script

### `checkin_report.py`

Covers:

- CUPPS desk transactions and SITA kiosk bag-drop events
- peak rolling hour
- desk and kiosk transaction durations
- penetration against departing flights
- an April–September side check

```powershell
python "Operations/02 Check-in/checkin_report.py" --start 2025-01-01 --end 2026-01-01 --out "Operations/02 Check-in/outputs/checkin.xlsx"
```

Without `--out` the results print to the console only.

- **Inputs:** `Reporting.CUPPS_CheckInHallTransactions_SourceData`, `SITA.v_DropOffEvent`, `EAL.FlightPerformance`
- **Outputs:** the `--out` workbook (updated in place)

---

## Status

Current.
