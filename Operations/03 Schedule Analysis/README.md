# 03 Schedule Analysis

## Purpose

Checks a historic forecast schedule against the data warehouse's future flights, to find flights missing from either side.

---

## Script

### `schedule_analysis.py`

```powershell
python "Operations/03 Schedule Analysis/schedule_analysis.py"
```

- **Inputs:**
  - the forecast schedule CSV on the Business Planning SharePoint (`Forecasting/Forecast prep automation_test/Historic forecasts/`), path set in `CSV_PATH`
  - `EAL.FlightPerformance_FutureFlights`
- **Outputs:** `outputs/schedule_analysis.xlsx` with sheets `missing_flights_detail` and `missing_flights_summary`

---

## Status

One-off check. Change `CSV_PATH` to reconcile a different schedule file.
