# 01 Reports

## Purpose

Regular PRM demand reporting: how many PRM passengers, against which passenger volumes and budget, and how they use vehicles and reception points.

---

## Scripts

### `prm_report_v2.py`: PRM demand report

Monthly, daily and yearly unique PRM passengers against departing and arriving passengers (penetration), with:

- comparison to budget
- ambulift usage
- landside and airside reception breakdowns
- SSR and booking-type breakdowns
- 3-year growth and peak day
- ECAC arrival/departure columns

Uses "Effective Month" logic: a PRM whose service crosses midnight or a month boundary counts in one month only, so monthly and yearly totals are not double-counted.

```powershell
python "PRM/01 Reports/prm_report_v2.py" --start 2024-01-01 --end 2026-11-01 --out "PRM/01 Reports/outputs/prm_2024_2026.xlsx"
```

`--budget` overrides the budget workbook path.

- **Inputs:** `PRM.CompletedServicesByJob`, `EAL.FlightPerformance`, and the budget workbook `01 Projects/Assisted Travel (PRM)/PRM Reports/Data/PRM_Data.xlsx`
- **Outputs:** the `--out` workbook (updated in place, with a `__bak_` copy). Includes a "PRM Debug Summary (Before vs After EM)" tab.

### `monthly_prm_stand_report.py`: PRM by stand

Ambulift usage by stand, WCHC/WCHS by stand group, focus on stands 8, 9 and 10 with passenger-level QA detail, vehicle type, SSR and stand summaries, and data quality checks.

```powershell
python "PRM/01 Reports/monthly_prm_stand_report.py" --start 2025-01-01 --end 2027-01-01
```

Other options: `--out`, `--csv-dir`, `--include-non-passenger-flights`.

- **Inputs:** `PRM.CompletedServicesByJob`, `EAL.FlightPerformance`
- **Outputs:** `outputs/monthly_prm_stand_report.xlsx` and CSVs in `outputs/monthly_prm_stand_report_csv/`

---

## Status

Both current. `prm_report_v2.py` replaces `prm_report.py` (now in `Archive/`), which used calendar-month sums.
