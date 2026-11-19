# 04 Minibus Trial

## Purpose

Evaluates the PRM vehicle trial run from 25 to 29 August 2026 (Monday–Thursday, 14:00–18:00) against a baseline period and a 4-week comparison.

---

## Script

### `prm_trial_analysis.py`

Reports:

- KPI summary
- vehicle service times and utilisation
- rolling-hour statistics and peaks
- SSR × vehicle mix
- weekday analysis

```powershell
python "PRM/04 Minibus Trial/prm_trial_analysis.py"
```

- **Inputs:** `PRM.CompletedServicesByJob`
- **Outputs:** `outputs/trial_KPIs_<run_date>_2.xlsx` (about 20 sheets)

Trial dates and the baseline window are set at the top of the script.

---

## Status

Complete for the August 2026 trial. Reusable for a future trial by changing the dates.
