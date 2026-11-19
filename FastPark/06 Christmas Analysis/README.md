# 06 Christmas Analysis

## Purpose

Compares the Christmas and New Year period (Friday before Christmas to the Sunday after New Year) across every season held, and projects the current season from its booking pace.

This is the **analysis**. The adjustment that changes the production forecast for this period lives in `01 Short Term Model/07 Christmas Adjustment`.

---

## Script

### `fastpark_christmas_analysis.py`

Covers:

- actuals by own window, same date and matched weekday
- booking curves (bookings on hand at each horizon)
- lead time and pace against prior seasons
- a projection for this season
- a completeness check on early years' actuals

```powershell
python "FastPark/06 Christmas Analysis/fastpark_christmas_analysis.py"
python "FastPark/06 Christmas Analysis/fastpark_christmas_analysis.py" --as-of 2026-11-17
```

- **Inputs:** `FastPark.v_EntryAndExits` (daily entries and exits), `AirportX.v_Bookings` (live bookings)
- **Outputs (`outputs/`):**
  - `FastPark_Christmas_Analysis_<stamp>.xlsx`: styled workbook with charts
  - `01_projection.png` to `07_median_days_booked_ahead.png`

---

## Status

Current.
