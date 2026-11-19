# 03 Uplift and Diagnostics

## Purpose

Measures how bookings on the books turn into actual entries and exits, so the forecast can uplift visible bookings correctly. Also contains diagnostics on forecast behaviour and booking lead times.

---

## Scripts

### `fastpark_uplift_analysis.py`: recommended booking uplifts (current)

Calculates recommended uplift factors for horizons D0 to D28 from historical on-books vs actuals:

- overall, monthly, by duration bucket and implied-exit uplifts
- lead-time distributions
- entry/exit reconciliation
- copy-ready D1–D14 arrays for the forecast script

```powershell
python "FastPark/03 Uplift and Diagnostics/fastpark_uplift_analysis.py"
```

- **Inputs:** `AirportX.v_Bookings`, `FastPark.v_EntryAndExits`
- **Outputs:** `outputs/fastpark_uplift_analysis/FastPark_Uplift_Analysis_<date>.xlsx`, plus optional CSVs of the uplift tables

### `fastpark_forecast_diagnostics.py`: forecast diagnostics (earlier sibling)

Booking visibility curves D1–D28, lead-time distribution, daily to 4-weekly penetration and its volatility, a back-test of the hard-coded uplift arrays, and how forecasts move as the day approaches.

- **Inputs:** `AirportX.v_Bookings`, `FastPark.v_EntryAndExits`, `EAL.FlightPerformance`
- **Outputs:** `outputs/fastpark_forecast_diagnostics/FastPark_Forecast_Diagnostics_V2<date>.xlsx`, plus optional CSVs

### `booking_leadtime.py`: booking lead times

How far ahead customers book, by duration bucket, with last-minute booking patterns and the share of eventual actuals already booked at each lead time. Date window is hard-coded at the top.

- **Inputs:** `AirportX.v_Bookings`, `FastPark.v_EntryAndExits`
- **Outputs:** console tables and charts only

### `debug_noshow.py`: no-show investigation

Exploratory reconciliation of bookings against barrier records, focused on bookings made 0–1 days ahead.

- **Inputs:** `AirportX.v_Bookings`, `FastPark.v_EntryAndExits`, `EAL.FlightPerformance`
- **Outputs:** console only

---

## Status

`fastpark_uplift_analysis.py` supersedes `fastpark_forecast_diagnostics.py` for uplift arrays; the diagnostics script is kept for its visibility and penetration views. `debug_noshow.py` is a scratch investigation.
