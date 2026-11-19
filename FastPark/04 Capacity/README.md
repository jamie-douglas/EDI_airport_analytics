# 04 Capacity

## Purpose

Capacity questions for the FastPark arrival process: how many kiosks, photobooth lanes and key lockers are needed, what a cap on peak bookings would do, and what forecast transactions mean at 15-minute level.

---

## Scripts

### `Capacity_Simulation.py`: arrival process simulation

Monte Carlo discrete-event simulation (simpy) of photobooth/ANPR lanes → check-in kiosks → hall occupancy, plus staff and key-locker returns. Compares the current setup (2 barriered photobooth lanes) with a single free-flow photobooth.

Reports jam probability, hall breaches, the minimum number of kiosks, key-locker sensitivity and a monthly breach report.

```powershell
python "FastPark/04 Capacity/Capacity_Simulation.py"
```

- **Inputs:**
  - ANPR scans: `02 Restricted Data/FastPark/ANPR Photobooth/*.csv`
  - `FastPark.v_EntryAndExits` and `FastPark.v_ForecastEntryandExits`
  - "Forecast B": `01 Projects/FastPark/Transaction Model/transaction_forecast.xlsx`
- **Outputs:** console results; a forecast cache in `.forecast_cache/` (safe to delete, it rebuilds)

### `fastpark_peak_cap_analysis_forecast.py`: peak booking cap

Models a cap of 350 booked entries per peak (03:00–05:59 and 10:00–13:59):

1. **Historical analysis:** whether peak arrivals booked an earlier, inside or later slot, by hour, weekday, duration and price.
2. **Six-week forecast:** reproduces the operational baseline and applies the cap under three scenarios (LOW_LOSS, CENTRAL, HIGH_LOSS), splitting excess volume into lost, circumvented and shifted.

- **Inputs:** `AirportX.v_Bookings`, `FastPark.v_EntryAndExits`, `EAL.FlightPerformance`, `EAL.FlightPerformance_FutureFlights`
- **Outputs:** `outputs/analysis/*.csv` and `outputs/forecast/*` (per-scenario CSVs and `all_scenarios_hourly_comparison.xlsx`)

### `transactions_breakdown.py`: monthly forecast → 15-minute profile

Breaks hard-coded monthly transaction forecasts into daily and 15-minute entry/exit profiles using the historical profile, then checks kiosk and check-in room capacity.

- **Inputs:** `FastPark.v_EntryAndExits`; monthly totals are set in the script
- **Outputs:** `01 Projects/FastPark/Transaction Model/transaction_breakdown_oct26.xlsx` (outside the repo, set in `modules/paths.py`)

---

## Status

All three are current. `Capacity_Simulation.py` replaces the earlier `fastpark_capacity_analysis.py` (now in `Archive/`).
