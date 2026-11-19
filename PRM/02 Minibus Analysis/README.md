# 02 Minibus Analysis

## Purpose

Analysis behind the minibus/ambulift proposal: how vehicles are used today, which operational situations would challenge a minibus model, ambulift driver cover, and an FTE model comparing staffing scenarios.

---

## Scripts

### `minibus_project.py`: vehicle efficiency (S25)

Summer 2025 vehicle efficiency: per-vehicle and end-to-end service times, rolling-hour usage by vehicle type and model, peak utilisation, and a flight PRM/employee summary.

- **Inputs:** `PRM.CompletedServicesByJob`, `EAL.FlightPerformance`
- **Outputs:** console only

### `minibus_challenges.py`: operational challenge scenarios

Tests eight scenarios against S25 actuals and the S26 schedule, including United at stand 10, non-Ryanair flights at stands 15A/15B/17, gates 7/8 with more than one wheelchair PRM, domestic WCHS/WCHC, vehicle capacity breaches, flights without a jetbridge, companions, and WCHS passengers with their own chair.

- **Inputs:** `PRM.CompletedServicesByJob`, `EAL.FlightPerformance`, future flights, and `02 Restricted Data/PRM Minibuses/ECAC/ECAC Arrivals 2025.xlsx` / `ECAC Departures 2025.xlsx`
- **Outputs:** console only

### `minibus_FTE.py`: ambulift driver FTE

Assigns driver shifts (gaps of 8+ hours), then measures hourly distinct-driver cover and average and maximum concurrent drivers.

- **Inputs:** `PRM.CompletedServicesByJob`
- **Outputs:** `outputs/travel_time_summary.xlsx`

### `minibus_optimisation.ipynb`: FTE and fleet model (work in progress)

"PRM FTE & Fleet Optimisation Model: S25 Baseline to S26 Forecast". Compares Current, Relay 1:1 and Relay Optimised scenarios with a Monte Carlo FTE engine.

- **Inputs:** `PRM.CompletedServicesByJob`, `EAL.FlightPerformance`, and assumption files on the Business Planning SharePoint (`5YP/2026/5. PRM/Assumptions/input files`)
- **Outputs:** inside the notebook only

---

## Status

Analysis for the 2026 proposal. The notebook is unfinished (see its "next steps" notes). The optimisation that replaced it is in `03 Fleet Optimisation`.
