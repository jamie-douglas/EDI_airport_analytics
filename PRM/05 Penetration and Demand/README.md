# 05 Penetration and Demand

## Purpose

PRM penetration (PRM passengers per passenger) by airline and country, and back-tests of ways to forecast it.

---

## Scripts

### `penetration_rate_analysis.py`: forecast method back-test

Compares penetration forecasting methods (Country+Month, Airline+Country, Airline+Country+Month, each with and without a monthly airport uplift), measures year-on-year accuracy for 2023–2026, and recommends an uplift. Written in VS Code `# %%` cell format.

- **Inputs:** `PRM.CompletedServicesByJob` (Billing PRM = 1), `EAL.FlightPerformance`. Connects with its own pyodbc settings near the top of the file.
- **Outputs (`outputs/`):** `penetration_rate_backtest.xlsx`, plus `raw_prm.csv`, `raw_pax.csv`, `merged_flight.csv`
- **Note:** `raw_prm.csv` holds passenger IDs. Keep `outputs/` local.

### `penetration_by_country_and_airline.ipynb`: penetration tables

Monthly penetration by airline and by country over the last three years, with year-on-year comparison and a list of groups that fell.

- **Inputs:** `PRM.CompletedServicesByJob`, `EAL.FlightPerformance`
- **Outputs:** `outputs/prm/penetration.xlsx`

### `future_flights.py`: future flights loader (stub)

Loads `EAL.FlightPerformance_FutureFlights` for a date window and applies `prm_opt` stand zones and sector rules. Start of input preparation for the optimiser; writes nothing yet.

---

## Status

`penetration_rate_analysis.py` and the notebook are current. `future_flights.py` is unfinished.
