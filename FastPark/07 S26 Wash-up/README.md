# 07 S26 Wash-up

## Purpose

The end-of-season review of the Summer 2026 FastPark forecast (arrivals 29 March to 24 October 2026): how accurate it was, what drove the errors, and how it compared with the R1/R3 budgets.

---

## Notebooks

Open them with this folder as the working directory (VS Code does this by default), so `inputs/` resolves.

### `FastPark S26 Wash-up.ipynb`: final narrative (current)

Sections: accuracy from first forecast to one week out, R1 budget vs actual, pricing as a driver, June vs September vs October, manual uplifts and events, 2025 vs 2026, and "the story".

- **Inputs:** `AirportX.v_Bookings`, `FastPark.v_EntryAndExits`, the daily forecast archive and R1 files on the Business Planning SharePoint
- **Outputs:** charts and tables inside the notebook only

### `Wash-up slides.ipynb`: slide exploration

Accuracy from 6 weeks to 1 week out, a monthly heatmap, R1 vs R3 vs actuals, an executive pack, pricing correlation and event overlays, and booking-timing diagnostics.

- **Inputs:** `AirportX.v_Bookings`, `FastPark.v_EntryAndExits`, the daily forecast archive on SharePoint, and `inputs/` (`R1_transaction_forecast.csv`, `R3_transaction_forecast.csv`, `hourly_entries_by_dow.csv`, `hourly_exits_by_dow.csv`)
- **Outputs:** `01 Projects/FastPark/Forecasting and Accuracy/Summer 2026 Wash-up/forecast_accuracy_<ddmmyy>.xlsx` (outside the repo)
- **Note:** the SharePoint user in cell 1 is set by hand; change it if someone else runs the notebook.

---

## Status

Season complete. `FastPark S26 Wash-up.ipynb` is the final version; `Wash-up slides.ipynb` is the working notebook behind it.
