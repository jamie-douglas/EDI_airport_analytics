# 05 Production Forecast

## Purpose

The live FastPark forecast: a 56-day daily and hourly forecast of entries and exits, run every day. See `../RUNBOOK.md` for day-to-day operation.

---

## Run

```powershell
powershell -ExecutionPolicy Bypass -File "FastPark/01 Short Term Model/05 Production Forecast/run_simplified_fastpark_shortterm.ps1"
```

The launcher:

1. checks or rebuilds the venv at `Documents\Venv FastPark ST model\highs`
2. runs `simplified_fastpark_shortterm.py` (the new model)
3. runs the old 2025 model, if its folder sits beside this model folder
4. runs `06 Comparison Framework/compare_live_forecasts.py`
5. on Mondays (or with `-ForceMondayTasks`): runs the forecast accuracy notebook and updates the weekly PowerPoint

---

## Files

| File | Role | Status |
|---|---|---|
| `run_simplified_fastpark_shortterm.ps1` | Daily launcher | **Active** |
| `simplified_fastpark_shortterm.py` | Production model. Settings in `PRODUCTION_CONFIG` | **Active** |
| `config/forecast_weights.csv` | Component weights per flow and horizon, read by the model | **Active** |
| `create_forecast_weights.py` | Rebuilds the weights from Gen 2 selected weights (`inputs/refined_selected_weights_backup_all_dates.csv`) | Used when refreshing weights |
| `create_forecast_parameters.py` | Builds per-horizon parameters from Gen 2/2.1 outputs | Not used by the active model |
| `fastpark_short_term_forecast_2026_v2.py` | Earlier consolidated Gen 2.1 production script | Superseded |
| `final_fastpark_short_term_model_2026` | Intermediate rewrite between v2 and simplified (no `.py` extension) | Superseded, not runnable |
| `compare_live_forecasts.py`, `fastpark_comparison.py` | Older copies of the `06 Comparison Framework` scripts | Superseded by `06` |
| `wmape.py` | Weighted-MAPE and staffing impact comparison of old vs new models | One-off evaluation |
| `flowchart TD.mmd` | Mermaid diagram of the production process | Documentation |

---

## Inputs

- `AirportX.v_Bookings` and `FastPark.v_EntryAndExits`, through `00 Utils/fastpark_local_utils.py` (DSN `AzureConnection`, your Windows username)
- `config/forecast_weights.csv`
- `07 Christmas Adjustment/christmas_forecast_adjustment.py`, when `toggle_christmas_adjustment` is on

---

## Outputs

- `outputs/fastpark_daily_forecast.csv` and `outputs/fastpark_hourly_forecast.csv`
- the legacy pack in `FastPark/Outputs/`: `Daily Forecast Output/<date>_daily_forecast.csv` (and `_DWH`), hourly entry/exit forecast and actuals CSVs, and the weekly hourly forecast
- the DWH upload copy (`toggle_dwh`) and the Workforce Management copy (`toggle_workforce`)
