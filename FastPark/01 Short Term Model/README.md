# FastPark Short Term Model

## Overview

This folder contains the FastPark short-term model stack from analysis through to production forecasting.

For day-to-day use, run the production launcher first and only run the upstream analysis/simulation pipeline when you want to refresh model weights.

Quick operations guide:

- RUNBOOK.md

## Production Files in Active Use

Only these production files are in the active runbook:

1. 05 Production Forecast/run_simplified_fastpark_shortterm.ps1
2. 05 Production Forecast/simplified_fastpark_shortterm.py
3. 05 Production Forecast/create_forecast_weights.py
4. 05 Production Forecast/compare_live_forecasts.py

Note: 05 Production Forecast/create_forecast_parameters.py is currently not part of the active runbook.

## Configuration You Can Change

Main configuration lives in 05 Production Forecast/simplified_fastpark_shortterm.py, inside PRODUCTION_CONFIG.

Only include/update these settings for normal operation:

### 1) Database connection

- db_username
  - Must update to your own username.

- db_dsn
  - Usually stays as AzureConnection unless your environment uses a different DSN.

### 2) Forecast weights path

- forecast_weights_path
  - Must point to the correct forecast_weights.csv used by the production run.
  - If this path is wrong, the model will not load the trained weights.

### 3) Output toggles

- save_new_outputs (default: True)
- save_legacy_outputs (default: True)
- toggle_workforce (default: True)
- toggle_dwh (default: True)
- toggle_accuracy (default: False)
- toggle_weekly (default: False)
  - Use these to switch output packs on or off depending on what you want to publish.
  - For testing, turn off DWH/workforce and any other outputs you do not need.

### 4) Output file paths

- daily_forecast_output_file
- hourly_forecast_output_file
- legacy_base_path
  - Set these to the folders/files where outputs should be written for your setup.

## Virtual Environment and Launcher

Use 05 Production Forecast/run_simplified_fastpark_shortterm.ps1 for normal runs.

What it does:

1. checks local venv health
2. creates/rebuilds venv if needed
3. installs required packages
4. runs simplified_fastpark_shortterm.py

Local venv path (per user/machine):

- Primary: %USERPROFILE%\Documents\Venv FastPark ST model\highs
- Fallback: 05 Production Forecast\highs (beside the launcher script)

Installed packages:

- numpy
- pandas
- openpyxl
- sqlalchemy
- pyodbc
- python-dotenv

## Run Main Forecast First

Recommended command:

powershell -ExecutionPolicy Bypass -File "FastPark Short Term Model/05 Production Forecast/run_simplified_fastpark_shortterm.ps1"

This is the primary operational command for day-to-day forecasting.

## If You Want to Re-run Analysis and Refresh Weights

Use this 4-step refresh path.

1. Re-run historical analysis
	 - python "FastPark Short Term Model/01 Historical Analysis/fastpark_forecast_analysis.py"

2. Re-run Generation 1 simulation
	 - python "FastPark Short Term Model/02 Simulation Gen 1/fastpark_forecast_simulation.py"

3. Re-run Generation 2 simulation
	 - python "FastPark Short Term Model/03 Simulation Gen 2/fastpark_forecast_simulation_refined.py"

4. Rebuild production weights
	 - python "FastPark Short Term Model/05 Production Forecast/create_forecast_weights.py"

After step 4, run the production launcher again.

Optional validation:

- python "FastPark Short Term Model/05 Production Forecast/compare_live_forecasts.py"

## Folder Structure

FastPark Short Term Model/
- 01 Historical Analysis/
- 02 Simulation Gen 1/
- 03 Simulation Gen 2/
- 04 Simulation Gen 2.1/
- 05 Production Forecast/
- 06 Comparison Framework/
- Archive/
- config/
- data/
- outputs/