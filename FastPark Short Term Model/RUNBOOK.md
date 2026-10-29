# FastPark Short Term Model Runbook

## Purpose

This is the operational quick guide for running the FastPark short-term production forecast, troubleshooting common issues, and understanding the full forecast process flow.

Primary audience:

- users running the model day to day
- analysts refreshing model weights

## Primary files

- 05 Production Forecast/run_simplified_fastpark_shortterm.ps1
- 05 Production Forecast/simplified_fastpark_shortterm.py
- 05 Production Forecast/create_forecast_weights.py
- 05 Production Forecast/compare_live_forecasts.py

## How to run (normal daily process)

Run this command from the repo root:

    powershell -ExecutionPolicy Bypass -File "FastPark Short Term Model/05 Production Forecast/run_simplified_fastpark_shortterm.ps1"

What the launcher does automatically:

1. checks for a local venv
2. creates or rebuilds it if missing/broken
3. installs required Python packages
4. runs the production forecast script

Venv location logic:

1. Primary: %USERPROFILE%\Documents\Venv FastPark ST model\highs
2. Fallback: 05 Production Forecast\highs (beside the launcher script)

## Required configuration before running

Update these values in PRODUCTION_CONFIG in:

- 05 Production Forecast/simplified_fastpark_shortterm.py

### Database connection

- db_username: set to your own username
- db_dsn: keep AzureConnection unless your environment requires a different DSN

### Forecast weights path

- forecast_weights_path: must point to the active forecast_weights.csv

### Output toggles (current defaults)

- save_new_outputs = True
- save_legacy_outputs = True
- toggle_workforce = True
- toggle_dwh = True
- toggle_accuracy = False
- toggle_weekly = False

Testing tip:

- turn off toggle_dwh, toggle_workforce, and other unneeded toggles for test runs

### Output paths

- daily_forecast_output_file
- hourly_forecast_output_file
- legacy_base_path

Set these to paths valid for your environment and user access rights.

## Troubleshooting

### 1) python.exe was not found on PATH

Symptom:

- launcher fails before creating venv

Fix:

1. install Python on the machine
2. ensure python.exe is available in PATH
3. rerun launcher

### 2) venv exists but is invalid

Symptom:

- launcher reports missing python.exe or pyvenv.cfg

Fix:

- rerun launcher and allow it to remove/recreate venv automatically

### 3) package import errors

Symptom:

- module not found while running production script

Fix:

1. rerun launcher to trigger package install in venv
2. confirm internet/proxy access for pip install
3. if needed, delete venv folder and rerun launcher

### 4) database connection errors

Symptom:

- fails when querying bookings/operations data

Fix:

1. ensure connected to VPN when not on site
2. verify db_username in PRODUCTION_CONFIG
3. verify db_dsn exists on local machine (ODBC DSN)
4. verify permissions for that user/DSN

### 5) forecast weights file not found

Symptom:

- production run fails loading weights

Fix:

1. verify forecast_weights_path in PRODUCTION_CONFIG
2. if weights need refresh, run:

       python "FastPark Short Term Model/05 Production Forecast/create_forecast_weights.py"

### 6) output path/file permission errors

Symptom:

- fails writing CSVs or legacy outputs

Fix:

1. Ensure files are not currently open
2. verify output folders exist
2. verify write access to configured paths
3. turn off unneeded output toggles for testing

### 7) script works in one location but not another

Symptom:

- run succeeds in old location but fails after moving folder

Fix:

1. update output paths and forecast_weights_path for new location
2. verify all four active production files moved together
3. verify external utilities are available if still importing from shared modules

## Detailed process map (forecast pipeline)

This map describes the production forecast execution inside:

- 05 Production Forecast/simplified_fastpark_shortterm.py

### Phase A: Bootstrap and validation

1. Load PRODUCTION_CONFIG.
2. Resolve run date and horizon settings.
3. Create SQL engine using configured DSN and username.
4. Validate critical paths and required settings.

### Phase B: Data extraction and cleaning

1. Load bookings data from source systems.
2. Load operations entry/exit actuals.
3. Clean bookings data (status filters, timestamps, durations).
4. Clean operations data and align event timestamps.
5. Reconcile booking records with operational records.

### Phase C: Feature preparation

1. Build historical booking visibility structures.
2. Build duration behavior features for exit modeling.
3. Build weekday/month and trend support features.
4. Prepare horizon-level feature sets for entry and exit forecasts.

### Phase D: Weights application

1. Load forecast_weights.csv from forecast_weights_path.
2. Select weights by demand_type and horizon_days.
3. Apply shrinkage and weighted component blending.
4. Generate non-negative daily entry and exit forecasts.

### Phase E: Hourly disaggregation

1. Convert daily forecasts to hourly using historical profiles.
2. Produce hourly entry and exit output tables.

### Phase F: Output export

1. Save new-model outputs if save_new_outputs is enabled.
2. Save legacy-format outputs if save_legacy_outputs is enabled.
3. Run optional output packs using toggle_workforce, toggle_dwh, toggle_accuracy, toggle_weekly.

### Phase G: Post-run validation

1. Confirm output files exist in configured destinations.
2. Review high-level totals and spot-check first rows.
3. Optionally run live comparison script:

       python "FastPark Short Term Model/05 Production Forecast/compare_live_forecasts.py"

## Refresh workflow (when re-training/re-calibrating)

Use this path when upstream behavior changed and you want updated production weights.

1. python "FastPark Short Term Model/01 Historical Analysis/fastpark_forecast_analysis.py"
2. python "FastPark Short Term Model/02 Simulation Gen 1/fastpark_forecast_simulation.py"
3. python "FastPark Short Term Model/03 Simulation Gen 2/fastpark_forecast_simulation_refined.py"
4. python "FastPark Short Term Model/05 Production Forecast/create_forecast_weights.py"
5. rerun launcher for production outputs

## Change control recommendation

When moving this folder away from the current repo:

1. move all active production files together
2. migrate any external utility dependencies or inline them locally
3. test with DWH/workforce toggles off first
4. then enable full output set once base run is stable
