# 05 Movements and Productivity

## Purpose

Analysis of FastPark valet driver movements: productivity per driver and hour, travel times between car park blocks, and how movements relate to transactions.

**All movement extracts contain driver names and booking refs.** They are read from `Documents/02 Restricted Data/FastPark/Movements/`, never from this folder. Several outputs (driver-level detail) also contain driver names, so keep `outputs/` local.

---

## Scripts

| Script | What it does | Input (Restricted Data/FastPark/Movements) | Output (`outputs/`) |
|---|---|---|---|
| `movements_analysis.py` | Driver shifts, movements per hour, hourly by date and weekday, travel-time pairs, system vs driver comparison | `movements_YTD_26.csv` | `driver_shift_productivity_2026.csv`, `summary_metrics.csv`, `hourly_by_date.csv`, `hourly_by_weekday.csv`, `travel_time_summary.csv`, `driver_hourly_detail.csv`, `driver_hourly_average.csv`, `hourly_comparison_system_vs_driver.csv` |
| `movements_analysis_v2.py` | Winter 2024 vs winter 2025 productivity comparison, with check-ins from `FastPark.v_EntryAndExits` | `movements_W24.csv`, `movements_W25.csv` | `winter_productivity_comparison_v2.xlsx` |
| `movements_SS.py` | Event-day comparison: hourly movements on 22, 26 and 27 Aug 2025 vs the August average | `Aug_22.csv`, `Aug_2627.csv`, `Aug2025.csv` | `august_event_movement_hourly_comparison.xlsx` |
| `movements_transaction_productivity_analysis.py` | Transactions vs movements, movements per hour by driver and hour, movements per booking, RETURN EV summaries | `movements_YTD_26.csv` | `movement_transaction_analysis/movement_transaction_productivity_summary.xlsx` |
| `block_entry_analysis.py` | Arrivals into blocks A–Y and the Mound by month, weekday and hour | `movements_sep 25_to_26.csv` | `block_area_analysis.xlsx`, `block_area_weekday_profile.png` |
| `drive_time_matrix.py` | Block-to-block driving-time matrices, percentiles and distribution fits | `movements_july25_26.csv` | `driving_time_matrices_v2.xlsx` |
| `distance_matrix.py` | Distance matrix between ~32 FastPark locations from hard-coded coordinates | none | `carpark_distance_matrix.xlsx` |

Run any of them from the repo root, for example:

```powershell
python "FastPark/05 Movements and Productivity/movements_analysis.py"
```

---

## Updating the data

Drop a new movement export into `02 Restricted Data/FastPark/Movements/` and change the filename at the top of the script.

---

## Status

All current. `movements_analysis.py` is the main productivity analysis; the others answer specific questions.
