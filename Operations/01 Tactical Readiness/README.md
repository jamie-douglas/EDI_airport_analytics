# 01 Tactical Readiness

## Purpose

The tactical readiness pack: looks ahead over the next few weeks and the summer season to flag busy days, security peaks and immigration overflow.

---

## Script

### `tactical_readiness.py`

For each forward window (default 2 and 4 weeks, plus the summer season):

- daily arrivals/departures summary with A/B/C day classification
- arrivals per hour and per immigration slot
- security rolling-hour peak and utilisation against capacity
- immigration queue model with overflow ("DownHall") windows
- a summary across windows

```powershell
cd "Operations/01 Tactical Readiness"
python tactical_readiness.py --outdir outputs/2026-11-19 --print-saves --csv
```

Options:

- `--weeks "2,4"`: forward windows in weeks
- `--no-summer`, `--summer-start`, `--summer-end`: the summer window
- `--imm-slot 15`: immigration slot size in minutes
- `--a-threshold`, `--b-threshold`: override the summer-derived A/B day thresholds
- `--outdir`: where charts and CSVs go. **No files are saved unless you set it.** Use a dated folder under `outputs/`, as past runs did.

- **Inputs:** `EAL.FlightPerformance_FutureFlights`, `Planning.V_OperationsForecast`
- **Outputs:** `<outdir>/tactical/plots/<start>_to_<end>/` with `daily.png`, `weekly_abc.png`, `peak_security.png`, `immigration.png`, `immigration_overflow_windows.png`, and CSVs with `--csv`

---

## Status

Current. Replaces `tactical.py` (now in `Archive/`).
