# FastPark

Code for the FastPark car park: the daily short-term forecast, operational reporting, booking uplift analysis, capacity modelling and valet driver productivity.

---

# Stages

| Stage | Purpose | Main script |
|---|---|---|
| [01 Short Term Model](01%20Short%20Term%20Model/README.md) | Daily 56-day entry/exit forecast (production) and the analysis behind it | `05 Production Forecast/run_simplified_fastpark_shortterm.ps1` |
| [02 Reports](02%20Reports/README.md) | Monthly operational report and data extracts | `fastpark_report.py` |
| [03 Uplift and Diagnostics](03%20Uplift%20and%20Diagnostics/README.md) | Booking uplift factors, forecast diagnostics, lead times | `fastpark_uplift_analysis.py` |
| [04 Capacity](04%20Capacity/README.md) | Kiosk/photobooth simulation, peak cap scenarios, transaction profiles | `Capacity_Simulation.py` |
| [05 Movements and Productivity](05%20Movements%20and%20Productivity/README.md) | Valet driver movements and productivity | `movements_analysis.py` |
| [06 Christmas Analysis](06%20Christmas%20Analysis/README.md) | Christmas/New Year bookings and actuals vs prior seasons | `fastpark_christmas_analysis.py` |
| [07 S26 Wash-up](07%20S26%20Wash-up/README.md) | Summer 2026 forecast accuracy wash-up | `FastPark S26 Wash-up.ipynb` |

---

# Outputs/

`FastPark/Outputs/` holds the **published forecast files** written by the production model:

- `Daily Forecast Output/<date>_daily_forecast.csv` and `<date>_daily_forecast_DWH.csv`
- `FastPark Entry/Exit Hourly Forecast Output.csv`
- `FastPark Entries/Exits Actuals Hourly.csv`
- `FastPark Hourly Forecast Output Weekly.csv`

The comparison scripts and the weekly PowerPoint update in `01 Short Term Model/06 Comparison Framework` read from here. The folder mirrors the `01 Short-Term Models/Outputs` folder on the Business Planning SharePoint.

---

# Data sources

Most stages query the data warehouse through `modules.utils` (or `00 Utils` for the production model):

- `AirportX.v_Bookings`: FastPark bookings
- `FastPark.v_EntryAndExits`: barrier entries and exits
- `EAL.FlightPerformance` and `EAL.FlightPerformance_FutureFlights`: flights

Movement extracts, ANPR scans and onsite/due-in exports contain personal data and are read from `Documents/02 Restricted Data/FastPark/`.
