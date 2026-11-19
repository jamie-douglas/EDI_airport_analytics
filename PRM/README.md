# PRM (Assisted Travel)

Code for passengers with reduced mobility (PRM): demand reporting, minibus and ambulift analysis, fleet optimisation, the 2026 vehicle trial, and penetration-rate forecasting.

---

# Stages

| Stage | Purpose | Main script |
|---|---|---|
| [01 Reports](01%20Reports/README.md) | PRM demand report and monthly stand report | `prm_report_v2.py` |
| [02 Minibus Analysis](02%20Minibus%20Analysis/README.md) | Vehicle efficiency, operational challenges, driver FTE, FTE model | `minibus_project.py` |
| [03 Fleet Optimisation](03%20Fleet%20Optimisation/README.md) | Monthly fleet sizing runs using `prm_opt` | `minibus_combinedW26.ipynb` |
| [04 Minibus Trial](04%20Minibus%20Trial/README.md) | Evaluation of the August 2026 vehicle trial | `prm_trial_analysis.py` |
| [05 Penetration and Demand](05%20Penetration%20and%20Demand/README.md) | PRM penetration by airline/country and forecast back-tests | `penetration_rate_analysis.py` |

---

# prm_opt/

The fleet optimisation library sits at the repo root (`prm_opt/`) so its package imports keep working. It is documented in `prm_opt/README.md` and `prm_opt/PROCESS.md`. Two pipelines live side by side:

- **Pipeline A (v1):** job-level, 15-minute buckets. Run by `run_s25.py` / `run_s26.py`.
- **Pipeline B (V2):** flight-level optimiser with P100/P90 demand. Run by `run_s25_s26_v2.py`, assumptions from `build_assumptions_v2.py`.

---

# Data sources

- `PRM.CompletedServicesByJob`: historical PRM services (Billing PRM = 1)
- `EAL.FlightPerformance` and `EAL.FlightPerformance_FutureFlights`: flights
- ECAC arrival/departure lists: `Documents/02 Restricted Data/PRM Minibuses/ECAC/` (personal data)
- PRM budget: `Documents/01 Projects/Assisted Travel (PRM)/PRM Reports/Data/PRM_Data.xlsx`
