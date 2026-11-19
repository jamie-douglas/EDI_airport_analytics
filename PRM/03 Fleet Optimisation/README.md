# 03 Fleet Optimisation

## Purpose

Sizes the ambulift, minibus and staff fleet month by month, using the `prm_opt` library at the repo root. Each month is run three ways:

- **S1:** policy baseline (pipeline A)
- **S2:** rule-based flight rules (`prm_opt/policy_s2_flight_rules.py`)
- **V2:** the flight-level optimiser (pipeline B), at P100 and P90 demand

---

## Notebooks

Open them with this folder as the working directory (VS Code does this by default).

| Notebook | Season | Months | Output folder |
|---|---|---|---|
| `minibus_combinedW26.ipynb` | Winter 2026/27 | Oct 2026 – Mar 2027 | `outputs/prm_w26/` |
| `minibus_combined_s1_s2_v2_new.ipynb` | Summer 2026 | Apr – Oct 2026 | `outputs/prm_combined_new/` |

Each month writes `<month>_combined.xlsx` and `<month>_v2.xlsx` to the `monthly/` subfolder.

- **Inputs:**
  - `PRM.CompletedServicesByJob`, `EAL.FlightPerformance`, `EAL.FlightPerformance_FutureFlights`
  - stand plans: `inputs/stands/stand_allocation-<month>.csv`

---

## Stand plans

The stand-allocation CSVs live in `inputs/stands/`. Both the W26 notebook (`STAND_PLAN_FILES`) and the `prm_opt` default read them from there.

**Change in November 2026:** before the restructure, both pointed at `data/stands` paths that did not resolve, so runs silently used the fallback stand distribution instead of the plans. Results from runs after this change will use the stand plans and may differ from earlier outputs.

---

## Run times

P90 fleet-expansion searches are slow. Some winter months took up to about 18 hours.

---

## Status

`minibus_combinedW26.ipynb` is current. The W26 notebook was forked from an older version and lacks the extra summary fields and REC sheets added in `_new`. Earlier notebooks are in `Archive/notebooks/`.
