## PRM Fleet Sizing and Operations Model
##### This README documents the **full PRM modelling pipeline** for Edinburgh Airport planning, covering:

This repository implements a Passenger with Reduced Mobility (PRM) planning and optimisation model to support Edinburgh Airport operational planning.


It supports:
- **S25 (historical analysis)** using realised PRM and flight performance data
- **S26 (forecast planning)** by simulating PRM demand on future flight schedules using S25-derived assumptions
- **Scenario 1 (S1): Policy baseline** – rule-based “current operation” logic
- **Scenario 2 (S2): Optimised** – Pyomo MILP model (v2) under time-bucketed constraints

The model is designed for **fleet sizing**, **hourly staffing**, and **SLA feasibility assessment** under real operational bottlenecks (lift, spin locking, time windows).

---
## 1. What this model is for

The model answers:

### Fleet sizing & staffing
- What ambulift and minibus fleet sizes are required at peak?
- What hourly staffing curves are required (drivers, vehicle agents, pushers)?

### Feasibility & performance
- Can we meet the SLA targets in peak periods?
- If not, what constraints or resources are binding (arrivals vs departures, lift, spin, vehicle capacity)?

### Planning & forecasting
- How does resource requirement change from S25 (historical) to S26 (forecast)?
- What operational changes are required to keep performance acceptable?

---
## 2. Scenarios

### Scenario 1 — Policy Baseline (S1)
S1 applies fixed, rule-based decisions (decision-tree style) to the canonical jobs table, then converts those decisions into capacity-aware vehicle and staffing curves.  
Purpose: represent “current operation logic” and provide a baseline for comparison.

### Scenario 2 — Optimised (S2, Pyomo Model v2)
S2 uses a time-bucketed MILP model to optimise mode choice and service timing under operational constraints (fleet time capacity, staff time capacity, lift bottleneck, spin locking).  
Purpose: quantify what resource levels and operating choices are required to achieve improved outcomes.

**Important:** Model v2 does **not** pool PRMs across flights (no batching across flights). That is an explicit future extension.

---
## 3. Data sources and inputs

### S25 (historical) data sources
S25 ingestion loads:
- historical PRM records (PRM.CompletedServicesByJob)
- historical flight performance (EAL.FlightPerformance)
and produces a passenger–flight PRM dataset used downstream.

### S26 (forecast) data sources
S26 ingestion loads:
- future flight schedules (EAL.FlightPerformance_FutureFlights)
and simulates PRM demand using statistical assumptions derived from S25.

### Stand plan inputs (CSV)
S26 stand assignment uses:
- deterministic stand plan CSVs (e.g., June/July) where covered
- empirical fallback stand distributions to ensure every forecast flight can be assigned a stand beyond the planned period

---
### 4. End-to-end pipeline (process map)


┌───────────────────────────────┐
│ External data sources         │
│ - Database tables (S25/S26)   │
│ - Stand plan CSVs (June/July) │
└───────────────┬───────────────┘
                │
                ▼
┌─────────────────────────────────────────┐
│ Ingestion                               │
│ - ingest_s25.ingest_s25()  (S25)        │
│ - ingest_s26.ingest_s26()  (S26)        │
└───────────────┬─────────────────────────┘
                │  produces df_prm_master
                ▼
┌─────────────────────────────────────────┐
│ Canonical job build                     │
│ - build_jobs.build_jobs()               │
└───────────────┬─────────────────────────┘
                │  produces jobs (canonical J)
      ┌─────────┴──────────┐
      │                    │
      ▼                    ▼
┌───────────────────┐  ┌───────────────────────────────┐
│ Scenario 1        │  │ Scenario 2 (Pyomo v2)         │
│ - policy_s1       │  │ - params: tau/spin/classes    │
│ - outputs baseline│  │ - debug_tools (optional)      │
└─────────┬─────────┘  │ - pyomo_model_v2.build_model  │
          │            └───────────┬───────────────────┘
          │                        │
          ▼                        ▼
┌─────────────────────────────────────────┐
│ Reporting / Outputs                     │
│ - outputs: summaries, allocations, peak │
│ - sanity checks                         │
└─────────────────────────────────────────┘
This pipeline structure is implemented in the scenario runnes run_s25.py and run_s26.py.


## 5. Assumptions window vs run window (CRITICAL)

For S26 forecasting, empirical distributions must be built on a stable *long* historical window (e.g., Apr–Oct S25), regardless of the shorter window you might optimise or report.

Assumptions built from too-small historical windows will bias:
- penetration rates
- SSR mix
- service time distributions (τ)
- stand distributions
- timing offsets

Recommended practice:
- Build S26 assumptions from a fixed long S25 range (e.g., Apr–Oct)
- Run S25/S26 optimisation on any shorter monthly windows as needed for reporting

---

## 6. Script-by-script reference (what each file does + where to tune)

### Core orchestration
#### `run_s25.py`
Runs S25 scenarios:
- `run_s25_s1(start, end, toggles)` → S1 baseline run
- `run_s25_s2_v2(...)` → S2 optimisation run
- `run_s25_s2_v2_sensitivity(...)` → sensitivity loop (e.g. vertical cycle time)
Tuning points: solver time limit, mip gap, threads, vertical cycle grid, toggles.

#### `run_s26.py`
Runs S26 scenarios:
- `run_s26_s1(...)` → baseline using simulated demand
- `run_s26_s2_v2(...)` / sensitivity wrapper → optimisation using simulated demand
Tuning points: same as S25 + S26 inputs from assumptions.

---

### Configuration and domain logic
#### `config.py`
Central configuration:
- stand sets, lift sets, penalties
- vehicle registry (vehicle models/capacities)
- planning toggles (SLA buffers, standby minutes, spill cap)
Tuning points: penalties, lift parameters, stand zoning, vehicle capacities.

#### `sector.py`
Normalises sector values so “Domestic / CTA / International” mean the same everywhere.

#### `minibus.py`
Passenger-level flags used upstream for eligibility and job classification.

---

### Ingestion
#### `ingest_s25.py`
Historical ingestion:
- loads PRM + flight data
- cleans/timestamps
- merges PRM records to flights
- constructs df_prm_master (contract aligned with S26)

Tuning points: merge logic, timestamp harmonisation, dropped/unmatched behaviour.

#### `ingest_s26.py`
Forecast ingestion:
- loads future flights
- assigns stands (deterministic + fallback)
- applies timing uncertainty (scheduled→chocks offsets, early/late)
- pairs turnarounds
- generates simulated PRM passenger jobs

Tuning points: early/late std, turnaround gap thresholds, spin window, random seed, stand assignment logic.

#### `ingest_stand_allocations.py`
Stand plan ingestion and stand distribution builder.
Tuning points: CSV schema harmonisation and CTA classification logic.

---

### S26 assumptions
#### `build_s26_assumptions.py`
Builds in-memory forecast inputs derived from S25:
- penetration rates & SSR mix
- service-time parameters (τ distributions)
- chocks offsets
- early/late variability
- fallback stand distributions
Tuning points: assumption window dates, conditioning granularity, fallback rules.

---

### Canonical jobs table
#### `build_jobs.py`
Creates the canonical jobs table (J) used by both scenarios.
This is where the model’s “truth” about each job is set:
- SLA start/deadline windows
- mode eligibility sets
- vertical requirement flags
- stand/zone mapping
Tuning points: eligibility rules, SLA window construction, vertical logic.

---

### Optimisation parameters
#### `params.py`
Builds optimisation inputs:
- τ(j,m): busy minutes per job & mode
- spin_removed[b]: ambulift minutes removed by spins
- vehicle classes from vehicle registry
Tuning points: spin lock threshold, fleet counts/capacities, τ construction assumptions.

---

### Scenario 1 policy logic
#### `policy_s1.py`
Applies S1 policy decisions to jobs.
Tuning points: rule thresholds and decision tree logic.

---

### Scenario 2 optimisation model
#### `pyomo_model_v2.py`
Builds and solves the Scenario 2 MILP.
Key decisions:
- mode selection per job
- service start bucket per job
- per-flight, per-bucket vehicle allocation
- vertical visit anchoring
- SLA breach indicator (y_j)

Key tuning levers:
- SLA penalty weight (LAMBDA)
- trip usage scaling (BIG)
- soft mode penalties
- toggles (spill cap, standby minutes)
- solver settings (time limit, mip gap)

---

### Debugging
#### `debug_tools.py`
Pre-solve feasibility checks and infeasibility diagnostics.
Use this when:
- solver returns infeasible
- results look structurally wrong
- you need to identify the binding constraint family

---

### Outputs
#### `outputs.py`
Extracts and reports:
- job assignments (S2)
- vehicle allocations (S2)
- SLA metrics (overall/arr/dep)
- peak fleet requirements and hourly curves
- sanity check tables

Tuning points: reporting aggregation choices (hour_method, day_from), baseline capacity assumptions.

---

## 7. Outputs produced

Typical outputs per run:
- `SLA_all`, `SLA_arr`, `SLA_dep`
- `PeakAmb`, `PeakMini`
- peak day and peak hour requirement tables
- hourly curves (peak day)
- solution allocations (S2) and audit/sanity tables

---

## 8. Key limitations

- No pooling across flights (no batching across flights)
- SLA is penalty-based (soft) unless explicitly implemented as a hard constraint
- Solver time limits can produce suboptimal solutions
- Some historical PRM rows may be dropped if they cannot be matched to flights

---

## 9. Stochastic scenarios (ω) in scope vs implementation

The scope document references multiple stochastic scenarios (ω) representing uncertainty (delays, congestion, downtime).  
This codebase does not implement stochastic optimisation because it would multiply variables/constraints by the number of scenarios and materially worsen solve time.

Instead, uncertainty is approximated through S26 simulation:
S25-derived distributions → simulated future demand instance → deterministic optimisation solve.

Future work could implement stochastic optimisation if computational constraints allow.
