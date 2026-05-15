## PRM Fleet Sizing and Operations Model
##### This README documents the **full PRM modelling pipeline** for Edinburgh Airport planning, covering:

S25 (historical) and S26 (forecast) runs
 - **Scenario 1** (policy/baseline operation)
 - **Scenario 2** (optimsation-based operation - Pyomo model v2)
 - A **pipeline-style process map** from data ingestion to final outputs, including the functions invoked and outputs produced
 - A **function-by-function reference** for every script in the pipeline (what each function does, inputs/outputs, and tuning points)

 ### 1. What this model is for

 The model is designed for **fleet sizing** and **operational feasibility assessment** of PRM services under time-bucketed constraints.

 Core questions it answers:
- What fleet sizes are required (ambulifts, minibuses)?
- What hourly staffing curves are required (drivers, vehicle agents, pushers)?
- What mode decisions are used (Scenario 1 fixed policy vs Scenario 2 optimised)?
- How do outputs differ between S25 (historical) and S26(forecast)?

- Can be used for future forecasting provided historical and forecast data is available

### 2. Scenarios

##### Scenario 1 - Policy Baseline

Scenario 1 applied a fixed decision policy (decision-tree rules) to the canonical jobs table, then converts those decisions into vehicle/staffinf curves and summaries

##### Scenario 2 - Optimisation (Pyomo v2)

Scenario 2 uses the **Pyomo Model v2** to optimise mode choice and size fleet/staffing under time-bucketed capacity constraints, including lift and spin effects. 

Model v2 is explicitly described as:
- Fleet sizing and operational feasibility under time buckets
- **No pooling across flights** (This would be the future scenario 3)

### 3. Data Sources and Inputs

##### S25 (Historical) data sources

S25 ingestion pulls historical flight performance (e.g., EAL.FlightPerformance) and PRM Records (PRM.CompletedServicesByJob), thn builds a passenger-flight PRM database used downstream

##### S26 (forecast) data sources

S26 ingestion loads future schedules (e.g. EAL.FlightPerformance_FutureFlights) and simulates PRM demand using assumptions built from S25

##### Stand plan inputs (CSV)

S26 stand assignments uses June/July stand CSVs (deterministic where covered) plus empirical distributions for extrapolation beyond July. 

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

### 5. Script-by-script + function-by-function reference


