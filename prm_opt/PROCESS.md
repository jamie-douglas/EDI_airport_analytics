# PRM OPT Process Walkthrough

This document is the detailed, code-aligned walkthrough of the full process under prm_opt.

It explains:
- what each stage creates,
- how data flows from ingestion to optimisation,
- how Scenario 1 and Scenario 2 differ,
- how monthly wrappers behave (including p90/p100 compare mode),
- and what the Pyomo model is actually doing.


## 0. One-Page Guide

### 0.1 What this process does

The model plans PRM resources across 15-minute buckets and tests whether the operation can meet service-level expectations under forecast demand.

In plain terms, it answers:
1. How many resources are needed?
2. When are they needed?
3. Can the plan meet the SLA target?
4. What changes if demand is higher than baseline?

### 0.2 What goes in

Core inputs:
1. Flight schedules and stand context.
2. PRM demand assumptions (penetration, SSR mix).
3. Service-time assumptions (tau by mode).
4. Operational policy choices from PlanningToggles.

### 0.3 What comes out

Core outputs:
1. Resource requirements by time bucket (Ambulift, Minibus, staffing).
2. SLA performance diagnostics.
3. Peak requirement indicators.
4. Monthly summaries (single mode or p90/p100 compare mode).

### 0.4 Headline KPI definitions and interpretation

Primary fields used in outputs:
1. sla_percent: Percent of jobs delivered within SLA. Higher is better.
2. allowed_breaches: Maximum SLA breaches tolerated by target policy.
3. actual_breaches: Breaches produced by the solution.
4. sla_floor_slack: Extra relaxation used when a hard SLA floor is enabled and otherwise infeasible.
5. PeakAmb: Peak concurrent ambulift requirement.
6. PeakMini: Peak concurrent minibus requirement.
7. PeakDrivers: Peak concurrent driver requirement.

Interpretation pattern:
1. Green: actual_breaches <= allowed_breaches and sla_floor_slack = 0.
2. Amber: Model solves but uses sla_floor_slack or sits close to the breach limit.
3. Red: Infeasible solve, no usable summary, or repeated SLA underperformance.

### 0.5 Decision framing for operations

Use results to decide:
1. Whether current fleet/staff is sufficient for expected demand.
2. Which periods are operationally highest risk.
3. The uplift needed between baseline demand and higher-demand risk case.


## 0B. Operator Quick Guide

### 0B.1 Before running

Checklist:
1. Confirm PlanningToggles.demand_mode aligns with the planning question.
2. Confirm SLA settings (sla_target_rate, enforce_hard_sla_floor, allow_sla_floor_slack).
3. Confirm stand logic assumptions are current.

### 0B.2 Standard run sequence

1. Build or load assumptions (S26 path) where required.
2. Ingest source data into df_prm_master.
3. Build canonical jobs table via build_jobs.
4. Run Scenario 1 (policy baseline) or Scenario 2 (optimisation).
5. Extract summary and hourly diagnostics.

### 0B.3 Immediate post-run checks

1. Solver status/termination is acceptable for use.
2. summary exists and includes SLA diagnostics.
3. actual_breaches vs allowed_breaches is understood.
4. sla_floor_slack is checked when hard floor mode is enabled.
5. Mode-tagged hourly files are produced in compare runs.

### 0B.4 Reporting cadence

For daily/weekly planning packs, publish:
1. Headline SLA diagnostics (sla_percent, allowed_breaches, actual_breaches).
2. Peak resource metrics (PeakAmb, PeakMini, PeakDrivers).
3. p90 vs p100 delta where compare mode is active.


## 0C. Troubleshooting Playbook

### 0C.1 No summary or failed run

Checks:
1. Review solver status/termination and mip gap/time-limit context.
2. Confirm whether hard constraints are over-tight (SLA floor, fleet caps, stand rules).
3. Re-run with diagnostics to inspect capacity and impossible-window warnings.

### 0C.2 SLA unexpectedly low

Checks:
1. Verify sla_target_rate and whether hard floor/slack settings are active.
2. Validate service-time and demand assumptions.
3. Check if demand_mode changed to a higher-risk profile.

### 0C.3 Monthly peak and hourly export do not match

Checks:
1. Distinguish peak-jobs day from true peak-resource day.
2. Use the true peak-ambulift-day hourly export for resource peak reconciliation.
3. Confirm file mode tags to avoid mixing p90 and p100 artefacts.

### 0C.4 Unexpected remote stand behavior

Checks:
1. Validate stand mapping inputs.
2. Confirm IsEffectiveRemote derivation is as expected.
3. Confirm no_push_remote_stands behavior is active in Scenario 2.

### 0C.5 Compare-mode confusion

Checks:
1. Ensure both modes were executed for the same period.
2. Compare only mode-suffixed columns and mode-tagged files.
3. Report deltas rather than absolute values alone.


## 0D. Compact Glossary

1. SLA: Service-level requirement for on-time completion.
2. sla_target_rate: Target on-time proportion used to derive allowed breaches.
3. allowed_breaches: Maximum breaches tolerated by policy target.
4. actual_breaches: Breaches in the current solution.
5. sla_percent: Achieved on-time percentage.
6. enforce_hard_sla_floor: Toggle to enforce a hard minimum SLA condition.
7. allow_sla_floor_slack: Toggle to permit controlled slack when hard floor would be infeasible.
8. sla_floor_slack: Magnitude of SLA floor relaxation used by the model.
9. demand_mode p100: Baseline full-demand mode.
10. demand_mode p90_stratified: Demand-shaped mode using stratified percentile capping.
11. compare mode: Monthly wrapper behavior that runs p90 and p100 side by side.
12. canonical jobs: Standardised job table produced by build_jobs and consumed by S1/S2.
13. IsEffectiveRemote: Derived remote-stand indicator used in operations/model rules.
14. PeakAmb: Peak concurrent ambulift requirement.
15. PeakMini: Peak concurrent minibus requirement.
16. PeakDrivers: Peak concurrent driver requirement.
17. feasible: A solution exists under active constraints.
18. optimal: Best solution proven within solver tolerance.
19. time-limited solve: Usable incumbent found before full optimality proof.
20. pre_solve_debug: Necessary-condition diagnostics before optimisation solve.


## 1. Folder-Level Purpose

The prm_opt package implements a planning and optimisation pipeline for PRM operations.

At a high level:
1. Build demand and operational context (S25 historical or S26 forecast).
2. Convert to one canonical jobs table.
3. Run either:
   - Scenario 1 (policy baseline), or
   - Scenario 2 (Pyomo MILP optimisation).
4. Extract KPIs, allocations, diagnostics, and peak-day/hourly artefacts.


## 2. Main Entry Points

Primary run entry points:
- run_s25.py
- run_s26.py

Core shared stages:
- ingest_s25.py / ingest_s26.py
- build_jobs.py
- params.py
- pyomo_model_v2.py
- outputs.py

Support and diagnostics:
- build_s26_assumptions.py
- ingest_stand_allocations.py
- debug_tools.py
- config.py
- policy_s1.py


## 3. Configuration Layer (config.py)

PlanningToggles controls behavior without changing model code.

Key groups:
1. Time and SLA controls
   - sla_buffer_mins
   - max_late_mins
   - sla_target_rate
   - enforce_hard_sla_floor
   - allow_sla_floor_slack
   - obj_sla_floor_slack_weight

2. Operational timing controls
   - preposition_arrival_mins
   - preposition_departure_mins
   - standby_dep_vert_mins
   - standby_arr_horiz_mins
   - vertical_cycle_mins
   - spill_bucket_cap

3. Demand scenario controls
   - demand_mode: p100 or p90_stratified
   - run_p100_risk_check_with_p90
   - p90_quantile
   - p90_strata

4. Fleet registry and domain constants
   - VEHICLE_MODELS (current and optional future vehicles)
   - stand/zone logic
   - lift constraints
   - airline exceptions

These toggles propagate through ingestion/job build/model build and monthly wrappers.


## 4. S25 Pipeline (Historical)

### 4.1 Ingest (ingest_s25.py)

ingest_s25(start, end) builds df_prm_master from historical PRM jobs + flight performance.

What happens:
1. Load flight-level history (load_flight_data).
2. Load PRM service segments (load_prm_data).
3. Compute SSR severity numeric and empirical segment durations.
4. Build mode-specific tau medians (tau_amb_mins, tau_mini_mins, tau_push_mins).
5. Build passenger-level flags (via passenger_level_flags).
6. Assign own-chair flags (WCHC fixed; WCHS probabilistic).
7. Aggregate segment rows to passenger-flight rows.
8. Merge passenger rows with flight context.
9. Derive turnaround and stress features.

Primary output created:
- df_prm_master (historical passenger-flight records with operational and tau columns).


### 4.2 Canonical job build (build_jobs.py)

build_jobs(df_prm_master, toggles=...) transforms ingest output into the canonical optimisation jobs table.

Key construction steps:
1. Defensive fills and schema harmonisation.
2. Time anchors and buckets:
   - release_time,
   - t bucket (release),
   - s bucket (scheduled/service).
3. SLA anchors:
   - sla_start_time,
   - sla_limit,
   - hard_deadline_time.
4. Domain flags:
   - needs_wc,
   - needs_vertical,
   - safety_stand,
   - lift_gate,
   - is_spin.
5. Create stable flight_key.
6. Apply demand mode behavior:
   - p100: no demand shaping,
   - p90_stratified: per-flight, per-stratum cap using quantile and deterministic retention priority.
7. Ensure tau columns exist.

Primary output created:
- jobs DataFrame indexed by j (canonical model input contract).


### 4.3 Scenario 1 baseline (run_s25_s1 in run_s25.py)

Flow:
1. ingest_s25 -> df_prm_master
2. build_jobs -> jobs
3. apply_policy_s1 -> fixed S1 decision per job
4. baseline_s1_vehicle_curves_capacity -> time-based resource curves
5. baseline_s1_summary -> KPI summary

Primary outputs created:
- jobs
- summary
- curve tables (ambulift/minibus/driver/veh_agent/pusher)
- fb_detail

No optimisation solve in Scenario 1.


### 4.4 Scenario 2 optimisation (run_s25_s2_v2 in run_s25.py)

Flow:
1. ingest_s25
2. build_jobs
3. build_tau_from_jobs / build_vehicle_classes / build_spin_minutes
4. pre_solve_debug (necessary-condition checks)
5. build_pyomo_model
6. solve (HiGHS)
7. extract_summary / extract_job_assignments / extract_vehicle_allocations / run_sanity_checks

Primary outputs created:
- model solve metadata (status, tc, st)
- summary KPIs
- job_assignments
- vehicle_allocations
- sanity_checks


### 4.5 S25 monthly wrapper (run_month_s25)

Default behavior:
- Runs Scenario 1 monthly and returns compact monthly KPIs.

Compare behavior (toggle-driven):
- If demand_mode=p90_stratified and run_p100_risk_check_with_p90=True,
  run_month_s25 executes both p90 and p100 and returns side-by-side columns with _p90/_p100 suffixes.

Also creates monthly hourly CSV (S1 peak-jobs day) with mode-tagged filename.


## 5. S26 Pipeline (Forecast)

### 5.1 Assumption build (build_s26_assumptions.py)

build_s26_assumptions(...) creates the S26 input bundle from S25 calibration windows.

What it builds:
1. Penetration and SSR mix.
2. Service-time parameters.
3. Mode tau parameters.
4. Scheduled->chocks offset distributions.
5. Early/late delay statistics.
6. Stand distributions (from plans + fallback).

Primary output created:
- assumptions dict with all inputs consumed by ingest_s26/run_s26_*


### 5.2 Forecast ingest (ingest_s26.py)

ingest_s26(...) simulates passenger-level PRM rows from future flight schedules and assumptions.

Main operations:
1. Load future flights.
2. Normalize fields (airline/sector/dir/pax).
3. Assign stand per flight:
   - exact from stand plan if available,
   - fallback probabilistic by airline/dir/sector hierarchy.
4. Sample schedule->chocks offsets and early/late effects.
5. Compute concurrent stress from chocks estimates.
6. Pair turnarounds.
7. Generate PRM passengers from penetration rates.
8. Assign SSR mix and own-chair probabilities.
9. Attach mode tau parameters and required context fields.
10. Compute IsEffectiveRemote from stand rules + no-jetbridge airline logic.

Primary output created:
- df_prm_master (forecast passenger-flight records compatible with build_jobs contract).


### 5.3 Scenario 1 and Scenario 2 (run_s26.py)

run_s26_s1:
- ingest_s26 -> build_jobs -> apply_policy_s1 -> baseline curves and summary.

run_s26_s2_v2:
- ingest_s26 -> build_jobs -> params -> pre_solve_debug -> build_pyomo_model -> solve -> extract outputs.

Same structural pattern as S25 after ingestion stage.


### 5.4 S26 monthly wrapper (run_month_s26)

run_month_s26 has date-sensitive source logic:
1. Historical-only month branch uses S25 runners.
2. Forecast-only month branch uses S26 runners.
3. Split month branch uses forecast section from cutoff onward.

Demand compare behavior:
- If demand_mode=p90_stratified and run_p100_risk_check_with_p90=True,
  it executes both p90 and p100 and returns compact compare columns (_p90/_p100).

Hourly exports created:
1. Peak-jobs-day hourly table (mode-tagged filename).
2. True peak-ambulift-day hourly table (S2, mode-tagged filename).

This separation is important because peak PRM jobs day and true peak ambulift concurrency day can differ.


## 6. Parameter Builders (params.py)

### 6.1 build_tau_from_jobs
Creates tau[(j, mode)] busy minutes dictionary from job-level tau columns.

### 6.2 build_spin_minutes
Computes spin-locked ambulift minutes removed per bucket.
Current implementation counts spin by flight-level event to avoid over-locking per passenger.

### 6.3 build_vehicle_classes
Builds model classes from VEHICLE_MODELS.
Supports include_future to include optional future fleet classes.

Primary outputs created:
- tau dict
- spin_removed dict
- vehicle classes structure consumed by model.


## 7. Scenario 1 Policy Rules (policy_s1.py)

apply_policy_s1(jobs) applies the learned decision-tree style rule set to each job.

Output created:
- decisions mapping j -> PassengerType-style decision label.

This module is deterministic and intentionally verbose to preserve trained rule structure.


## 8. Pyomo Model (pyomo_model_v2.py)

build_pyomo_model(jobs, tau, spin_removed, toggles) is the Scenario 2 core MILP.

### 8.1 Core sets and sparse index design
1. J: jobs
2. F: flights
3. B: time buckets
4. JB: feasible job-bucket pairs
5. FB: feasible flight-bucket pairs

JB and FB sparsity reduces model size by avoiding impossible pairs.

### 8.2 Key decision variables
1. Passenger-level decisions
   - x[j,m]: mode choice (Amb/Mini/Push)
   - A[j,b]: service start bucket
   - y[j]: SLA breach indicator
2. Linearisation helpers
   - U[j,b,m] = A AND x
3. Flight/bucket deployment
   - k[(type,class),(f,b)] vehicles allocated
   - V[f,b] vertical visit anchors
4. Standby and indicator variables
   - Z_* and W_* binaries for combined-operation standby logic
5. Staffing
   - H_drv, H_vehag, H_push by bucket

### 8.3 Objective composition
Objective combines:
1. Trip/vehicle deployment cost
2. Soft mode penalties (e.g., push, amb horizontal, transfer)
3. SLA breach penalty
4. SLA excess-over-target penalty
5. Optional hard-floor slack penalty
6. Staff regularisation
7. Vehicle capex term

### 8.4 SLA mechanism
1. Per-job SLA delay constraint with Big-M and y[j].
2. allowed_breaches derived from sla_target_rate and job count.
3. Optional hard SLA floor:
   - strict floor, or
   - floor plus slack variable if enabled.

### 8.5 Constraint families
Main constraint groups:
1. passenger timing and serve-once
2. eligibility and policy hard rules
3. vertical visit anchoring
4. standby indicator logic
5. seat and wheelchair capacity
6. fleet exclusivity and total minibus cap
7. ambulift/minibus time-cap constraints with spill vectors and spin removal
8. staffing aggregation and limits
9. lift bottleneck capacity

Important hard rule present:
- no_push_remote_stands: Push disallowed on remote stands.


## 9. Diagnostics and Feasibility Tools (debug_tools.py)

pre_solve_debug(...) runs fast necessary-condition checks before solve.

What it checks:
1. Structural schema and missingness.
2. Bucket horizon coverage.
3. Deadline/SLA impossible-at-release conditions.
4. Spin saturation and lock sanity.
5. Coarse capacity feasibility checks (amb/mini/lift).
6. Tight-window checks and other quick infeasibility signals.

These checks are necessary-condition checks, not proof of feasibility.


## 10. Reporting and Extraction (outputs.py)

### 10.1 extract_job_assignments
One row per job with chosen mode, service bucket, SLA breach flag, and audit fields.

### 10.2 extract_vehicle_allocations
Sparse extraction of positive k allocations by vehicle type/class/flight/bucket.

### 10.3 extract_summary
Builds KPIs including:
- SLA_all / SLA_arr / SLA_dep
- allowed_breaches / actual_breaches
- sla_percent / sla_floor_slack
- PeakAmb / PeakMini / PeakDrivers
- current fleet and gap metrics

### 10.4 peak-day reports
- peak_day_hourly_fleet_report computes hourly Amb/Mini requirement and gaps for chosen peak day.
- print_run_report and print_run_report_s1 format human-readable run diagnostics.


## 11. Legacy and Utility Scripts

Legacy model files retained for reference:
- pyomo_model.py
- pyomo_model_old.py
- pyomo_model_legacy.py

Helper build scripts (one-off or standalone generation):
- build_penetration_and_ssr_mix.py
- build_service_time_params.py
- build_stand_fallback_distribution.py
- build_chocks_offset_params.py
- build_early_late_params.py

These overlap conceptually with build_s26_assumptions.py but can be used independently.


## 12. Practical Notebook Execution Pattern

Typical monthly notebook flow:
1. Build toggles (including demand_mode and risk-check toggle if needed).
2. Build S26 assumptions once from stable historical window.
3. Run monthly loops:
   - run_month_s25(...)
   - run_month_s26(...)
4. Export DataFrames to CSV.
5. Use generated hourly CSV artefacts for deep-dive checks.

If compare mode is enabled, monthly rows contain paired p90/p100 metrics in compact suffix form.


## 13. What Is Created at Each Stage (Quick Index)

1. ingest_s25 / ingest_s26
   - df_prm_master
2. build_jobs
   - jobs (canonical optimisation table)
3. params
   - tau, spin_removed, classes
4. pyomo_model_v2
   - ConcreteModel with all variables/constraints/objective
5. solve + extraction
   - summary, job_assignments, vehicle_allocations, sanity_checks
6. monthly wrappers
   - monthly KPI dictionaries (single mode or compare mode)
   - mode-tagged hourly CSV exports


## 14. Current Behavioral Defaults

If you do nothing special:
1. demand_mode defaults to p100.
2. run_p100_risk_check_with_p90 defaults to False.
3. monthly wrappers run single-mode outputs only.
4. hard SLA floor is off unless explicitly enabled in toggles.

This means baseline behavior remains standard and opt-in features activate only when toggled.
