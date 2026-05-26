notebooks/outputs/minibus_optimisation_final.ipynb

INITIALISATION
 ├── Import libraries and project modules
 ├── Set date ranges (S25 and S26)
 └── Define planning toggles (operational constraints)

--------------------------------------------------

WORKFLOW 1: S25 MONTHLY PIPELINE (Historical)

 ├── Generate list of months
 ├── For each month:
 │     └── run_month_s25(month, toggles)
 │
 ├── Collect monthly outputs
 ├── Convert to DataFrame
 └── Export CSV (S25_monthly_outputs_v2)

--------------------------------------------------

WORKFLOW 2: BUILD S26 ASSUMPTIONS (Forecast Inputs)

 └── build_s26_assumptions(
         s25_start,
         s25_end
     )

   → Produces:
     - Demand assumptions
     - Operational parameters
     - Inputs required for S26 modelling

--------------------------------------------------

WORKFLOW 3: S26 MONTHLY PIPELINE (Forecast)

 ├── Generate list of months (Apr → Oct 2026)
 ├── For each month:
 │     └── run_month_s26(month, assumptions, toggles)
 │
 ├── Collect monthly outputs
 ├── Convert to DataFrame
 └── Export CSV (S26_monthly_outputs_v3)

 prm_opt/runs_s25/run_month_s25():

 run_month_s25(month_start, toggles)

 ├── 1. Define time window for the month
 │      ├── Convert month_start → string
 │      └── Compute month_end
 │
 ├── 2. Ingest historical data
 │      └── ingest_s25(start, end)
 │
 ├── 3. Build jobs representation
 │      └── build_jobs(raw_data)
 │
 ├── 4. Build model inputs (parameters)
 │      ├── build_tau_from_jobs()
 │      ├── build_spin_minutes()
 │      └── build_vehicle_classes()
 │
 ├── 5. Build optimisation model
 │      └── build_pyomo_model(...)
 │
 ├── 6. Solve model
 │      └── solver (HiGHS, with limits/gap/toggles)
 │
 ├── 7. Extract results
 │      ├── extract_summary()
 │      ├── extract_job_assignments()
 │      └── extract_vehicle_allocations()
 │
 ├── 8. Run sanity checks
 │      └── run_sanity_checks()
 │
 └── 9. Return formatted monthly output

 prm_opt/runs_s26/run_month_s26():

S26 VARIATION (Forecast Pipeline)

Same steps as S25 pipeline, except:

 ├── Step 2 (Ingestion)
 │     └── ingest_s26() instead of ingest_s25()

 ├── Additional Inputs
 │     └── Uses assumptions:
 │           - penetration_rates
 │           - service_time_params
 │           - stand distributions
 │           etc.

 └── Output Interpretation
       └── Forecasted demand instead of observed demand

CORE PIPELINE (USED FOR BOTH S25 & S26)

1. Define time window
2. Ingest data
3. Build jobs
4. Build model parameters
5. Build optimisation model
6. Solve model
7. Extract results
8. Run sanity checks
9. Return outputs

----------------------------------------

S25 (Historical)
 → ingest_s25 (actual operational data)

S26 (Forecast)
 → ingest_s26 (simulated demand using assumptions)
 → additional assumption inputs (from S25 calibration)

 prm_opt/ingest_s25/ingest_s25()

 ingest_s25(start, end)

1. Load flight data
   ├── load_flight_data(start, end)
   ├── Query EAL.FlightPerformance with columns:
   │     ├── Flight ID
   │     ├── Scheduled Flight DT
   │     ├── A/D
   │     ├── Flight Number
   │     ├── Airline Code
   │     ├── CountryName
   │     ├── Sector
   │     ├── Pax
   │     ├── Stand
   │     ├── Departure Gate
   │     ├── Actual Flight DT
   │     ├── Chocks DT
   │     ├── Turnaround Flight Number
   │     ├── Turnaround Scheduled DT
   │     ├── Minutes on Chocks
   │     └── Remote Stand
   ├── Convert timestamps to datetime
   └── Sort by Chocks DT
   → df_flights (one row per flight)

2. Engineer flight-level features
   ├── Create IsEffectiveRemote flag
   │     └── Based on Remote Stand OR NO_JETBRIDGE_AIRLINES
   ├── Compute Concurrent Stress
   │     ├── Define ±30 min window around Chocks DT
   │     ├── Count overlapping flights
   │     └── Subtract self
   → df_flights enriched with operational features

3. Load PRM service data (raw segments)
   ├── load_prm_data(start, end)
   │     ├── Format start/end for query
   │     ├── Query PRM.CompletedServicesByJob with columns:
   │     │     ├── Job ID
   │     │     ├── Passenger ID
   │     │     ├── Flight ID
   │     │     ├── Airline Code
   │     │     ├── Flight Number
   │     │     ├── Sector
   │     │     ├── A/D
   │     │     ├── Adhoc Or Planned
   │     │     ├── Request Created DT
   │     │     ├── SSR Code
   │     │     ├── Job Start Time
   │     │     ├── Job End Time
   │     │     ├── Scheduled PU Location
   │     │     ├── Scheduled DO Location
   │     │     ├── Actual PU Location
   │     │     ├── Actual DO Location
   │     │     ├── Scheduled PU DT
   │     │     ├── Location Arrival DT
   │     │     ├── Plane Gate Arrival DT
   │     │     ├── Scheduled Flight DT
   │     │     ├── Actual Flight DT
   │     │     ├── Vehicle Model
   │     │     └── Vehicle Type
   │     └── Return raw PRM segment dataset
   └── Each row = one service segment (not yet a job)
   → df_prm (segment-level data)

4. Compute empirical service times (tau)
   ├── Map SSR → numeric severity (map_ssr)
   │     └── WCHC=3, WCHS=2, else=1
   ├── Compute segment_mins = End - Start
   ├── Clip negative values to 0
   ├── Group by (SSR Code, A/D, Vehicle Type)
   │     └── Take median(segment_mins)
   ├── Pivot to wide format
   │     ├── tau_amb_mins
   │     ├── tau_mini_mins
   │     └── tau_push_mins
   → veh_svc_wide (empirical service time parameters)

5. Build passenger-level features
   ├── passenger_level_flags(df_prm)
   ├── Derive vertical/service complexity flags
   └── Prepare for aggregation
   → df_prm_flags (still segment-level, enriched)

6. Assign operational attributes (e.g. own chair, location)
   ├── Set random seed
   ├── Generate random draw per passenger
   ├── Assign Has Own Chair
   │     ├── WCHC → 1
   │     └── WCHS → probabilistic (WCHS_OWN_CHAIR_PROB)
   ├── Assign Strategic Location
   │     ├── Arrival → pickup location
   │     └── Departure → drop-off location
   → df_prm_flags updated with operational attributes

7. Aggregate segments → passenger-flight jobs
   ├── Group by (Passenger ID, Airline Code, Flight Number)
   ├── Apply aggregation rules:
   │     ├── Sector: first
   │     ├── A/D: first
   │     ├── Day: first
   │     ├── Adhoc Or Planned: first
   │     ├── SSR Code: first
   │     ├── SSR numeric: first
   │     ├── Has Own Chair: max
   │     ├── Job Start Time: min
   │     ├── Job End Time: max
   │     ├── Strategic Location: first
   │     ├── Location Arrival DT: min
   │     ├── Plane Gate Arrival DT: first
   │     ├── Scheduled Flight DT: first
   │     ├── Stand: first
   │     └── PassengerType: first
   ├── Merge with veh_svc_wide (tau values)
   ├── Fill missing tau values with defaults (20 mins)
   → df_prm_grouped (one row per passenger-flight)

8. Compute flight-level PRM metrics
   ├── Group by (Flight Number, Airline Code, Day)
   ├── Count unique Passenger ID
   └── Create PRM Flight Count
   ├── Merge into df_flights
   └── Fill missing with 0
   → df_flights with PRM demand metrics

9. Merge PRM data with flight data
   ├── Join df_prm_grouped with df_flights
   ├── Keys:
   │     ├── Flight Number
   │     ├── Airline Code
   │     ├── A/D
   │     └── Scheduled Flight DT
   ├── Retain:
   │     ├── Flight Number
   │     ├── Airline Code
   │     ├── A/D
   │     ├── Scheduled Flight DT
   │     ├── IsEffectiveRemote
   │     ├── Concurrent Stress
   │     ├── Minutes on Chocks
   │     ├── PRM Flight Count
   │     ├── Chocks DT
   │     └── CountryName
   ├── Enforce validate="m:1"
   │     └── Ensures many PRM rows map to exactly one flight row
   │     └── Throws error if duplicate flight matches exist (data integrity check)
   → df_prm_master (joined passenger + flight dataset)

10. Clean + filter unmatched records
   ├── Filter to valid Scheduled Flight DT window
   ├── Identify unmatched PRM rows
   ├── Diagnose join mismatches
   └── Drop invalid/unresolvable records
   → Cleaned df_prm_master

→ OUTPUT: df_prm_master

    Final columns (core):
    ├── Passenger ID
    ├── Airline Code
    ├── Flight Number
    ├── A/D
    ├── Sector
    ├── SSR Code
    ├── SSR numeric
    ├── Has Own Chair
    ├── Strategic Location
    ├── Job Start Time
    ├── Job End Time
    ├── Location Arrival DT
    ├── Plane Gate Arrival DT
    ├── Scheduled Flight DT
    ├── Stand
    ├── PassengerType

    Service time inputs:
    ├── tau_amb_mins
    ├── tau_mini_mins
    ├── tau_push_mins

    Flight-level context:
    ├── Chocks DT
    ├── Minutes on Chocks
    ├── IsEffectiveRemote
    ├── Concurrent Stress
    ├── PRM Flight Count
    ├── CountryName

modules/domain/prm/minibuses/passenger_level_flags():

passenger_level_flags(prm_df)

1. Input dataset
   ├── prm_df (segment-level PRM data)
   ├── Required columns:
   │     ├── Passenger ID
   │     ├── A/D
   │     └── Vehicle Type
   └── Each row = one service segment
   → Segment-level PRM dataset

2. Create working copy
   ├── x = prm_df.copy()
   ├── Prevents modification of original dataset
   → x (working dataframe)

3. Define grouping structure
   ├── group_cols = ["Passenger ID", "A/D"]
   ├── Groups all segments belonging to the same:
   │     ├── passenger
   │     └── direction (arrival or departure)
   → Passenger-direction grouping defined

4. Create Used_Ambulift flag
   ├── Group by (Passenger ID, A/D)
   ├── For each group:
   │     ├── Collect all Vehicle Type values
   │     ├── Convert to set
   │     ├── Check if "Ambulift" exists
   │     └── Return 1 if yes, else 0
   ├── Apply using transform (row-level output)
   → Column created:
         Used_Ambulift ∈ {0,1}
         (same value repeated for all rows of that passenger/A-D group)

5. Create Used_Minibus flag
   ├── Group by (Passenger ID, A/D)
   ├── For each group:
   │     ├── Collect all Vehicle Type values
   │     ├── Convert to set
   │     ├── Check if "Mini Bus" exists
   │     └── Return 1 if yes, else 0
   ├── Apply using transform (row-level output)
   → Column created:
         Used_Minibus ∈ {0,1}
         (same value repeated for all rows of that passenger/A-D group)

6. Initialise PassengerType
   ├── Set default:
   │     PassengerType = "No Vehicle"
   → All rows initially classified as no vehicle use

7. Assign PassengerType classification
   ├── Apply rules:

   ├── Case 1:
   │     Used_Ambulift == 1 AND Used_Minibus == 0
   │     → PassengerType = "Ambulift Only"

   ├── Case 2:
   │     Used_Ambulift == 0 AND Used_Minibus == 1
   │     → PassengerType = "Mini Bus Only"

   ├── Case 3:
   │     Used_Ambulift == 1 AND Used_Minibus == 1
   │     → PassengerType = "Both"

   └── Default remains:
         "No Vehicle"

   → Column created:
         PassengerType ∈ {
             "Ambulift Only",
             "Mini Bus Only",
             "Both",
             "No Vehicle"
         }

8. Preserve row-level structure
   ├── No aggregation occurs
   ├── Number of rows remains unchanged
   ├── Flags are duplicated across all rows in each group
   → Segment-level dataset maintained

9. Return result
   └── return x

→ OUTPUT: df_prm_flags

   Added columns:
   ├── Used_Ambulift (0/1)
   ├── Used_Minibus (0/1)
   └── PassengerType (categorical)

   Key behaviour:
   ├── Flags are computed at Passenger ID + A/D level
   ├── Broadcast back to all segment rows via transform
   └── Enables correct aggregation later in ingest (Step 7)