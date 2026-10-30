# FastPark Historical Analysis

## Overview

This analysis framework was created to understand the historical drivers of FastPark demand before developing any forecasting models.

The intention was not to build a forecast directly, but to answer a series of business questions:

- What drives FastPark entries?
- What drives FastPark exits?
- How much future demand is visible through bookings?
- How reliable are expected return dates?
- How do duration patterns affect exit forecasting?
- How useful are passenger volumes as forecasting drivers?
- What effect do pricing and market positioning have on demand?
- What historical behaviours should be carried forward into future forecasting models?

The output of this analysis was subsequently used to build:

- Simulation Generation 1
- Simulation Generation 2
- Simulation Generation 2.1
- The Production Forecast Model

---

# Script

```text
fastpark_forecast_analysis.py
```

---

# Data Sources

The analysis combines three core datasets.

## AirportX.v_Bookings

Used for:

- Booking creation dates
- Planned entry dates
- Planned exit dates
- Lead times
- Planned duration
- Booking status
- Cancellation dates
- Product information
- Pricing information
- Airline information

---

## FastPark.v_EntryAndExits

Used for:

- Actual entries
- Actual exits
- Expected return dates
- Check-in behaviour
- Return behaviour
- Operational durations

---

## EAL.FlightPerformance

Used for:

- Departure passengers
- Arrival passengers
- Flight volumes
- Airline mix
- Country mix
- Domestic / international mix

---

# Analysis Stages

The script runs through fourteen major stages.

---

## Stage 1: Load Booking Data

Loads FastPark bookings from:

```text
AirportX.v_Bookings
```

Includes:

- Valid bookings (B)
- Cancelled bookings (CX)
- Unknown bookings (F)

---

## Stage 2: Load Operational Data

Loads actual operational movements from:

```text
FastPark.v_EntryAndExits
```

Creates the foundation for:

- Actual entries
- Actual exits
- Duration analysis
- Return behaviour analysis

---

## Stage 3: Load Flight Data

Loads:

```text
EAL.FlightPerformance
```

Used to understand airport demand context.

---

## Stage 4: Data Cleaning

Creates:

### Booking Features

- Lead time
- Planned duration
- Entry weekday
- Entry month
- Exit weekday
- Exit month

### Pricing Features

- Booking value
- Price per day
- Product price per day

### Operational Features

- Actual entry timestamps
- Actual exit timestamps
- Expected return timestamps

### Flight Features

- Passenger counts
- Arrivals
- Departures
- Domestic / international indicators

---

## Stage 5: Booking To Operations Reconciliation

Joins:

```text
AirportX.v_Bookings.bookingId
=
FastPark.v_EntryAndExits.BookingReference
```

Purpose:

- Understand matching quality
- Identify missing operational records
- Validate forecasting inputs

---

## Stage 6: Status, Cancellation and No Show Analysis

Analyses:

### Booking Status

- B
- CX
- F

### Cancellations

Measures:

- Cancellation rates
- Cancellation timing
- Duration-driven cancellation behaviour
- Lead-time-driven cancellation behaviour

### No Shows

Measures:

```text
Valid Booking
+
No Operational Check-In
```

and evaluates whether no-shows are material to forecasting.

---

## Stage 7: Daily and Hourly Actual Demand

Creates:

### Daily

- Entries
- Exits
- Movements
- Net Flow

### Hourly

- Entries by hour
- Exits by hour
- Movements by hour

Also creates weekday-level hourly operational profiles.

---

## Stage 8: Occupancy Reconstruction

Rebuilds historical occupancy using:

```text
Current Cars On Site
+
Historical Entries
-
Historical Exits
```

to estimate:

- Historic cars on site
- Occupancy %
- Available spaces
- Occupancy bands

This was used to understand operational pressure.

---

## Stage 9: Passenger Context

Creates:

### Daily Passenger Summaries

- Departing passengers
- Arriving passengers
- Total passengers

### Hourly Passenger Summaries

- Hourly departures
- Hourly arrivals

### Passenger Mix

- Domestic departures
- International departures
- Domestic arrivals
- International arrivals

---

## Stage 10: Demand Driver Analysis

Combines FastPark demand with airport activity.

Investigates relationships between:

### FastPark Demand

- Entries
- Exits
- Movements

### Passenger Demand

- Departing passengers
- Arriving passengers
- Total passengers

### Passenger Mix

- Domestic share
- International share

### Pricing

- Booking values
- Daily prices
- Relative prices

### Occupancy

- Occupancy %
- Capacity pressure

---

## Stage 11: Booking Curve Analysis

Creates:

### Entry Booking Curves

Measures visibility of future entries at:

```text
T-56
T-42
T-28
T-21
T-14
T-10
T-7
T-5
T-3
T-2
T-1
T-0
```

### Exit Booking Curves

Equivalent visibility analysis for exits.

### Duration Segmented Curves

Tests whether:

```text
Short stays
```

and

```text
Long stays
```

become visible at different points.

This proved important for exit forecasting.

---

## Stage 12: Duration Analysis

Compares:

### Planned Duration

```text
Booking Exit Date
-
Booking Entry Date
```

against

### Actual Duration

```text
Actual Exit
-
Actual Entry
```

Purpose:

Determine whether planned duration is a reliable future forecasting input.

---

## Stage 13: Return Behaviour Analysis

Compares:

```text
ExpectedReturnDate
```

against:

```text
ActualCheckedOutDate
```

Measures:

- Early returns
- Late returns
- Major return deviations
- Different-day returns

These findings were later used in hourly and exit forecasting design.

---

## Stage 14: Tendency Analysis

Backtests:

### Rolling Penetration

```text
Entries / Departing Passengers
Exits / Arriving Passengers
```

using rolling windows.

### Same Weekday Penetration

Uses:

```text
Previous N matching weekdays
```

### Weighted Weekday Penetration

Gives more importance to recent weeks.

Purpose:

Determine whether tendency-based forecasting methods add value.

---

# Key Outputs

Workbook:

```text
outputs/fastpark_historical_analysis_v6.xlsx
```

The workbook contains the outputs from all analysis stages.

Examples include:

- Data Validation Summary
- Daily FastPark Actuals
- Hourly FastPark Actuals
- Demand Driver Dataset
- Driver Correlations
- Passenger Mix Analysis
- Occupancy Analysis
- Hourly Profiles
- Booking Curves
- Duration Analysis
- Return Analysis
- Pricing Analysis
- Tendency Analysis

---

# Key Findings

## Entry Forecasting

The strongest explanatory variables were:

1. Booking visibility
2. Same-weekday behaviour
3. Weekday-month seasonality

Passenger demand was useful but generally weaker than booking visibility at shorter horizons.

---

## Exit Forecasting

The strongest explanatory variables were:

1. Exit booking visibility
2. Planned duration
3. Expected return behaviour

Duration emerged as one of the most important variables for future modelling.

---

## Booking Curves

Booking visibility was shown to be highly predictive, particularly at shorter horizons.

This ultimately became one of the core forecasting signals used in later simulations.

---

## Duration

Planned duration was strongly correlated with actual duration.

This justified its use in forecasting future exits.

---

## Return Behaviour

Expected return dates were shown to contain useful forecasting information.

This became a candidate signal for future hourly exit forecasting.

---

## Pricing

Pricing and relative pricing were shown to be potentially useful explanatory variables.

These findings ultimately led to the creation of the future pricing-focused forecasting work.

---

# Outputs Location

```text
01 Historical Analysis/
│
├── fastpark_forecast_analysis.py
│
└── outputs/
    └── fastpark_historical_analysis_v6.xlsx
```

---

# How To Run

From the analysis directory:

```bash
python fastpark_forecast_analysis.py
```

The script will:

1. Connect to Azure SQL.
2. Load bookings.
3. Load operations.
4. Load flight data.
5. Perform all analysis stages.
6. Export the output workbook.

---

# When To Re-Run

The historical analysis does not need to run daily.

Recommended re-run triggers:

- Significant increases in available history.
- Changes to booking behaviour.
- Changes to operational processes.
- Before forecast recalibration.
- Before introducing new forecasting drivers.

Recommended frequency:

```text
Quarterly
```

or immediately before major forecast redevelopment work.

---

# Relationship To Later Work

The outputs from this analysis directly informed:

```text
02 Simulation Gen 1
03 Simulation Gen 2
04 Simulation Gen 2.1
05 Production Forecast
```

and should be considered the foundational evidence base for the FastPark forecasting framework.