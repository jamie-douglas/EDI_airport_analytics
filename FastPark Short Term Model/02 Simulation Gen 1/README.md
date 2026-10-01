# FastPark Forecast Simulation - Generation 1

## Overview

Generation 1 was the first full forecasting backtest framework developed for FastPark.

The goal was to move beyond descriptive analysis and answer a practical forecasting question:

> If we had stood on a historical day at T-56, T-28, T-14, T-7, T-1 or T-0, what forecast would we have produced using only the information available at that time?

The framework evaluates multiple forecasting methodologies using historical backtesting and compares forecast performance against actual operational entries and exits.

Generation 1 became the first formal FastPark forecasting tournament.

---

# Objectives

The project had four core objectives:

1. Create a genuine historical backtest.
2. Prevent look-ahead bias.
3. Compare competing forecasting approaches fairly.
4. Identify which forecasting methods worked best at different forecast horizons.

---

# Questions Generation 1 Was Designed To Answer

Generation 1 was not trying to create the final production forecast.

Instead it was attempting to answer a series of forecasting questions:

### Entries

- Do bookings outperform passenger models?
- Are recent same-weekday observations useful?
- Does weighting recent history improve accuracy?
- Does seasonality improve longer-horizon forecasts?
- Is there value in combining bookings and historical behaviour?

### Exits

- Can exits be forecast from bookings?
- Does stay duration improve forecasts?
- Are expected return dates useful?
- Can entry cohorts explain future exits?
- Does seasonality improve longer-horizon forecasts?

The purpose of the simulation was to identify the strongest forecasting signals before building a more advanced forecasting framework.

---

# Key Principle: No Look-Ahead Bias

A forecast at T-28 may use:

- Bookings known at T-28
- Cancellations known at T-28
- Historical demand
- Historical passenger behaviour
- Historical booking curves

A forecast at T-28 may NOT use:

- Future bookings
- Future cancellations
- Actual target-day demand
- Actual target-day passenger counts

This principle was retained through Generation 2 and Generation 2.1.

---

# Data Sources

## AirportX.v_Bookings

Provides:

- Booking visibility
- Entry dates
- Exit dates
- Lead times
- Duration
- Cancellations

---

## FastPark.v_EntryAndExits

Provides:

- Actual entries
- Actual exits
- Expected returns
- Operational durations

---

## EAL.FlightPerformance

Provides:

- Arriving passengers
- Departing passengers
- Domestic / international splits
- Flight volumes

---

# Test Design

## Historical Test Period

Target dates:

```text
1st – 7th
14th – 20th
```

for every month:

```text
July 2025 → July 2026
```

Total:

```text
182 target dates
```

---

## Forecast Horizons

Each target date was tested at:

```text
T-0
T-1
T-2
T-3
T-4
T-5
T-6
T-7
T-14
T-21
T-28
T-35
T-42
T-49
T-56
```

---

# Forecast Approaches Tested

## Entry Models (E1-E15)

The Generation 1 framework deliberately tested a broad range of forecasting approaches rather than assuming a booking-driven forecast would always be best.

The objective was to understand how forecasting performance changes by horizon and whether different information sources become more valuable at different points in the booking curve.

---

### Historical Tendency Models

These models use historical FastPark entry demand only.

#### E1 - Last Same Weekday

Uses the most recent occurrence of the same weekday as the forecast target.

Example:

```text
Forecasting Saturday
↓
Use previous Saturday
```

Purpose:

Determine whether the most recent observation contains sufficient information to generate an accurate forecast.

---

#### E2 - Average Last 2 Same Weekdays

Uses the average of the previous two matching weekdays.

Example:

```text
Forecasting Saturday
↓
Average previous two Saturdays
```

Purpose:

Reduce volatility from single-day anomalies.

---

#### E3 - Average Last 4 Same Weekdays

Uses the average of the previous four matching weekdays.

Purpose:

Provide a balance between:

- Recency
- Forecast stability


---

#### E4 - Average Last 6 Same Weekdays

Uses the average of the previous six matching weekdays.

Purpose:

Test whether additional history improves forecast stability.

---

#### E5 - Average Last 8 Same Weekdays

Uses the average of the previous eight matching weekdays.

Purpose:

Test long historical windows.

---

### Weighted Historical Models

These models apply greater weight to more recent historical observations.

---

#### E6 - Weighted Last 4 Same Weekdays

Most recent weekday receives highest influence.

Purpose:

Test whether recent demand conditions matter more than older history.

---

#### E7 - Weighted Last 6 Same Weekdays

Weighted version of the six-week model.

Purpose:

Provide additional stability while still prioritising recent history.

---

#### E8 - Weighted Last 8 Same Weekdays

Weighted version of the eight-week model.

Purpose:

Test longer weighted historical windows.

---

### Passenger-Based Models

These models convert airport passengers into FastPark demand using penetration assumptions.

---

#### E9 - Same Weekday Passenger Penetration

Forecast:

```text
Estimated Departing Passengers
×
Historical Same Weekday Entry Penetration
```

Purpose:

Determine whether airport demand explains FastPark demand.

---

#### E10 - Average Passenger Penetration

Forecast:

```text
Estimated Departing Passengers
×
Average Historical Entry Penetration
```

Purpose:

Test whether passenger demand alone can generate a useful forecast.

---

### Booking Visibility Models

These models use the current book of business.

---

#### E11 - Booking Visibility Curve

Forecast:

```text
Known Bookings
×
Historical Booking Curve Completion Factor
```

Purpose:

Estimate final demand from the bookings already visible at the forecast cut-off.


---

#### E12 - Booking Curve + Same Weekday

Forecast:

```text
Booking Visibility
+
Same Weekday History
```

Purpose:

Combine real booking visibility with historical seasonality.

---

### Hybrid Models

---

#### E13 - Booking + Passenger Hybrid

Forecast:

```text
Booking Visibility
+
Passenger Demand
```

Purpose:

Combine the two strongest conceptual demand drivers:

- FastPark bookings
- Airport departure demand

---

### Seasonal Models

---

#### E14 - Weekday-Month Historical

Forecast:

```text
Same Weekday
+
Same Month
```

Example:

```text
July Saturday
↓
Previous July Saturdays
```

Purpose:

Test whether monthly seasonality improves forecast accuracy.


---

#### E15 - Weekday-Month + Booking Curve

Forecast:

```text
Booking Visibility
+
Weekday-Month Seasonality
```

Purpose:

Combine long-range seasonality with booking visibility.


---

## Exit Models (X1-X17)

Exit demand behaves differently from entry demand.

While entries are strongly driven by future bookings, exits are often driven by:

- Existing cars already on site
- Stay duration
- Return behaviour

Generation 1 therefore tested a broader range of exit-specific models.

---

### Historical Tendency Models

#### X1 - Last Same Weekday

Uses the most recent matching weekday exit demand.

---

#### X2 - Average Last 2 Same Weekdays

Average exits from the previous two matching weekdays.

---

#### X3 - Average Last 4 Same Weekdays

Average exits from the previous four matching weekdays.

---

#### X4 - Average Last 6 Same Weekdays

Average exits from the previous six matching weekdays.

---

#### X5 - Average Last 8 Same Weekdays

Average exits from the previous eight matching weekdays.

---

### Weighted Historical Models

#### X6 - Weighted Last 4 Same Weekdays

Weighted same-weekday exit history.

---

#### X7 - Weighted Last 6 Same Weekdays

Weighted six-week exit history.

---

#### X8 - Weighted Last 8 Same Weekdays

Weighted eight-week exit history.

---

### Passenger Models

#### X9 - Same Weekday Passenger Penetration

Forecast:

```text
Estimated Arriving Passengers
×
Historical Exit Penetration
```

---

#### X10 - Average Passenger Penetration

Forecast:

```text
Estimated Arriving Passengers
×
Average Historical Exit Penetration
```

---

### Booking Visibility Models

#### X11 - Exit Booking Visibility Curve

Forecast:

```text
Known Exit Bookings
×
Historical Exit Completion Factor
```

Purpose:

Estimate final exit demand from visible exit bookings.

---

### Duration Models

#### X12 - Exit Curve + Duration

Forecast:

```text
Exit Booking Visibility
+
Duration-Specific Completion Factors
```

Purpose:

Test whether different stay-length segments have different booking visibility characteristics.


---

### Return Behaviour Models

#### X13 - Expected Return Cohort

Forecast:

```text
Bookings Returning On Target Date
```

using:

```text
ExpectedReturnDate
```

Purpose:

Determine whether operational return information improves forecasts.

---

#### X14 - Expected Return + Historical Deviation

Forecast:

```text
Expected Return Cohort
+
Historical Early / Late Return Behaviour
```

Purpose:

Incorporate operational return behaviour into the forecast.

---

### Hybrid Models

#### X15 - Exit Booking + Passenger Hybrid

Forecast:

```text
Exit Booking Visibility
+
Airport Arrival Demand
```

Purpose:

Combine booking visibility with airport demand signals.

---

### Cohort Models

#### X16 - Entry Cohort + Duration

Forecast:

```text
Known Historical Entry Cohorts
×
Duration Probabilities
```

Purpose:

Estimate future exits using the cars already in the system.


---

### Seasonal Models

#### X17 - Weekday-Month Historical

Forecast:

```text
Same Weekday
+
Same Month
```

Purpose:

Capture longer-term seasonal exit behaviour.

---

# Additional Analysis

## Booking Curves

Generation 1 did not simply test forecast models.

It also created the first booking visibility framework.

Built:

### Entry Booking Curves

Measured:

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

visibility for future entries.

---

### Exit Booking Curves

Measured equivalent visibility for future exits.

---

### Duration-Specific Curves

Measured visibility separately for:

```text
0–1 day stays
2–3 day stays
4–7 day stays
8–10 day stays
11–14 day stays
15–21 day stays
22+ day stays
```

Purpose:

Determine whether stay length affects forecasting visibility.


---

## Hourly Profile Validation

Generation 1 also evaluated how daily forecasts should be converted into hourly demand.

It is important to note that this was:

```text
Hourly Profile Validation
```

and NOT:

```text
A separate hourly forecasting tournament.
```

The process was:

### Step 1

Identify the best daily forecast model.

### Step 2

Convert that daily forecast into an hourly forecast using historical hourly demand shares.

### Step 3

Test alternative profile windows:

```text
4 weeks
6 weeks
8 weeks
12 weeks
```

### Step 4

Compare hourly forecasts against actual hourly demand.

Purpose:

Answer:

> What historical hourly profile window creates the most accurate operational forecast?

rather than:

> Which forecasting model creates the best hourly forecast?

This distinction later became important because Production Forecasts moved toward:

### Entries

```text
Booking Entry Times
+
Historical Hourly Behaviour
```

and

### Exits

```text
Expected Return Times
+
Historical Return Deviations
```

rather than relying entirely on historical profiles.

# Outputs

## Daily Tournament

Workbook:

```text
02 Simulation Gen 1/
└── outputs/
    └── fastpark_forecast_simulation_v2.xlsx
```

### Best By Horizon

Shows the best-performing forecasting model at each forecast horizon.

Example:

```text
ENTRY T-7
↓
E12 Booking Curve + Same Weekday

EXIT T-14
↓
X12 Exit Curve + Duration
```

This became the primary decision-making sheet for identifying the strongest forecasting approaches at each horizon.

---

### Entry Performance

Performance metrics for all Entry models (E1-E15).

Measures:

- WAPE
- MAE
- RMSE
- Bias

for every forecast horizon.

---

### Exit Performance

Performance metrics for all Exit models (X1-X17).

Measures:

- WAPE
- MAE
- RMSE
- Bias

for every forecast horizon.

---

### Entry WAPE Matrix

Scenario versus horizon matrix.

Purpose:

Quickly identify:

- Short-horizon winners
- Long-horizon winners
- Consistent performers

---

### Exit WAPE Matrix

Equivalent view for Exit models.

---

### Overall Ranking

Ranks models across all horizons.

Used to identify:

```text
Most Consistent Model
```

rather than:

```text
Best Model At One Horizon
```

---

### Model Stability

Measures:

- Average WAPE Rank
- Median WAPE Rank
- Horizon Wins
- Win Rate %

Used to identify models that repeatedly perform well.

---

### Forecast Coverage

Measures:

- Forecast availability
- Actual availability
- Coverage %

Used to prevent low-coverage models being selected unfairly.

---

### Scenario Definitions

Documentation for:

```text
E1-E15
X1-X17
```

including:

- Description
- Family
- Forecast logic

---

### Test Dates

Lists all historical dates included in the simulation.

---

### Data Quality

Summary of:

- Booking records
- Operational records
- Passenger records
- Validation targets

Used to validate simulation inputs.

---

### Validation Targets

Shows all historical target dates and confirms actual operational demand exists.

---

### Daily Simulation

The underlying simulation output.

Contains every forecast produced for every:

```text
Target Date
Horizon
Scenario
```

---

### Daily Actuals

Historical entry and exit demand used as forecasting targets.

---

### Passenger Context

Historical passenger demand used by passenger-based forecasting models.

---

### Best Hybrid Weights

Best-performing blend weights for:

```text
Booking + Weekday
Booking + Passenger
Booking + Seasonality
```

tests.

This directly influenced Generation 2.

---

### All Weight Tests

Every weight combination tested during calibration.

Important for auditability but not used directly in production forecasting.

---

### WAPE By Weekday

Model performance broken down by weekday.

---

### WAPE By Month

Model performance broken down by month.

---

### Demand Band Analysis

Performance split into:

- Low demand
- Medium demand
- High demand
- Peak demand

periods.

---

### Visibility Summary

Summary of booking visibility and completion behaviour.

---

### Visibility Detail

Detailed booking visibility observations.

---

### Worst Forecast Days

Largest forecast misses.

Used to identify weaknesses and edge cases.

---

### Family Comparison

Comparison by forecasting family:

```text
Historical
Weighted Historical
Passenger
Booking Curve
Hybrid
Seasonal
```

---

### Incremental Benefit

Measures whether adding:

- Bookings
- Passengers
- Duration
- Seasonality

improved forecasting performance versus simpler approaches.

---

## Hourly Profile Validation

Workbook:

```text
02 Simulation Gen 1/
└── outputs/
    └── fastpark_hourly_forecast_simulation.xlsx
```

Generation 1 did not run a separate hourly forecasting tournament.

Instead:

1. The best daily forecasting model at each horizon was identified.
2. Daily forecasts were converted into hourly forecasts.
3. Alternative historical profile windows were tested.
4. Hourly forecasts were compared against actual hourly demand.

The goal was therefore:

> What is the best historical profile window for distributing an accurate daily forecast into hours?

rather than:

> What is the best hourly forecasting model?

### Best Profile Windows

Shows the winning profile window for each:

```text
Flow
Horizon
```

using hourly forecast accuracy.

Profile windows tested:

```text
4 weeks
6 weeks
8 weeks
12 weeks
```

---

### Hourly Performance

Hourly WAPE, MAE, RMSE and Bias.

Broken down by:

- Flow
- Horizon
- Hour
- Profile Window

---

### Peak Hour Analysis

Compares:

```text
Forecast Peak Hour
```

vs

```text
Actual Peak Hour
```

for entries and exits.

---

### Hourly Forecasts

Complete hourly forecast output generated during the hourly validation process.

### Important Limitation

Generation 1 validated:

```text
Hourly Profile Windows
```

but did not test:

```text
Booking Entry Times
Expected Return Times
Return Deviation Models
```

These concepts were explored later during production forecast development.

---

# Key Findings

## Entries

### Short Horizons

Strongest models:

#### E11 – Booking Visibility Curve

```text
Known Bookings
×
Historical Booking Completion Factor
```

#### E12 – Booking Curve + Same Weekday

```text
Booking Visibility
+
Historical Weekday Behaviour
```

Finding:

Booking visibility consistently dominated short-horizon entry forecasting.

---

### Long Horizons

Strongest models:

#### E14 – Weekday-Month Historical

```text
Same Weekday
+
Same Month
```

#### E15 – Weekday-Month + Booking Curve

```text
Booking Visibility
+
Weekday-Month Seasonality
```

Finding:

Seasonality became increasingly important as booking visibility weakened.

---

## Exits

### Short Horizons

Strongest models:

#### X11 – Exit Booking Visibility Curve

```text
Known Exit Bookings
×
Historical Exit Completion Factor
```

#### X12 – Exit Curve + Duration

```text
Exit Visibility
+
Duration-Specific Completion Factors
```

Finding:

Exit visibility alone was useful, but duration materially improved accuracy.

---

### Long Horizons

Strongest models:

#### X15 – Exit Booking + Passenger Hybrid

```text
Exit Visibility
+
Airport Arrival Demand
```

#### X17 – Weekday-Month Historical

```text
Same Weekday
+
Same Month
```

Finding:

Exit forecasting gradually transitions from booking-driven to seasonality-driven as visibility decreases.

---

## Duration

Duration emerged as one of the most important forecasting drivers.

This finding survived into:

- Generation 2
- Generation 2.1
- Production Forecast

and ultimately became one of the core exit forecasting components.

---

## Passenger Models

Passenger models contained useful information but generally underperformed booking-based approaches at shorter horizons.

Passenger demand was therefore retained as a supporting signal rather than becoming the primary forecast driver.

---

## Hourly Profiles

Hourly profile selection had a measurable impact on operational forecast accuracy.

However, the analysis also highlighted a future opportunity:

```text
Entries:
Booking Entry Time

Exits:
Expected Return Time
```

potentially contain stronger hourly information than historical profiles alone.

---

# Why Generation 2 Was Created

Generation 1 successfully identified the strongest forecasting signals.

### Entries

Short-horizon forecasting was dominated by:

- Booking Visibility
- Booking Curve Hybrids

Long-horizon forecasting was increasingly driven by:

- Weekday-Month Seasonality

### Exits

Short-horizon forecasting was dominated by:

- Exit Visibility
- Duration

Long-horizon forecasting increasingly relied on:

- Seasonality
- Hybrid Approaches

---

Generation 1 therefore answered:

> Which forecasting signals work?

Generation 2 was created to answer a different question:

> What is the optimal blend of those signals?

Generation 2 introduced:

- Rolling-Origin Validation
- Ensemble Weight Optimisation
- Booking Curve Shrinkage
- All-Date Testing

rather than selecting a single winner at each horizon.

---

# How To Run

Run:

```bash
python fastpark_forecast_simulation.py
```

The process:

1. Loads historical data.
2. Builds booking curves.
3. Builds passenger context.
4. Builds duration models.
5. Runs all scenarios.
6. Scores forecasts.
7. Produces daily workbook.
8. Produces hourly workbook.

---

# When To Re-Run

Recommended:

- After major operational changes.
- After large additions to historical history.
- Before major forecast redevelopment work.
- Before recalibrating Production Forecast parameters.

Typical frequency:

```text
Quarterly
```

or before major forecasting releases.

---

# Relationship To Later Models

Generation 1 established the evidence base for all later forecasting work.

It demonstrated that:

## Entry Forecasting

Important signals included:

- Booking Visibility
- Historical Weekday Behaviour
- Weekday-Month Seasonality

## Exit Forecasting

Important signals included:

- Exit Visibility
- Duration
- Return Behaviour

---

## Generation 2

Generation 2 retained the strongest signals identified by Generation 1 and introduced:

- Rolling-Origin Validation
- Forecast Weight Optimisation
- Ensemble Forecasting
- Booking Curve Shrinkage

Generation 2 moved from:

```text
Best Single Model
```

toward:

```text
Best Combination Of Signals
```

---

## Generation 2.1

Generation 2.1 expanded the framework further by testing:

- Booking Pace
- Cancellation Behaviour
- Calendar Effects
- Regime Changes
- Weekday Trend Effects

The objective was to determine whether additional non-price information improved forecasting accuracy beyond the signal set identified in Generation 1 and Generation 2.

---

## Production Forecast

The production forecasting model ultimately combines concepts first identified in:

- Historical Analysis
- Generation 1
- Generation 2
- Generation 2.1

into a horizon-specific forecasting framework.