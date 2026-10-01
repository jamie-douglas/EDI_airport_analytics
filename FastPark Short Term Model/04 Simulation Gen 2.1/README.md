# FastPark Forecast Simulation - Generation 2.1

## Overview

Generation 2.1 was created to test whether additional non-price forecasting features could improve the Generation 2 ensemble framework.

Generation 2 had already demonstrated that:

```text
Booking Visibility
+
Duration
+
Seasonality
+
Trend
```

could be successfully combined using rolling-origin ensemble forecasting.

The objective of Generation 2.1 was to answer a new question:

> Can additional operational and behavioural features improve forecasting accuracy beyond the Generation 2 ensemble?

Unlike Generation 2, this framework focused on feature engineering rather than introducing new core forecasting signals.

---

# Objectives

The project had six primary objectives:

1. Test whether booking pace contains useful information.
2. Test whether cancellation behaviour contains useful information.
3. Test whether calendar events influence demand.
4. Test whether booking-position demand regimes exist.
5. Test whether weekday-specific trend effects are useful.
6. Determine whether alternative historical windows outperform the fixed Generation 2 settings.

---

# Relationship To Previous Generations

## Historical Analysis

Established:

- Booking visibility
- Duration behaviour
- Return behaviour
- Passenger relationships
- Cancellation behaviour

---

## Generation 1

Identified:

- Strong individual forecasting models.
- Importance of booking visibility.
- Importance of duration.
- Importance of seasonality.

---

## Generation 2

Demonstrated:

```text
Best Combination Of Forecasting Signals
```

using:

- Rolling-origin validation
- Ensemble weighting
- Booking shrinkage
- Trend adjustment

---

## Generation 2.1

Asked:

> Can additional behavioural and operational information improve the Generation 2 ensemble?

---

# New Features Tested

Generation 2.1 retained the Generation 2 forecasting framework and introduced additional experimental features.

---

## Booking Pace

### Purpose

Measure how quickly bookings are accumulating.

Generation 2 uses:

```text
Known Bookings
```

Generation 2.1 additionally tests:

```text
Rate of Booking Growth
```

between historical forecast horizons.

---

### Example

```text
T-28 = 100 bookings
T-21 = 160 bookings
```

This indicates higher booking pace than:

```text
T-28 = 100 bookings
T-21 = 110 bookings
```

---

### Question

Does the speed of booking accumulation contain predictive information beyond the booking count itself?

---

## Historical Window Optimisation

### Purpose

Generation 2 used:

```text
4 same weekdays
```

throughout the framework.

Generation 2.1 tested:

```text
1
2
4
6
8
```

same-weekday windows.

---

### Question

Does the optimal historical window change by:

```text
Flow
Horizon
```

rather than remaining fixed?

---

## Calendar Effects

### Purpose

Test whether demand behaves differently during known calendar periods.

---

### Categories Tested

#### Public Holidays

Examples:

```text
Bank Holidays
```

---

#### School Holidays

Examples:

```text
Summer Holidays
October Holidays
February Holidays
```

---

#### Easter

Explicit Easter travel periods.

---

#### Christmas / New Year

Seasonal holiday travel period.

---

#### Special Events

Examples:

```text
Royal Highland Show
Edinburgh Festival
Summer Sessions
```

---

### Question

Do these periods systematically alter demand behaviour?

---

## Booking Regimes

### Purpose

Classify demand into different booking environments.

---

### Regimes

Examples:

```text
Very Low
Low
Normal
High
Very High
```

booking positions.

---

### Question

Should forecasts behave differently during:

```text
High Demand
```

than during:

```text
Low Demand
```

periods?

---

## Weekday-Specific Trend

### Purpose

Generation 2 compares:

```text
Recent Period
vs
Historical Period
```

Generation 2.1 tests:

```text
Recent Fridays
vs
Historical Fridays
```

and equivalent weekday comparisons.

---

### Question

Do weekday-specific trends improve forecasting performance beyond a general trend factor?

---

## Cancellation Behaviour

### Purpose

Generation 1 and Generation 2 already remove cancellations that were known at the forecast cut-off.

Generation 2.1 tests whether cancellation behaviour itself contains predictive information.

---

### Example

Recent cancellation rates may be:

```text
Very Low
Low
Normal
High
Very High
```

relative to historical expectations.

---

### Question

Can unusual cancellation patterns improve demand forecasting beyond booking visibility?

---

# Experiments

Generation 2.1 grouped features into controlled experiments.

---

## BASELINE

Generation 2 feature set only.

Used as the benchmark for comparison.

---

## ADD_BOOKING_PACE

Tests:

```text
Booking Pace
```

in addition to the Generation 2 framework.

---

## ADD_CANCELLATION

Tests:

```text
Cancellation Behaviour
```

in addition to the Generation 2 framework.

---

## ADD_CALENDAR

Tests:

```text
Calendar Effects
```

in addition to the Generation 2 framework.

---

## ADD_BOOKING_REGIME

Tests:

```text
Booking Regimes
```

in addition to the Generation 2 framework.

---

## ADD_WEEKDAY_TREND

Tests:

```text
Weekday-Specific Trend
```

in addition to the Generation 2 framework.

---

## ALL_NON_PRICE

Combines all non-price features simultaneously.

Purpose:

Determine whether multiple behavioural features provide incremental benefit when used together.

---

# Rolling-Origin Framework

Generation 2.1 retains the same rolling-origin approach used in Generation 2.

Each fold contains:

```text
Training
Validation
Test
```

periods.

The test month is never used during feature selection.

This ensures:

```text
No Look-Ahead Bias
```

throughout the process.

---

# Outputs

Workbook:

```text
04 Simulation Gen 2.1/
└── outputs/
    └── fastpark_forecast_simulation_non_price.xlsx
```

---

## Experiment Performance

Primary performance table.

Shows:

```text
Flow
Horizon
Experiment
WAPE
MAE
RMSE
Bias
```

Used to determine whether additional features improve forecasting accuracy.

---

## Incremental Benefit

Compares each experiment against:

```text
BASELINE
```

Measures:

- WAPE improvement
- MAE improvement
- RMSE improvement
- Bias improvement

One of the most important sheets in the workbook.

---

## Fold Win Rate

Measures:

```text
How Often An Experiment Beats Baseline
```

across rolling-origin folds.

Useful for identifying:

```text
Consistent Improvements
```

rather than isolated improvements.

---

## Selected Weights

Contains:

- Winning weights
- Winning shrinkage strengths
- Winning experiment configuration

for every horizon and fold.

---

## Historical Window Tests

Contains:

```text
1 Week
2 Week
4 Week
6 Week
8 Week
```

same-weekday history testing results.

This later influenced:

```text
forecast_parameters.csv
```

used by the Production Forecast.

---

## Rolling Folds

Documents:

- Training Periods
- Validation Periods
- Test Periods

for every rolling-origin fold.

---

## Out Of Sample Forecasts

Contains the actual validation forecasts generated during testing.

These are the true unseen forecasts.

---

## Extended Components

Contains all engineered features:

- Pace
- Calendar
- Regime
- Weekday Trend
- Cancellation Features

Used primarily for diagnostics and feature analysis.

---

# Key Findings

## Booking Pace

Booking pace contained useful information beyond booking visibility alone.

This demonstrated that:

```text
Rate Of Booking Growth
```

can matter as well as:

```text
Current Booking Position
```

---

## Historical Window Selection

The optimal same-weekday history length varied by:

- Flow
- Horizon

This demonstrated that:

```text
One Historical Window
```

is not optimal across the full forecast range.

---

## Calendar Effects

Calendar periods influenced demand behaviour.

However, their impact varied significantly between:

- Event types
- Horizons
- Demand flows

---

## Booking Regimes

Evidence suggested that demand behaves differently during different booking-position environments.

This introduced the idea that:

```text
Forecast Behaviour Should Adapt To Demand Conditions
```

rather than remaining fixed.

---

## Weekday Trend

Weekday-specific trends performed differently from general demand trends.

This highlighted the importance of maintaining weekday structure within demand forecasts.

---

## Cancellation Behaviour

Cancellation patterns contained useful information in specific situations.

The value was not universal across every horizon but demonstrated that cancellation activity can provide additional context.

---

# Features Influencing Production Forecast

Generation 2.1 contributed directly to the Production Forecast framework.

### Adopted

✅ Historical Window Selection

✅ Booking Shrinkage Refinement

✅ Hourly Profile Selection Process

---

### Influenced Production Design

⚠ Booking Pace

⚠ Cancellation Behaviour

⚠ Calendar Effects

⚠ Booking Regimes

⚠ Weekday Trend

These features informed the final Production Forecast architecture even where they were not implemented as direct forecast components.

---

# Why A Production Forecast Was Needed

Generation 2.1 successfully answered:

> Which non-price features appear useful?

The next challenge became:

> How do we convert these findings into a repeatable operational forecasting process?

This led to:

```text
forecast_weights.csv

forecast_parameters.csv

fastpark_short_term_forecast_2026.py
```

and the Production Forecast framework.

---

# How To Run

```bash
python fastpark_forecast_simulation_non_price.py
```

Process:

1. Import Generation 1 framework.
2. Import Generation 2 framework.
3. Build all-date component table.
4. Create non-price features.
5. Run rolling-origin feature tournament.
6. Compare experiments.
7. Export workbook.

---

# When To Re-Run

Recommended:

- Before production parameter recalibration.
- After major behavioural changes.
- After significant changes in booking patterns.
- Before updating forecast parameters.

Typical frequency:

```text
Quarterly
```

or before major forecast releases.

---

# Relationship To Later Models

## Historical Analysis

Identified forecasting signals.

---

## Generation 1

Identified strongest individual forecasting methods.

---

## Generation 2

Optimised combinations of forecasting signals.

---

## Generation 2.1

Optimised feature engineering and behavioural adjustments.

---

## Production Forecast

Combined:

- Generation 1 signal selection
- Generation 2 weighting
- Generation 2.1 feature engineering

into a configurable operational forecasting framework.