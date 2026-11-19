# FastPark Forecast Simulation - Generation 2

## Overview

Generation 2 was created after Generation 1 had successfully identified the strongest forecasting signals.

Generation 1 answered:

> Which forecasting approaches work?

Generation 2 was designed to answer a different question:

> What is the optimal blend of those forecasting signals?

Instead of selecting a single winning model, Generation 2 introduced weighted ensembles, rolling-origin validation and horizon-specific optimisation.

The result was the first forecasting framework capable of learning different forecasting behaviour at:

```text
T-0
T-7
T-14
T-28
T-56
```

rather than assuming the same model should be used at every horizon.


Folder Dependencies
-------------------

Generation 2 depends on:

    ../02 Simulation Gen 1/
        fastpark_forecast_simulation.py

Outputs are written to:

    ./outputs/

This structure should be maintained if the project is moved.


---

# Why Generation 2 Was Created

Generation 1 revealed a number of consistent findings.

## Entries

Short horizons were dominated by:

- Booking Visibility Curve
- Booking Visibility + Same Weekday

Long horizons increasingly favoured:

- Weekday-Month Seasonality
- Historical Demand Patterns

---

## Exits

Short horizons were dominated by:

- Exit Booking Visibility
- Duration Adjusted Exit Visibility

Long horizons increasingly relied on:

- Historical Seasonality
- Hybrid Forecasts

---

This created a natural question:

> Rather than choosing one model, can we combine the strongest elements of each?

Generation 2 was built to answer that question.

---

# Key Changes From Generation 1

## 1. All-Date Testing

Generation 1 tested:

```text
1st-7th
14th-20th
```

of each month.

Generation 2 tests:

```text
Every calendar day
```

within the modelling period.

This significantly increases the sample size and robustness of the results.

---

## 2. Rolling-Origin Validation

Generation 1 effectively evaluated performance across a single historical sample.

Generation 2 introduces:

```text
Train
Validation
Test
```

monthly rolling folds.

A model is selected using:

```text
Training Data
Validation Data
```

and then evaluated on a completely unseen month.

This greatly reduces overfitting.

---

## 3. Signal Reduction

Generation 1 tested:

```text
15 Entry Models
17 Exit Models
```

Generation 2 keeps only the strongest independent forecasting signals.

### Entry Components

- Booking Visibility
- Same Weekday Historical Demand
- Weekday-Month Historical Demand
- Trend-Adjusted Weekday-Month Demand

### Exit Components

- Exit Booking Visibility
- Duration Adjusted Exit Visibility
- Same Weekday Historical Demand
- Trend-Adjusted Weekday-Month Demand

---

## 4. Forecast Blending

Generation 1 focused on:

```text
Best Individual Model
```

Generation 2 focuses on:

```text
Best Combination Of Signals
```

using non-negative ensemble weights.

---

## 5. Booking Shrinkage

Generation 2 introduces:

```text
Booking Shrinkage
```

to stabilise booking-based forecasts.

When booking visibility is weak:

```text
Booking Forecast
↓
Historical Forecast
```

contribute together.

When booking visibility is strong:

```text
Booking Forecast
```

dominates.

---

## 6. Weight Stability

Generation 2 introduces:

```text
Weight Stability Penalties
```

to reduce excessive month-to-month weight changes.

---

# Forecast Components

## Entry Forecast Components

### Entry Booking

```text
Visible Bookings
×
Historical Booking Curve
```

---

### Entry Weekday

```text
Previous Matching Weekdays
```

---

### Entry Month

```text
Same Weekday
+
Same Month
```

historical demand.

---

### Entry Trend Month

```text
Month Forecast
×
Historical Trend Factor
```

where trend compares:

```text
Recent 28 Days
vs
Previous 84 Days
```

---

## Exit Forecast Components

### Exit Booking

```text
Visible Exit Bookings
×
Historical Exit Curve
```

---

### Exit Duration

```text
Exit Booking
+
Duration Adjustment
```

using duration-specific completion factors.

---

### Exit Weekday

```text
Previous Matching Weekdays
```

---

### Exit Trend Month

```text
Month Forecast
×
Trend Factor
```

---

# Rolling-Origin Framework

Generation 2 uses monthly rolling-origin folds.

Example:

```text
Training:
Jul → Dec

Validation:
Jan

Test:
Feb
```

Then:

```text
Training:
Jul → Jan

Validation:
Feb

Test:
Mar
```

and so on.

This ensures:

```text
Future months never select their own weights.
```

---

# Outputs

## Main Workbook

```text
03 Simulation Gen 2/
└── outputs/
    └── fastpark_forecast_simulation_refined.xlsx
```

---

### Refined Performance

Performance of the final ensemble forecasts by:

- Flow
- Horizon

Includes:

- WAPE
- MAE
- RMSE
- Bias

---

### Baseline Comparison

Compares:

```text
Refined Ensemble
```

against:

```text
Entry Booking
Entry Weekday
Entry Month
Entry Trend

Exit Booking
Exit Duration
Exit Weekday
Exit Trend
```

This is one of the most important sheets because it proves whether the ensemble actually delivers value.

---

### Recommended Weights

Final production-ready weights.

These became the foundation for:

```text
forecast_weights.csv
```

used by the Production Forecast.

---

### Selected Weights

Winning weights from every rolling fold.

Used to understand:

- Stability
- Variability
- Consistency

---

### Weight Stability

Measures variation in selected weights across folds.

Purpose:

Identify whether weight selection is:

```text
Stable
```

or:

```text
Highly Variable
```

---

### Rolling Folds

Documents:

- Training Period
- Validation Period
- Test Period

for every fold.

---

### Out of Sample Forecasts

The true validation output.

Contains every forecast generated on unseen data.

---

### Component Table

The complete component dataset.

Contains:

- Booking
- Duration
- Weekday
- Month
- Trend

for every date and horizon.

---

### Worst Forecast Days

Largest forecasting misses.

Used for:

- Diagnostics
- Future improvement work

---

### All Weight Tests

Every weight combination tested.

Provides a complete audit trail for model selection.

---

# Key Findings

## Entries

### Short Horizons

Booking visibility overwhelmingly dominated forecast accuracy.

The strongest weights were heavily concentrated in:

```text
Entry Booking
```

particularly at:

```text
T-0
T-1
T-2
T-7
```

---

### Long Horizons

Weights gradually shifted toward:

```text
Weekday
Month
Trend
```

signals.

This confirmed the findings from Generation 1.

---

## Exits

### Short Horizons

Duration remained one of the strongest forecasting components.

Large weights repeatedly appeared on:

```text
Exit Duration
```

particularly between:

```text
T-0
T-14
```

---

### Long Horizons

Seasonality and trend became increasingly important.

---

## Shrinkage

Booking shrinkage improved stability at longer horizons where booking visibility was weak.


---

# What Generation 2 Proved

Generation 2 demonstrated that:

```text
Weighted Ensembles
>
Single Winning Models
```

for both:

- Entries
- Exits

It also demonstrated that:

```text
Different horizons require different behaviours.
```

A T-0 forecast and a T-56 forecast should not use the same forecasting logic.

This became one of the most important design principles in the Production Forecast.

---

# Why Generation 2.1 Was Created

Generation 2 successfully optimised:

- Booking Visibility
- Duration
- Seasonality
- Trend

However it deliberately ignored:

- Booking Pace
- Cancellation Behaviour
- Calendar Effects
- Regime Changes

The next question became:

> Can additional non-price features improve forecast accuracy?

This became Generation 2.1.

---

# How To Run

```bash
python fastpark_forecast_simulation_refined.py
```

Process:

1. Load Generation 1 data.
2. Build all-date component table.
3. Create rolling-origin folds.
4. Optimise ensemble weights.
5. Apply out-of-sample testing.
6. Generate performance outputs.
7. Export workbook.

---

# When To Re-Run

Recommended:

- Before production weight recalibration.
- After major behavioural changes.
- After material increases in available history.
- Before updating forecast_weights.csv.

Typical frequency:

```text
Quarterly
```

or before major model releases.

---

# Relationship To Later Models

## Generation 1

Identified:

```text
Useful Signals
```

---

## Generation 2

Optimised:

```text
Signal Weighting
```

---

## Generation 2.1

Tested:

```text
Additional Feature Engineering
```

including:

- Booking Pace
- Cancellation Behaviour
- Calendar Effects
- Regime Effects

---

## Production Forecast

The production forecast ultimately combines:

- Generation 1 signal selection
- Generation 2 ensemble weighting
- Generation 2.1 feature engineering

into a horizon-specific forecasting framework.