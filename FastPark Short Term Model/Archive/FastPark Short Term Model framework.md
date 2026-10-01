# FastPark Forecast Framework
## Research, Development and Production Methodology

---

# Part 1
# Business Problem, Original Forecast and Historical Analysis

---

# 1. Executive Summary

This document records the complete development journey of the FastPark forecasting framework from the original passenger-penetration forecasting approach through:

```text
Original Passenger Forecast
↓
Historical Analysis
↓
Generation 1
↓
Generation 2
↓
Generation 2.1
↓
Production Forecast
```

The objective was not simply to build a forecast.

The objective was to understand:

```text
What actually drives FastPark demand?
```

before deciding:

```text
How FastPark demand should be forecast.
```

Every component included in the final production forecast must be traceable to evidence generated during this process.

Throughout the project the central principle remained:

```text
Evidence before implementation.
```

No forecasting component was included simply because it appeared logical.

Forecasting components were included only when historical analysis and subsequent simulations demonstrated that they contained measurable predictive value.

---

# 2. Business Problem

## Original Forecasting Framework

Prior to redevelopment, FastPark forecasts were primarily based on passenger demand. There was some consideration of current bookings for the next two weeks although there was often a disconnect between the forecast for the next two weeks and the longer term forecast. 

The working assumption was:

```text
Airport Demand
↓
Passenger Forecast
↓
Historical Penetration
↓
FastPark Demand
```

Entry forecasting was effectively:

```text
Future Departing Passengers
×
Historical Entry Penetration
```

Exit forecasting was effectively:

```text
Future Arriving Passengers
×
Historical Exit Penetration
```


- It was easy to explain.
- It used readily available airport forecasts.
- It required limited operational data.
- It was relatively simple to maintain.

---

## Concerns With The Original Forecast

Several concerns existed regarding this approach.

### Bookings Were Highlighted for the first two weeks although were not used after this time. 

The business already possessed future booking information. Whilst this was used for the first two weeks of forecasting it was largely ignored after this time. There was limited evidence to why the uplift numbers were chosen and how to adjust them. 

The original forecast largely ignored:

```text
Known Durations
Known Entry Dates
Known Exit Dates
```

despite these being available before demand occurred.

---

### Operational Behaviour Was Ignored

The original forecast did not utilise:

```text
ExpectedReturnDate
Historical Duration Behaviour
Historical Return Behaviour
```

despite these being observable.

---

### No Explicit Visibility Measurement

The forecast did not quantify:

```text
How much future demand is already known?
```

which ultimately became one of the most important questions in the project.

---

## Primary Research Question

The project therefore began with:

> Are passengers actually the strongest predictor of future FastPark demand?

or alternatively:

> Does the operational booking system already contain stronger forecasting signals?

---

# 3. Research Objectives

Before attempting model development six major research objectives were defined.

---

## Objective 1

Determine how much future demand is visible in bookings.

---

## Objective 2

Measure cancellation and no-show behaviour.

---

## Objective 3

Determine whether duration explains future exits.

---

## Objective 4

Determine whether ExpectedReturnDate explains future exits.

---

## Objective 5

Measure the relationship between passenger demand and FastPark demand.

---

## Objective 6

Determine whether multiple forecasting signals can outperform single-model forecasts.

---

# 4. Historical Analysis

## Purpose

Before developing any forecasting models it was necessary to understand how FastPark actually behaves.

The Historical Analysis phase was intentionally separated from forecasting.

The objective was:

```text
Understanding
```

rather than:

```text
Prediction
```

The purpose of the analysis was to identify which observable variables:

- Explain future entries.
- Explain future exits.
- Are available before demand occurs.
- Could potentially become forecasting signals.

No forecasting models were built at this stage.

The aim was to determine what should be tested in the forecasting stages that followed.

---

# 5. Historical Analysis Dataset

## Booking Data

Source:

```text
AirportX.v_Bookings
```

### Booking Population

| Metric | Value |
|----------|----------:|
| Total Bookings | 393,381 |
| Valid Bookings (B) | 366,584 |
| Cancelled Bookings (CX) | 26,487 |
| Status F Bookings | 310 |

### Interpretation

The booking population is highly clean.

```text
93.19%
```

of all bookings ultimately remained valid.

Only:

```text
6.73%
```

were cancelled.

Unknown status records represented:

```text
0.08%
```

of the entire booking population.

This demonstrates that the booking system itself contains a large volume of potentially usable forecasting information.

---

## Operational Dataset

Source:

```text
FastPark.v_EntryAndExits
```

### Population

```text
685,225 operational records
```

This dataset provides:

- Actual Entries
- Actual Exits
- ExpectedReturnDate
- Return Flight Information (if provided by customer)

and acts as the source of operational truth throughout the project.

---

## Daily Demand Dataset

Generated:

```text
896 observation days
```

containing:

- Entries
- Exits
- Movements
- Net Flow

This later becomes the primary forecasting target.

---

## Hourly Demand Dataset

Generated:

```text
20,319 hourly observations
```

containing:

- Hourly Entries
- Hourly Exits
- Hourly Movements

This later becomes the foundation for hourly forecasting.

---

# Finding 1 — Booking Visibility

## Research Question

How much future FastPark demand is visible before it occurs?

More specifically:

> Can bookings provide a measurable forward-looking signal before operational demand occurs?

---

## Method

Entry and exit booking visibility curves were constructed.

For entries, visibility was measured using:

```text
Booking Creation Timestamp (createdAt)
↓
Planned Entry Date
```

and compared against:

```text
Actual Operational Entries
```

For exits, visibility was measured using:

```text
Booking Creation Timestamp (createdAt)
↓
Planned Exit Date
```

and compared against:

```text
Actual Operational Exits
```

Visibility was measured at:

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

A total of:

```text
10,368 booking visibility observations
```

were generated.

---

## Evidence

### Entry Demand Visibility

Average proportion of final operational entry demand already visible in bookings:

| Horizon | Visibility |
|----------|----------:|
| T-56 | 23.11% |
| T-42 | 30.71% |
| T-28 | 42.61% |
| T-21 | 50.70% |
| T-14 | 59.64% |
| T-10 | 65.86% |
| T-7 | 71.78% |
| T-5 | 77.13% |
| T-3 | 83.49% |
| T-2 | 87.35% |
| T-1 | 92.37% |
| T-0 | 101.20% |

### Exit Demand Visibility

Average proportion of final operational exit demand already visible in bookings:

| Horizon | Visibility |
|----------|----------:|
| T-56 | 28.65% |
| T-42 | 38.51% |
| T-28 | 53.28% |
| T-21 | 62.17% |
| T-14 | 74.03% |
| T-10 | 83.43% |
| T-7 | 92.55% |
| T-5 | 97.63% |
| T-3 | 101.43% |
| T-2 | 102.67% |
| T-1 | 103.53% |
| T-0 | 104.13% |

---

## Findings

### Future Demand Is Visible Long Before It Occurs

Even at:

```text
T-56
```

approximately:

```text
23% of future entries
29% of future exits
```

were already visible.

This demonstrates that substantial future demand exists within the booking system well before operational demand occurs.

---

### Entry Visibility Grows Progressively

Visibility increases consistently:

```text
23%
↓
43%
↓
60%
↓
72%
↓
92%
```

between:

```text
T-56
and
T-1
```

This demonstrates that bookings provide progressively increasing information as demand approaches.

---

### Exit Visibility Is Even Stronger

Exit demand consistently exhibited higher visibility than entry demand.

| Horizon | Entry Visibility | Exit Visibility |
|----------|----------:|----------:|
| T-56 | 23.1% | 28.6% |
| T-28 | 42.6% | 53.3% |
| T-14 | 59.6% | 74.0% |
| T-7 | 71.8% | 92.6% |

This suggests that future exits become visible earlier than future entries.

---

### Visibility Approaches Full Demand

By:

```text
T-1
```

the booking system already contains:

```text
92.4%
```

of final entry demand and:

```text
103.5%
```

of final exit demand.

---

## Interpretation

The booking system contains direct forecasting information.

Demand does not appear unexpectedly.

Instead:

```text
Future Demand
```

is progressively revealed through bookings.

This directly challenges the original forecasting philosophy based solely on:

```text
Passenger Forecast
×
Penetration
```

because bookings themselves contain substantial forward-looking information.

---

## Decision

Booking visibility should become the primary forecasting signal tested in Generation 1.

Future forecasting work should explicitly test:

```text
Booking Visibility Models

Booking Visibility Hybrid Models
```

---

# Finding 2 — Cancellation Behaviour

## Research Question

Do cancellations materially affect future demand?

More specifically:

> Is demand lost randomly, or are there identifiable cancellation patterns that could eventually improve forecast accuracy?

---

## Method

Analyse all cancelled bookings and investigate:

- Cancellation frequency
- Cancellation timing
- Cancellation lead times
- Cancellation duration segments
- Cancellation by airline

---

## Evidence

### Overall Booking Status Distribution

| Status | Bookings | Share |
|----------|----------:|----------:|
| Valid Bookings (B) | 366,584 | 93.19% |
| Cancelled Bookings (CX) | 26,487 | 6.73% |
| Status F | 310 | 0.08% |

Approximately:

```text
1 in every 15 bookings
```

was ultimately cancelled.

---

### Cancellation Timing

| Metric | Value |
|----------|----------:|
| Average Days Before Entry | 40.42 Days |
| Average Time Between Booking Creation And Cancellation | 33.22 Days |

Most cancellations were therefore known well before arrival.

---

### Cancellation By Lead Time

| Lead Time Band | Cancellation Rate |
|----------|----------:|
| 1 Day | 1.44% |
| 2–3 Days | 4.29% |

Customers booking further in advance were materially more likely to cancel.

---

### Cancellation By Duration

| Duration Band | Cancellation Rate |
|----------|----------:|
| 0–1 Days | 9.27% |
| 2–3 Days | 4.72% |
| 4–7 Days | 6.05% |

The highest cancellation rates occurred among very short-duration stays.

---

## Findings

Cancellation behaviour clearly exists.

More importantly:

```text
Cancellation behaviour is not consistent.
```

The probability of cancellation varies according to:

- Lead Time
- Duration
- Customer Behaviour

---

## Interpretation

A booking population should not automatically be interpreted as:

```text
Future Demand
```

because a measurable proportion of demand disappears before arrival.

---

## Decision

Cancellation behaviour should become a future forecasting experiment.

---

# Finding 3 — No Show Behaviour

## Research Question

How many valid bookings fail to become operational demand?

---

## Method

Define:

```text
No Show
=
Valid Booking
+
No Operational Check-In
```

and analyse:

- Overall no-show behaviour
- No-shows by lead time
- No-shows by duration

---

## Evidence

### Overall No Show Rate

| Metric | Value |
|----------|----------:|
| Valid Historical Bookings | 366,584 |
| No Shows | 11,428 |
| No Show Rate | 3.12% |

Approximately:

```text
1 in every 32 valid bookings
```

never arrived.

---

### No Show Rate By Duration

| Duration Band | No Show Rate |
|----------|----------:|
| 0–1 Days | 38.86% |
| 2–3 Days | 9.09% |
| 4–7 Days | 3.02% |

---

### Check-In Validation

| Duration Band | Actual Check-In Rate |
|----------|----------:|
| 0–1 Days | 61.14% |
| 2–3 Days | 90.91% |
| 4–7 Days | 96.98% |

---

## Findings

Short-duration bookings behave very differently from longer-duration bookings.

The reliability of:

```text
Booking Visibility
```

is therefore not constant across the booking population.

---

## Interpretation

Not all bookings carry equal forecasting value.

Booking reliability varies significantly by duration.

---

## Decision

Future forecasting should investigate:

- Duration-specific visibility.
- Duration-specific completion factors.
- Duration-specific forecasting approaches.

---

# Finding 4 — Duration Behaviour

## Research Question

Can stay duration explain future exits?

More importantly:

> Is duration both predictable and observable before the customer exits?

---

## Method

Compare:

```text
Planned Duration
```

against:

```text
Actual Duration
```

for all completed stays.

---

## Evidence

### Population

```text
355,121 completed stays
```

---

### Planned vs Actual Duration

| Metric | Value |
|----------|----------:|
| Average Planned Duration | 8.041 Days |
| Average Actual Duration | 8.013 Days |
| Average Difference | -0.028 Days |
| Median Difference | -0.017 Days |

---

### Translation

Average error:

```text
-0.028 Days
≈ -40 Minutes
```

Median error:

```text
-0.017 Days
≈ -25 Minutes
```

---

## Findings

For more than:

```text
355,000 completed stays
```

planned duration and actual duration were almost identical.

---

## Interpretation

Duration is both:

```text
Observable Before The Event
```

and:

```text
Predictive Of The Event
```

making it one of the strongest forecasting candidates identified during Historical Analysis.

---

## Decision

Duration should become a primary forecasting signal for exits and must be tested directly in Generation 1.

---

# Finding 5 — Expected Return Behaviour

## Research Question

Can ExpectedReturnDate explain future exits?

---

## Method

Compare:

```text
ExpectedReturnDate
```

against:

```text
Actual Exit Timestamp
```

and measure deviation.

---

## Evidence

### Population

```text
355,123 return records
```

---

### Return Deviation Distribution

| Metric | Minutes |
|----------|----------:|
| Mean Deviation | 52.97 |
| 25th Percentile | 26 |
| Median Deviation | 44 |
| 75th Percentile | 70 |

---

### Interpretation Of Distribution

50% of customers returned within:

```text
44 minutes
```

of their expected return time.

75% returned within:

```text
70 minutes
```

of their expected return time.

---

## Findings

ExpectedReturnDate is not perfectly accurate.

However it demonstrates a much stronger relationship with reality than a purely seasonal estimate would provide.

---

## Decision

Expected return behaviour should become a dedicated forecasting candidate.

---

# Finding 6 — Passenger Relationships

## Research Question

Do passengers explain FastPark demand?

---

## Method

Analyse:

```text
Entries ↔ Departing Passenger Demand

Exits ↔ Arriving Passenger Demand
```

using:

- Correlations
- Passenger Mix
- Passenger Bands
- Penetration Analysis

---

## Evidence

### Strongest Entry Driver

| Variable | Correlation |
|----------|----------:|
| International Departing Share | 0.633 |

---

### Strongest Exit Driver

| Variable | Correlation |
|----------|----------:|
| International Arriving Passengers | 0.644 |

---

### Entry Penetration By Passenger Band

| Band | Penetration |
|----------|----------:|
| Lowest Demand Band | 2.39% |
| Highest Demand Band | 1.68% |

---

### Exit Penetration By Passenger Band

| Band | Penetration |
|----------|----------:|
| Lowest Demand Band | 2.23% |
| Highest Demand Band | 1.74% |

---

### Passenger Band Forecast MAE

| Method | MAE |
|----------|----------:|
| Entry Passenger Band Forecast | 70.85 |
| Exit Passenger Band Forecast | 67.04 |

---

## Findings

Passenger demand clearly influences FastPark demand.

However penetration is not stable and varies materially across passenger-volume bands.

---

## Interpretation

Passengers are likely to contain forecasting value, but Historical Analysis alone cannot determine whether passengers outperform booking visibility.

---

## Decision

Passenger-based forecasting must be tested directly in Generation 1.

---

# Historical Analysis Summary

## Strong Candidate Signals


✅ Booking Visibility

✅ Duration (Exits)

✅ Expected Return Behaviour (Exits)

✅ Passenger Demand

---

## Candidate Future Enhancements

⚠  Same-Weekday Behaviour

⚠ Seasonality

⚠ Cancellation Behaviour

⚠ No Show Behaviour

⚠ Duration Segmentation

⚠ Calendar Effects

⚠ Booking Regimes

⚠ Hourly Structures

---

# Outcome

Historical Analysis answered:

> What information appears to drive FastPark demand?

The next question became:

> Which forecasting models can exploit these signals most effectively?

This directly led to:

```text
Generation 1
```

the first FastPark forecasting tournament.

# Part 2
# Generation 1 - Historical Forecast Tournament

---

# 6. Generation 1

## Objective

Historical Analysis successfully identified a number of candidate forecasting signals.

### Entries

Historical Analysis identified:

- Booking Visibility
- Passenger Demand
- Same Weekday Behaviour
- Month-Specific Seasonality

as potentially important forecasting drivers.

---

### Exits

Historical Analysis identified:

- Exit Visibility
- Duration
- Expected Return Behaviour
- Passenger Demand

as potentially important forecasting drivers.

---

However Historical Analysis could not answer an essential question.

> Which forecasting methodologies actually work?

Historical Analysis identified signals.

Generation 1 was designed to identify forecasting models.

---

## Research Question

The central Generation 1 question was:

> If we had stood at a historical point in time and only used information available at that moment, which forecasting methods would have produced the most accurate forecasts?

---

## Design Principles

### No Look-Ahead Bias

Generation 1 was intentionally designed as a genuine forecasting exercise rather than a descriptive analysis.

At:

```text
T-28
```

a model was allowed to use:

- Bookings known by T-28.
- Cancellations known by T-28.
- Historical actuals before T-28.
- Historical passenger relationships before T-28.
- Historical booking curve behaviour before T-28.

A model was not allowed to use:

- Future bookings.
- Future cancellations.
- Future demand.
- Future passengers.

This ensured every forecast represented a legitimate historical forecasting scenario.

---

## Test Design

Generation 1 tested:

```text
182 target dates
```

across:

```text
15 forecast horizons
```

covering:

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

## Forecast Models

### Entries

Generation 1 tested:

```text
E1-E15
```

including:

- Historical Tendency
- Weighted Historical
- Passenger Models
- Booking Visibility Models
- Hybrid Models
- Seasonal Models

---

### Exits

Generation 1 tested:

```text
X1-X17
```

including:

- Historical Tendency
- Weighted Historical
- Passenger Models
- Booking Visibility Models
- Duration Models
- Return Behaviour Models
- Cohort Models
- Seasonal Models

---

# 7. Generation 1 Results

## Entry Forecasting

---

# Short Horizon Entry Results

## T-0

Winner:

```text
E11
Booking Visibility Curve
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 1.11% |
| MAE | 5.04 |
| RMSE | 6.74 |
| Bias | +0.67 |

This was one of the strongest forecasting results observed anywhere in the project.

---

## T-1

Winner:

```text
E11
Booking Visibility Curve
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 3.36% |
| MAE | 15.27 |
| RMSE | 21.49 |
| Bias | -0.44 |

---

## T-2

Winner:

```text
E11
Booking Visibility Curve
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 4.55% |
| MAE | 20.71 |
| RMSE | 28.60 |
| Bias | +0.33 |

---

## T-3

Winner:

```text
E11
Booking Visibility Curve
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 5.27% |
| MAE | 23.96 |
| RMSE | 32.90 |
| Bias | +0.92 |

---

## T-4

Winner:

```text
E11
Booking Visibility Curve
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 5.96% |
| MAE | 27.10 |
| RMSE | 36.71 |
| Bias | +2.32 |

---

## T-5

Winner:

```text
E11
Booking Visibility Curve
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 6.46% |
| MAE | 29.36 |
| RMSE | 39.12 |
| Bias | +3.06 |

---

## T-6

Winner:

```text
E11
Booking Visibility Curve
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 6.96% |
| MAE | 31.65 |
| RMSE | 41.97 |
| Bias | +2.87 |

---

## Key Finding

The first major conclusion from Generation 1 was:

```text
Booking Visibility Dominates Short-Horizon Entry Forecasting.
```

At every horizon between:

```text
T-0
and
T-6
```

the winning forecasting model was:

```text
E11
Booking Visibility Curve
```

which directly validated the booking visibility findings from Historical Analysis.

---

# Medium Horizon Entry Results

## T-7

Winner:

```text
E12
Booking Curve + Same Weekday
```

Performance:

```text
WAPE = 7.68%
```

---

## T-14

Winner:

```text
E12
Booking Curve + Same Weekday
```

Performance:

```text
WAPE = 8.96%
```

---

## T-21

Winner:

```text
E12
Booking Curve + Same Weekday
```

Performance:

```text
WAPE = 10.65%
```

---

## Key Finding

Booking visibility alone stopped being optimal.

The winning models now combined:

```text
Booking Visibility
+
Historical Behaviour
```

demonstrating that:

```text
Bookings contain most of the forecasting signal

BUT

Bookings are improved by historical context.
```

---

# Long Horizon Entry Results

## T-28

Winner:

```text
E15
Weekday-Month + Booking Curve
```

Performance:

```text
WAPE = 12.11%
```

---

## T-35

Winner:

```text
E14
Weekday-Month Historical
```

Performance:

```text
WAPE = 13.04%
```

---

## T-42

Winner:

```text
E14
Weekday-Month Historical
```

Performance:

```text
WAPE = 13.04%
```

---

## T-49

Winner:

```text
E14
Weekday-Month Historical
```

Performance:

```text
WAPE = 13.04%
```

---

## T-56

Winner:

```text
E14
Weekday-Month Historical
```

Performance:

```text
WAPE = 13.04%
```

---

## Key Finding

As visibility weakens:

```text
Seasonality dominates.
```

Historical Analysis suggested month-specific seasonality existed.

Generation 1 demonstrated that:

```text
Seasonality improves forecasting accuracy.
```

---

# Overall Entry Ranking

Across all horizons:

| Rank | Model | Family | WAPE |
|----------:|----------|----------|----------:|
| 1 | E15 Weekday-Month + Booking Curve | Seasonal Hybrid | 10.23% |
| 2 | E12 Booking Curve + Same Weekday | Booking Hybrid | 10.60% |
| 3 | E13 Booking + Passenger Hybrid | Hybrid | 10.80% |
| 4 | E14 Weekday-Month Historical | Seasonal Historical | 12.88% |
| 5 | E11 Booking Visibility Curve | Booking Curve | 13.68% |

---

## Interpretation

The best overall models all shared one common characteristic:

```text
Multiple Signals
```

performed better than:

```text
Single Signals
```

This observation ultimately became one of the key motivations for Generation 2.

---

# Passenger Model Results

Historical Analysis suggested passengers might explain demand.

Generation 1 directly tested this.

### Passenger Models

```text
E9
Same Weekday Passenger Penetration

E10
Average Passenger Penetration
```

Performance:

| Model | Overall WAPE |
|----------|----------:|
| E10 | 16.94% |
| E9 | 17.14% |

---

### Comparison

| Model | Overall WAPE |
|----------|----------:|
| E15 | 10.23% |
| E12 | 10.60% |
| E11 | 13.68% |
| E10 | 16.94% |
| E9 | 17.14% |

---

## Key Finding

Passenger forecasting contained useful information.

However:

```text
Passenger Demand
```

was substantially weaker than:

```text
Booking Visibility
```

at most forecasting horizons.

---

# Incremental Benefit Analysis

One of the most important analyses in Generation 1 compared:

```text
Booking Visibility
```

against:

```text
Booking Visibility
+
Additional Signal
```

---

## Weekday Added To Booking Curve

### T-7

```text
E11
7.69%

↓

E12
7.68%
```

Improvement:

```text
0.01 percentage points
```

Very small.

---

### T-14

```text
E11
12.91%

↓

E12
8.96%
```

Improvement:

```text
3.95 percentage points
```

---

### T-21

```text
E11
16.78%

↓

E12
10.65%
```

Improvement:

```text
6.14 percentage points
```

---

### T-56

```text
E11
32.97%

↓

E12
19.78%
```

Improvement:

```text
13.19 percentage points
```

---

## Key Finding

Historical behaviour becomes increasingly valuable as booking visibility weakens.

---

# Entry Forecasting Conclusions

Generation 1 demonstrated that:

### Short Horizons

```text
Bookings Dominate
```

---

### Medium Horizons

```text
Bookings
+
Historical Weekday Behaviour
```

perform best.

---

### Long Horizons

```text
Month-Specific Seasonality
```

becomes increasingly important.

---

### Passenger Demand

Contains useful information.

However:

```text
Passengers
≠
Best Forecast
```

---

# Generation 1 Entry Forecasting Decision

Generation 1 identified:

```text
Booking Visibility

Historical Behaviour

Seasonality
```

as the strongest forecasting signals.

However:

```text
No Single Model
```

won every horizon.

This raised a new question:

> Instead of selecting a single winner, can we build a weighted combination of the strongest signals?

This directly motivated:

```text
Generation 2
```

which moved from:

```text
Best Model
```

to:

```text
Best Combination Of Signals
```

# 8. Exit Forecasting Results

## Research Question

Historical Analysis identified four major candidate forecasting signals for exits:

```text
Booking Visibility
Duration
Expected Return Behaviour
Passenger Demand
```

Generation 1 was designed to answer:

> Which forecasting methodologies make the best use of these signals?

---

## Core Hypothesis

Historical Analysis demonstrated:

- Exit demand becomes visible before it occurs.
- Planned duration closely matches actual duration.
- ExpectedReturnDate contains meaningful behavioural information.
- Passenger demand exhibits moderate correlation with exit volumes.

Generation 1 tests whether those relationships can be converted into accurate forecasting models.

---

# Exit Winners By Horizon

| Horizon | Winner | WAPE |
|----------|----------|----------:|
| T-0 | X11 Exit Booking Visibility Curve | 2.87% |
| T-1 | X11 Exit Booking Visibility Curve | 2.96% |
| T-2 | X11 Exit Booking Visibility Curve | 3.15% |
| T-3 | X12 Exit Curve + Duration | 3.22% |
| T-4 | X12 Exit Curve + Duration | 3.41% |
| T-5 | X12 Exit Curve + Duration | 3.52% |
| T-6 | X12 Exit Curve + Duration | 3.75% |
| T-7 | X12 Exit Curve + Duration | 4.07% |
| T-14 | X12 Exit Curve + Duration | 7.64% |
| T-21 | X15 Exit Booking + Passenger Hybrid | 9.96% |
| T-28 | X15 Exit Booking + Passenger Hybrid | 11.11% |
| T-35 | X15 Exit Booking + Passenger Hybrid | 12.66% |
| T-42 | X15 Exit Booking + Passenger Hybrid | 14.24% |
| T-49 | X17 Weekday-Month Historical | 14.85% |
| T-56 | X17 Weekday-Month Historical | 14.85% |

---

# Short Horizon Exit Results

## T-0

Winner:

```text
X11
Exit Booking Visibility Curve
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 2.87% |
| MAE | 13.08 |
| RMSE | 16.31 |
| Bias | +0.33 |

### Interpretation

At:

```text
T-0
```

a pure booking visibility forecast was capable of explaining almost all future exit demand.

This immediately validated the Historical Analysis finding that:

```text
Future exit demand is highly visible before the exit occurs.
```

---

## T-1

Winner:

```text
X11
Exit Booking Visibility Curve
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 2.96% |
| MAE | 13.47 |
| RMSE | 16.91 |
| Bias | +0.50 |

---

## T-2

Winner:

```text
X11
Exit Booking Visibility Curve
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 3.15% |
| MAE | 14.33 |
| RMSE | 18.77 |
| Bias | -0.01 |

---

### Key Finding

For:

```text
T-0
T-1
T-2
```

the dominant forecasting signal was:

```text
Exit Visibility
```

alone.

---

# Duration Begins To Dominate

## T-3

Winner:

```text
X12
Exit Curve + Duration
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 3.22% |
| MAE | 14.66 |
| RMSE | 20.15 |
| Bias | -1.18 |

---

## T-4

Winner:

```text
X12
Exit Curve + Duration
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 3.41% |
| MAE | 15.54 |
| RMSE | 21.20 |
| Bias | -0.77 |

---

## T-5

Winner:

```text
X12
Exit Curve + Duration
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 3.52% |
| MAE | 16.02 |
| RMSE | 21.66 |
| Bias | -1.30 |

---

## T-6

Winner:

```text
X12
Exit Curve + Duration
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 3.75% |
| MAE | 17.08 |
| RMSE | 22.76 |
| Bias | -1.50 |

---

## T-7

Winner:

```text
X12
Exit Curve + Duration
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 4.07% |
| MAE | 18.53 |
| RMSE | 23.87 |
| Bias | -1.81 |

---

## T-14

Winner:

```text
X12
Exit Curve + Duration
```

Performance:

| Metric | Value |
|----------|----------:|
| WAPE | 7.64% |
| MAE | 34.81 |
| RMSE | 44.01 |
| Bias | +0.55 |

---

## Key Finding

Once horizons extended beyond:

```text
T-2
```

the dominant forecasting signal became:

```text
Exit Visibility
+
Duration
```

rather than visibility alone.

This was one of the strongest validations of the Historical Analysis phase.

---

# Duration Contribution

## Historical Analysis Evidence

Duration validation demonstrated:

| Metric | Value |
|----------|----------:|
| Completed Stays Analysed | 355,121 |
| Average Planned Duration | 8.041 Days |
| Average Actual Duration | 8.013 Days |
| Average Difference | -0.028 Days |
| Median Difference | -0.017 Days |

Equivalent to:

```text
Average Error ≈ -40 Minutes
Median Error ≈ -25 Minutes
```

---

## Generation 1 Evidence

The repeated appearance of:

```text
X12
Exit Curve + Duration
```

among winning models demonstrated that duration is not simply a descriptive operational metric.

Duration directly improves forecasting accuracy.

---

## Interpretation

Duration satisfies two forecasting requirements:

### Requirement 1

```text
Known Before Exit Occurs
```

### Requirement 2

```text
Predictive Of Exit Behaviour
```

Very few variables satisfy both conditions simultaneously.

---

## Conclusion

Duration became one of the strongest production forecast candidates identified anywhere in the project.

---

# Passenger Contribution

## Passenger Models Tested

```text
X9
Same Weekday Passenger Penetration

X10
Average Passenger Penetration
```

---

## Why Were Passenger Models Tested?

Historical Analysis demonstrated:

```text
Strongest External Exit Driver

International Arriving Passengers

Correlation = 0.644
```

suggesting that passenger demand may help explain future exits.

---

## Overall Passenger Model Performance

| Rank | Model | Family | WAPE |
|----------:|----------|----------|----------:|
| 9 | X9 Same Weekday Passenger Penetration | Passenger Driver | 16.45% |
| 11 | X10 Average Passenger Penetration | Passenger Driver | 16.49% |

---

## Comparison

| Rank | Model | WAPE |
|----------:|----------|----------:|
| 1 | X15 Exit Booking + Passenger Hybrid | 10.20% |
| 2 | X12 Exit Curve + Duration | 10.34% |
| 3 | X11 Exit Booking Visibility Curve | 11.42% |
| 9 | X9 Same Weekday Passenger Penetration | 16.45% |
| 11 | X10 Average Passenger Penetration | 16.49% |

---

## Interpretation

Passenger demand clearly contains information.

However:

```text
Passenger Demand
```

contains substantially less forecasting information than:

```text
Booking Visibility
```

and:

```text
Duration
```

---

## Conclusion

Passenger demand should remain a supporting signal rather than a primary production forecasting signal.

---

# Hybrid Forecast Evidence

One of the most important discoveries in Generation 1 was the effectiveness of hybrid models.

## Exit Booking + Passenger Hybrid

Optimal blend weights varied significantly by horizon.

| Horizon | Booking Weight | Passenger Weight | WAPE |
|----------|----------:|----------:|----------:|
| T-0 | 100% | 0% | 2.87% |
| T-7 | 90% | 10% | 4.83% |
| T-14 | 75% | 25% | 7.25% |
| T-21 | 65% | 35% | 9.26% |
| T-28 | 60% | 40% | 10.91% |
| T-35 | 45% | 55% | 12.61% |
| T-42 | 40% | 60% | 13.85% |
| T-49 | 35% | 65% | 15.13% |
| T-56 | 35% | 65% | 15.78% |

---

## Key Finding

As booking visibility weakened:

```text
Passenger Information
```

became increasingly useful.

However:

```text
Passenger Information
```

never fully replaced booking visibility.

---

# Return Behaviour Contribution

## Historical Analysis Evidence

Return behaviour analysis used:

```text
355,123 return records
```

and demonstrated:

| Metric | Minutes |
|----------|----------:|
| Mean Deviation | 52.97 |
| Median Deviation | 44 |
| 75th Percentile | 70 |

---

## Generation 1 Return Models

```text
X13
Expected Return Cohort

X14
Expected Return + Historical Deviation
```

were created specifically to test whether ExpectedReturnDate could be converted into a forecasting signal.

---

## Conclusion

Return behaviour contains forecasting information.

However:

```text
Expected Return Models
```

did not consistently outperform:

```text
Booking Visibility
+
Duration
```

and therefore remained a candidate forecasting signal rather than a dominant framework.

---

# Long Horizon Exit Results

As horizons extended:

```text
T-21
T-28
T-35
T-42
T-49
T-56
```

the strongest models increasingly became:

```text
X15
Exit Booking + Passenger Hybrid

X17
Weekday-Month Historical
```

---

## Interpretation

The exit forecasting problem evolved exactly like the entry forecasting problem.

### Short Horizon

```text
Known Information
```

dominates.

### Long Horizon

```text
Historical Structure
```

dominates.

---

# Overall Exit Ranking

Across all horizons:

| Rank | Model | Family | WAPE |
|----------:|----------|----------|----------:|
| 1 | X15 Exit Booking + Passenger Hybrid | Hybrid | 10.20% |
| 2 | X12 Exit Curve + Duration | Booking Curve + Duration | 10.34% |
| 3 | X11 Exit Booking Visibility Curve | Booking Curve | 11.42% |
| 4 | X17 Weekday-Month Historical | Seasonal Historical | 14.30% |
| 5 | X7 Weighted Last 6 Same Weekdays | Weighted Historical | 15.92% |
| 6 | X6 Weighted Last 4 Same Weekdays | Weighted Historical | 15.93% |
| 7 | X3 Average Last 4 Same Weekdays | Historical Tendency | 16.05% |
| 8 | X8 Weighted Last 8 Same Weekdays | Weighted Historical | 16.18% |
| 9 | X9 Same Weekday Passenger Penetration | Passenger Driver | 16.45% |
| 10 | X4 Average Last 6 Same Weekdays | Historical Tendency | 16.45% |

---

## Interpretation

The Exit rankings differ materially from the Entry rankings.

Entries were dominated by:

```text
Booking Visibility
+
Seasonality
```

Exits were dominated by:

```text
Booking Visibility
+
Duration
```

The second-best overall model:

```text
X12
Exit Curve + Duration
```

provides direct experimental evidence that Duration is one of the strongest forecasting signals in the entire project.

---

# Overall Exit Conclusions

Generation 1 demonstrated four major principles.

## Principle 1

```text
Exit Visibility Works
```

Future exits are partially visible before they occur.

---

## Principle 2

```text
Duration Matters
```

Duration repeatedly improved forecasting performance and survived every stage of the forecasting journey.

---

## Principle 3

```text
Expected Return Behaviour Contains Signal
```

but not enough signal to dominate visibility and duration.

---

## Principle 4

```text
Seasonality Matters At Longer Horizons
```

As visibility weakens:

```text
Weekday-Month Behaviour
```

becomes increasingly important.

---

# Generation 1 Overall Conclusions

Generation 1 successfully answered:

> Which forecasting methods work?

### Entry Forecasting

Short Horizons:

```text
Booking Visibility
```

Medium Horizons:

```text
Booking Visibility
+
Historical Behaviour
```

Long Horizons:

```text
Seasonality
```

---

### Exit Forecasting

Short Horizons:

```text
Exit Visibility
+
Duration
```

Long Horizons:

```text
Seasonality
+
Hybrid Forecasts
```

---

# What Generation 1 Could Not Answer

Generation 1 identified:

```text
The Best Models
```

but not:

```text
The Best Combination Of Signals
```

Different models captured different useful information.

Examples:

```text
E11
Booking Visibility

E12
Booking Visibility + Weekday

E14
Seasonality

X12
Duration

X17
Seasonality
```

This naturally raised a new question:

> Why choose only one model if multiple models are capturing useful information?

---

# Generation 1 Final Decision

Generation 1 demonstrated that:

```text
No Single Model
```

was optimal across every forecasting horizon.

The next logical step was therefore not:

```text
Build More Models
```

but:

```text
Combine The Strongest Signals
```

in a structured and evidence-based manner.

This directly motivated:

```text
Generation 2
```

which moved the framework from:

```text
Best Individual Forecast
```

toward:

```text
Best Combination Of Forecast Signals
```

# Part 3
# Generation 2 - Ensemble Learning, Signal Weighting and Horizon-Specific Forecasting

---

# 9. Why Generation 2 Was Created

Historical Analysis answered:

> What information appears to drive FastPark demand?

Generation 1 answered:

> Which forecasting models perform best?

However Generation 1 also revealed a deeper finding.

The forecasting problem was not being solved by a single model.

Different horizons consistently selected different winning models.

### Entry Winners By Horizon

| Horizon | Winner | WAPE |
|----------|----------|----------:|
| T-0 | E11 Booking Visibility Curve | 1.11% |
| T-7 | E12 Booking Curve + Same Weekday | 7.68% |
| T-14 | E12 Booking Curve + Same Weekday | 8.96% |
| T-21 | E12 Booking Curve + Same Weekday | 10.65% |
| T-28 | E15 Weekday-Month + Booking Curve | 12.11% |
| T-35 | E14 Weekday-Month Historical | 13.04% |
| T-42 | E14 Weekday-Month Historical | 13.04% |
| T-49 | E14 Weekday-Month Historical | 13.04% |
| T-56 | E14 Weekday-Month Historical | 13.04% |

---

### Exit Winners By Horizon

| Horizon | Winner | WAPE |
|----------|----------|----------:|
| T-0 | X11 Exit Booking Visibility Curve | 2.87% |
| T-1 | X11 Exit Booking Visibility Curve | 2.96% |
| T-2 | X11 Exit Booking Visibility Curve | 3.15% |
| T-3 | X12 Exit Curve + Duration | 3.22% |
| T-4 | X12 Exit Curve + Duration | 3.41% |
| T-5 | X12 Exit Curve + Duration | 3.52% |
| T-6 | X12 Exit Curve + Duration | 3.75% |
| T-7 | X12 Exit Curve + Duration | 4.07% |
| T-14 | X12 Exit Curve + Duration | 7.64% |
| T-21 | X15 Exit Booking + Passenger Hybrid | 9.96% |
| T-28 | X15 Exit Booking + Passenger Hybrid | 11.11% |
| T-35 | X15 Exit Booking + Passenger Hybrid | 12.66% |
| T-42 | X15 Exit Booking + Passenger Hybrid | 14.24% |
| T-49 | X17 Weekday-Month Historical | 14.85% |
| T-56 | X17 Weekday-Month Historical | 14.85% |

---

## Core Observation

Generation 1 demonstrated that:

```text
Different Horizons
Require Different Forecast Logic
```

Examples:

### Entries

```text
T-0
→ Visibility

T-7
→ Visibility + Historical Behaviour

T-28
→ Visibility + Seasonality

T-56
→ Seasonality
```

---

### Exits

```text
T-0
→ Exit Visibility

T-7
→ Exit Visibility + Duration

T-21
→ Exit Visibility + Passenger Context

T-56
→ Seasonality
```

The implication was significant.

The forecasting problem was no longer:

```text
Which model wins?
```

The forecasting problem had become:

```text
What combination of signals wins?
```

This directly motivated Generation 2.

---

# 10. Research Question

The central Generation 2 question was:

> What is the optimal combination of forecasting signals at each forecast horizon?

Rather than selecting:

```text
E11

or

E12

or

E15
```

Generation 2 attempted to learn:

```text
Booking Weight
Weekday Weight
Seasonality Weight
Trend Weight
```

at each horizon independently.

---

# 11. Methodological Improvements

Generation 2 introduced four major changes.

---

## Improvement 1 — All-Date Testing

Generation 1 tested:

```text
Days 1–7
Days 14–20
```

of each month.

Generation 2 tested:

```text
Every Calendar Day
```

within the simulation period.

This significantly increased the sample size and reduced date-selection bias.

---

## Improvement 2 — Rolling-Origin Validation

Generation 2 introduced:

```text
Training
Validation
Test
```

rolling monthly folds.

Example:

```text
Training
Jul 2025 → Dec 2025

Validation
Jan 2026

Test
Feb 2026
```

followed by:

```text
Training
Jul 2025 → Jan 2026

Validation
Feb 2026

Test
Mar 2026
```

This ensured:

```text
Future periods could not select their own weights.
```

and produced a much more realistic estimate of production performance.

---

## Improvement 3 — Signal Reduction

Generation 1 tested:

```text
32 forecasting models
```

Generation 2 reduced the problem to its strongest independent forecasting signals.

### Entry Components

```text
Booking Visibility

Same Weekday

Weekday-Month

Trend-Adjusted Month
```

---

### Exit Components

```text
Booking Visibility

Duration

Same Weekday

Trend-Adjusted Month
```

Only signals that repeatedly demonstrated forecasting value in Generation 1 were retained.

---

## Improvement 4 — Booking Shrinkage

Generation 2 introduced booking shrinkage.

Purpose:

```text
Reduce Instability At Long Horizons
```

When:

```text
Booking Visibility Is Weak
```

the forecast partially reverts toward:

```text
Historical Behaviour
```

rather than fully trusting sparse booking information.

---

# 12. Entry Forecast Results

## Research Question

How should entry forecasting signals be weighted?

---

# T-0 Entry Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Booking | 100.0% |
| Weekday | 0.0% |
| Month | 0.0% |
| Trend | 0.0% |

Training WAPE:

```text
1.11%
```

Validation WAPE:

```text
0.88%
```

---

## Interpretation

Generation 2 independently rediscovered the same conclusion reached in Generation 1:

```text
T-0 Forecasting
=
Booking Visibility Problem
```

No historical component materially improved the forecast.

---

# T-14 Entry Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Booking | 77.5% |
| Month | 22.5% |
| Weekday | 0.0% |
| Trend | 0.0% |

Validation WAPE:

```text
9.56%
```

---

## Interpretation

Booking visibility remains dominant.

However:

```text
Month-Specific Seasonality
```

is now contributing approximately:

```text
22.5%
```

of the final forecast.

---

# T-21 Entry Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Booking | 62.5% |
| Month | 37.5% |

Validation WAPE:

```text
11.27%
```

---

## Interpretation

At approximately three weeks before arrival:

```text
Seasonality
```

is almost as important as:

```text
Booking Visibility.
```

---

# T-35 Entry Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Booking | 62.5% |
| Weekday | 37.5% |

Validation WAPE:

```text
8.82%
```

---

## Interpretation

Historical weekday structure is now contributing significant forecasting value.

This confirms Generation 1 findings that:

```text
Weekday Behaviour
```

remains predictive beyond booking visibility.

---

# T-42 Entry Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Booking | 37.5% |
| Weekday | 20.0% |
| Trend | 42.5% |

Shrinkage Strength:

```text
50
```

Validation WAPE:

```text
10.02%
```

---

## Interpretation

Trend has now become the largest signal.

Generation 2 is effectively saying:

```text
Recent Demand Behaviour
```

contains more forecasting information than:

```text
Current Booking Position
```

at this horizon.

---

# T-49 Entry Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Booking | 22.5% |
| Weekday | 17.5% |
| Trend | 60.0% |

Validation WAPE:

```text
11.24%
```

---

# T-56 Entry Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Booking | 35.0% |
| Weekday | 50.0% |
| Month | 7.5% |
| Trend | 7.5% |

Validation WAPE:

```text
9.95%
```

---

## Evolution Of Entry Forecasting

The forecasting philosophy changes dramatically by horizon.

| Horizon | Booking | Historical Structure |
|----------|----------:|----------:|
| T-0 | 100% | 0% |
| T-14 | 77.5% | 22.5% |
| T-21 | 62.5% | 37.5% |
| T-42 | 37.5% | 62.5% |
| T-49 | 22.5% | 77.5% |
| T-56 | 35.0% | 65.0% |

---

## Conclusion

Generation 2 proved:

```text
Short Horizons
=
Visibility Problem

Long Horizons
=
Historical Behaviour Problem
```


---

# 13. Entry Weight Stability

Weight stability determines whether the optimiser repeatedly reaches the same conclusion.

---

## T-0

Average weights across all folds:

| Signal | Average Weight |
|----------|----------:|
| Booking | 99.17% |
| Weekday | 0.00% |
| Month | 0.42% |
| Trend | 0.42% |

Average Validation WAPE:

```text
1.26%
```

---

## T-14

Average Weights:

| Signal | Average Weight |
|----------|----------:|
| Booking | 58.75% |
| Weekday | 19.58% |
| Month | 21.67% |
| Trend | 0.00% |

Average Validation WAPE:

```text
9.12%
```

---

## T-56

Average Weights:

| Signal | Average Weight |
|----------|----------:|
| Booking | 16.25% |
| Weekday | 12.92% |
| Month | 69.58% |
| Trend | 1.25% |

Average Validation WAPE:

```text
10.49%
```

---

## Interpretation

The strongest conclusions remained stable across folds.

This provides strong evidence that:

```text
Booking Visiblity

Seasonality

Trend
```

are genuinely useful forecasting signals rather than statistical noise.

---

# 14. Exit Forecast Results

## Research Question

How should exit forecasting signals be weighted?

Historical Analysis demonstrated:

```text
Duration
```

was likely to matter.

Generation 1 demonstrated:

```text
Duration
+
Exit Visibility
```

consistently outperformed visibility alone.

Generation 2 was designed to quantify exactly how important each signal was across the forecast horizon.

---

# Exit Forecast Winners from Generation 1

Before attempting optimisation, the strongest models identified by Generation 1 were:

| Horizon | Winner | WAPE |
|----------|----------|----------:|
| T-0 | X11 Exit Booking Visibility Curve | 2.87% |
| T-1 | X11 Exit Booking Visibility Curve | 2.96% |
| T-2 | X11 Exit Booking Visibility Curve | 3.15% |
| T-3 | X12 Exit Curve + Duration | 3.22% |
| T-4 | X12 Exit Curve + Duration | 3.41% |
| T-5 | X12 Exit Curve + Duration | 3.52% |
| T-6 | X12 Exit Curve + Duration | 3.75% |
| T-7 | X12 Exit Curve + Duration | 4.07% |
| T-14 | X12 Exit Curve + Duration | 7.64% |
| T-21 | X15 Exit Booking + Passenger Hybrid | 9.96% |
| T-28 | X15 Exit Booking + Passenger Hybrid | 11.11% |
| T-35 | X15 Exit Booking + Passenger Hybrid | 12.66% |
| T-42 | X15 Exit Booking + Passenger Hybrid | 14.24% |
| T-49 | X17 Weekday-Month Historical | 14.85% |
| T-56 | X17 Weekday-Month Historical | 14.85% |

This progression suggested:

```text
Visibility
↓
Visibility + Duration
↓
Visibility + Hybrid Signals
↓
Seasonality
```

The purpose of Generation 2 was to determine whether this transition could be quantified through signal weighting.

---

# T-0 Exit Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Exit Booking | 87.5% |
| Duration | 2.5% |
| Weekday | 7.5% |
| Trend | 2.5% |

Training WAPE:

```text
3.04%
```

Validation WAPE:

```text
2.66%
```

---

## Interpretation

Short-horizon exits remain primarily a:

```text
Booking Visibility Problem
```

However, unlike entries, Generation 2 still assigned small weight to:

```text
Duration
Weekday Behaviour
Trend
```

indicating that exit forecasting is inherently more complex than entry forecasting.

---

# T-3 Exit Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Exit Booking | 0.0% |
| Duration | 90.0% |
| Weekday | 10.0% |
| Trend | 0.0% |

Validation WAPE:

```text
3.51%
```

---

## Interpretation

This is one of the most important findings in the entire project.

At:

```text
T-3
```

the optimiser completely discarded booking visibility and overwhelmingly preferred:

```text
Duration
```

This was powerful evidence that:

```text
Duration Is Not Merely Descriptive
```

It is a primary forecasting signal.

---

# T-7 Exit Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Exit Booking | 25.0% |
| Duration | 70.0% |
| Weekday | 5.0% |
| Trend | 0.0% |

Validation WAPE:

```text
4.36%
```

---

## Interpretation

Even one week before demand:

```text
Duration
```

remains the dominant forecasting signal.

Booking visibility contributes, but no longer dominates.

---

# T-14 Exit Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Exit Booking | 7.5% |
| Duration | 77.5% |
| Weekday | 15.0% |
| Trend | 0.0% |

Validation WAPE:

```text
7.15%
```

---

## Interpretation

Duration remains overwhelmingly important at two weeks before demand.

This is a striking contrast with entry forecasting, where booking visibility remained dominant at similar horizons.

---

# T-21 Exit Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Exit Booking | 40.0% |
| Duration | 25.0% |
| Weekday | 35.0% |
| Trend | 0.0% |

Validation WAPE:

```text
7.67%
```

---

## Interpretation

No single signal dominates.

The optimiser begins blending:

```text
Visibility
Duration
Historical Behaviour
```

creating a more balanced forecast.

---

# T-28 Exit Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Exit Booking | 37.5% |
| Duration | 0.0% |
| Weekday | 57.5% |
| Trend | 5.0% |

Validation WAPE:

```text
6.32%
```

---

## Interpretation

This was one of the most surprising Generation 2 results.

Duration disappears entirely.

The dominant forecasting signal becomes:

```text
Weekday Behaviour
```

demonstrating that different horizons contain fundamentally different forecasting information.

---

# T-42 Exit Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Exit Booking | 0.0% |
| Duration | 42.5% |
| Weekday | 0.0% |
| Trend | 57.5% |

Validation WAPE:

```text
6.97%
```

---

## Interpretation

Trend becomes the largest forecasting signal.

The optimiser is effectively using:

```text
Recent Exit Behaviour
```

rather than:

```text
Current Exit Visibility
```

to predict future demand.

---

# T-49 Exit Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Exit Booking | 0.0% |
| Duration | 50.0% |
| Trend | 50.0% |
| Weekday | 0.0% |

Validation WAPE:

```text
7.13%
```

---

# T-56 Exit Forecast

Recommended Weights:

| Signal | Weight |
|----------|----------:|
| Exit Booking | 0.0% |
| Duration | 82.5% |
| Weekday | 5.0% |
| Trend | 12.5% |

Validation WAPE:

```text
7.08%
```

---

## Interpretation

Even at the longest forecast horizon tested:

```text
Duration
```

remains the dominant forecasting signal.

This was one of the strongest validations of the Historical Analysis duration findings.

---

# Exit Weight Evolution

The evolution of exit weights tells a fundamentally different story from entries.

| Horizon | Exit Booking | Duration | Weekday | Trend |
|----------|----------:|----------:|----------:|----------:|
| T-0 | 87.5% | 2.5% | 7.5% | 2.5% |
| T-3 | 0.0% | 90.0% | 10.0% | 0.0% |
| T-7 | 25.0% | 70.0% | 5.0% | 0.0% |
| T-14 | 7.5% | 77.5% | 15.0% | 0.0% |
| T-21 | 40.0% | 25.0% | 35.0% | 0.0% |
| T-28 | 37.5% | 0.0% | 57.5% | 5.0% |
| T-42 | 0.0% | 42.5% | 0.0% | 57.5% |
| T-49 | 0.0% | 50.0% | 0.0% | 50.0% |
| T-56 | 0.0% | 82.5% | 5.0% | 12.5% |

---

## Major Discovery

Generation 2 demonstrated that:

```text
Entry Forecasting
=
Visibility Problem
```

whereas:

```text
Exit Forecasting
=
Visibility
+
Duration
+
Historical Behaviour Problem
```

The two demand streams behave fundamentally differently.

---

# Exit Weight Stability

Weight stability verifies that the optimiser repeatedly reached similar conclusions across independent folds.

## T-0

Average Weights:

| Signal | Average Weight |
|----------|----------:|
| Exit Booking | 70.83% |
| Duration | 23.75% |
| Weekday | 4.58% |
| Trend | 0.83% |

Average Validation WAPE:

```text
2.64%
```

---

## T-14

Average Weights:

| Signal | Average Weight |
|----------|----------:|
| Exit Booking | 13.33% |
| Duration | 65.83% |
| Weekday | 10.83% |
| Trend | 10.00% |

Average Validation WAPE:

```text
7.05%
```

---

## T-56

Average Weights:

| Signal | Average Weight |
|----------|----------:|
| Exit Booking | 11.25% |
| Duration | 26.25% |
| Weekday | 40.42% |
| Trend | 22.08% |

Average Validation WAPE:

```text
13.33%
```

---

## Interpretation

The optimiser repeatedly selected:

```text
Duration
```

across independent validation folds.

This strongly indicates that:

```text
Duration Is A Genuine Forecast Driver
```

rather than a statistical artefact.

---

# Exit Forecast Conclusions

Generation 2 demonstrated:

### Principle 1

Exit Visibility remains valuable at short horizons.

---

### Principle 2

Duration becomes the dominant forecasting signal at many short and medium horizons.

---

### Principle 3

Historical behaviour increasingly dominates as visibility weakens.

---

### Principle 4

Exit forecasting behaves fundamentally differently from entry forecasting.

---

### Principle 5

Duration survived every stage of the forecasting journey:

```text
Historical Analysis
↓
Generation 1
↓
Generation 2
```

making it one of the strongest candidates for production implementation.

---

# 17. What Entered Production

Generation 2 directly contributed:

✅ Rolling-Origin Validation

✅ Ensemble Forecasting

✅ Horizon-Specific Weighting

✅ Booking Shrinkage

✅ Weight Stability Monitoring

✅ Signal Weight Learning

The direct output became:

```text
forecast_weights.csv
```

which forms the basis of the Production Forecast.

---

# 18. What Generation 2 Could Not Answer

Generation 2 optimised:

```text
Signal Weighting
```

but treated several behavioural variables as static.

It did not investigate:

- Booking Pace
- Cancellation Behaviour
- Calendar Effects
- Booking Regimes
- Alternative Historical Windows
- Weekday-Specific Trend Effects

Those remained unresolved.

---

# Part 4
# Generation 2.1 - Behavioural Feature Engineering and Non-Price Optimisation

---

# 19. Why Generation 2.1 Was Created

Generation 2 successfully answered:

> What is the optimal combination of forecasting signals?

By the end of Generation 2 the framework had learned:

- How much booking visibility to use.
- How much historical behaviour to use.
- How much seasonality to use.
- How much trend to use.

Generation 2 demonstrated that:

```text
Forecasting performance is horizon-specific.
```

Different horizons preferred dramatically different signal combinations.

For example:

| Horizon | Booking | Historical Structure |
|----------|----------:|----------:|
| T-0 | 100% | 0% |
| T-14 | 77.5% | 22.5% |
| T-21 | 62.5% | 37.5% |
| T-42 | 37.5% | 62.5% |
| T-56 | 35.0% | 65.0% |

---

However Generation 2 still treated many observable variables as static.

For example:

```text
100 bookings
```

was treated identically regardless of whether:

```text
Bookings were accelerating rapidly
```

or:

```text
Bookings were barely growing.
```

Similarly:

```text
A holiday period
```

was treated identically to:

```text
A normal trading period.
```

And:

```text
A period of unusually high cancellations
```

was treated identically to:

```text
A normal cancellation environment.
```

Generation 2.1 therefore investigated:

```text
Behavioural Features
```

rather than:

```text
Forecast Signals.
```

---


# 20. Generation 2.1 - Behavioural Feature Engineering

## Research Question

Generation 2 successfully identified the optimal weighting of forecasting signals across all forecast horizons.

However, a further question remained:

```text
Can forecasting accuracy be improved
through behavioural and operational context?
```

Generation 2 used:

```text
Booking Visibility
Weekday Behaviour
Seasonality
Trend
Duration
```

but treated many operational scenarios identically.

Generation 2.1 therefore investigated whether additional:

```text
Behavioural Features
```

could improve the already optimised Generation 2 framework.

---

# Features Tested

Six feature groups were evaluated.

| Feature | Purpose |
|----------|----------|
| Booking Pace | Measure booking growth velocity |
| Historical Window Selection | Optimise same-weekday lookback periods |
| Calendar Effects | Model holidays and events |
| Booking Regimes | Model demand environments |
| Weekday-Specific Trend | Model weekday-level momentum |
| Cancellation Behaviour | Model real-time cancellation activity |

Each feature was tested independently against the final Generation 2 baseline.

---

# 21. Booking Pace

## Hypothesis

Generation 2 understands:

```text
Current Booking Position
```

Generation 2.1 investigated whether:

```text
Booking Growth Rate
```

contains additional forecasting information.

Two days may both have:

```text
100 visible bookings
```

but one day may be gaining bookings rapidly while another may be experiencing little growth.

---

## Method

Booking pace was calculated using booking growth between forecast horizons.

Each forecast was categorised into:

```text
Very Low
Low
Normal
High
Very High
```

booking-pace environments.

Adjustments were fitted using training-period behaviour only.

---

## Entry Results

### T-1

| Metric | Value |
|----------|----------:|
| Baseline WAPE | 4.508% |
| Booking Pace WAPE | 4.322% |
| Improvement | +0.186 points |
| Relative Reduction | 4.1% |
| Win Rate | 66.7% |

### T-2

| Metric | Value |
|----------|----------:|
| Baseline WAPE | 5.742% |
| Booking Pace WAPE | 5.470% |
| Improvement | +0.272 points |
| Relative Reduction | 4.7% |
| Win Rate | 83.3% |

### Longer Horizons

Improvements remained visible at some longer horizons:

| Horizon | Baseline | Booking Pace |
|----------|----------:|----------:|
| T-28 | 11.46% | 11.31% |
| T-42 | 12.09% | 10.73% |
| T-56 | 12.20% | 10.50% |

---

## Exit Results

Performance gains were generally smaller but remained positive at selected horizons.

Booking pace contributed useful information beyond:

```text
Exit Visibility
Duration
```

particularly when visibility was still evolving.

---

## Interpretation

Booking pace captures something fundamentally different from booking visibility.

Generation 2 answers:

```text
Where demand currently is.
```

Booking pace answers:

```text
Where demand is moving.
```

---

## Decision

✅ Influenced Production Forecast

Booking Pace demonstrated genuine forecasting value, although benefits were horizon-specific rather than universal.

---

# 22. Cancellation Behaviour

## Hypothesis

Historical Analysis demonstrated:

```text
Cancellation behaviour is not random.
```

Generation 2.1 investigated whether unusual cancellation activity provided additional forecasting information beyond visible bookings.

---

## Method

Three cancellation windows were tested:

```text
1 Day
3 Day
7 Day
```

Cancellation behaviour was classified relative to historical norms as:

```text
Very Low
Low
Normal
High
Very High
```

No future information was used.

Only cancellations known at the historical forecast cutoff were included.

---

## Entry Results

### T-1

| Metric | Value |
|----------|----------:|
| Baseline WAPE | 4.508% |
| Cancellation WAPE | 4.406% |
| Improvement | +0.102 points |
| Win Rate | 50.0% |

### T-2

| Metric | Value |
|----------|----------:|
| Baseline WAPE | 5.742% |
| Cancellation WAPE | 5.625% |
| Improvement | +0.117 points |
| Win Rate | 83.3% |

### Longer Horizons

Selected horizons also improved:

| Horizon | Baseline | Cancellation |
|----------|----------:|----------:|
| T-28 | 11.46% | 11.02% |
| T-42 | 12.09% | 11.62% |
| T-56 | 12.20% | 11.51% |

---

## Exit Results

Cancellation behaviour produced mixed but occasionally useful improvements.

Examples included:

| Horizon | Baseline | Cancellation |
|----------|----------:|----------:|
| T-14 | 7.78% | 7.72% |
| T-42 | 15.43% | 15.34% |

Performance improvements were generally smaller than Booking Pace but remained measurable.

---

## Interpretation

Generation 2 asks:

```text
How many bookings are visible?
```

Cancellation behaviour asks:

```text
How reliable are those visible bookings?
```

This was a real forecasting signal but not a dominant one.

---

## Decision

✅ Influenced Production Forecast

Cancellation behaviour demonstrated useful forecasting information but did not justify becoming a major standalone forecast component.

---

# 23. Calendar Effects

## Hypothesis

Demand was expected to be affected by:

- Public Holidays
- School Holidays
- Easter
- Christmas/New Year
- Major Events

including:

```text
Edinburgh Festival
Royal Highland Show
Summer Sessions
```

---

## Method

Calendar categories were manually maintained and supplied to the simulation.

Historical behaviour within each category was used to adjust forecasts.

---

## Entry Results

| Horizon | Baseline | Calendar |
|----------|----------:|----------:|
| T-0 | 1.176% | 1.176% |
| T-1 | 4.508% | 4.508% |
| T-2 | 5.742% | 5.849% |

---

## Exit Results

| Horizon | Baseline | Calendar |
|----------|----------:|----------:|
| T-14 | 7.78% | 7.96% |
| T-21 | 9.13% | 9.17% |
| T-28 | 10.74% | 9.79% |

---

## Interpretation

Calendar effects did not provide consistent improvement.

This does not imply that holidays are unimportant.

Instead, Generation 2's:

```text
Weekday Structure
Month Seasonality
Trend Components
```

already captured much of the available calendar information.

---

## Decision

❌ Not Adopted

Calendar adjustments failed to demonstrate consistent incremental forecasting value.

---

# 24. Booking Regimes

## Hypothesis

Forecasts may behave differently during:

```text
Low Demand Environments
```

and:

```text
High Demand Environments.
```

Generation 2.1 investigated whether demand should be segmented into separate behavioural regimes.

---

## Method

Demand was categorised into five booking-position bands:

```text
Q1
Q2
Q3
Q4
Q5
```

using rolling training-period quantiles.

---

## Results

Performance was mixed.

### Entry Examples

| Horizon | Baseline | Regime |
|----------|----------:|----------:|
| T-1 | 4.508% | 4.553% |
| T-28 | 11.46% | 12.68% |
| T-56 | 12.20% | 11.58% |

### Exit Examples

| Horizon | Baseline | Regime |
|----------|----------:|----------:|
| T-42 | 15.43% | 15.18% |
| T-49 | 15.32% | 15.25% |
| T-56 | 14.65% | 15.73% |

---

## Interpretation

Demand environments clearly exist.

However, they could not be translated into consistently improved forecasts.

---

## Decision

❌ Not Adopted

Results lacked stability and repeatability.

---

# 25. Weekday-Specific Trend

## Hypothesis

Generation 2 already used trend.

Generation 2.1 investigated whether:

```text
Recent Fridays
versus
Historical Fridays
```

outperformed:

```text
General Trend Behaviour.
```

---

## Entry Results

| Horizon | Baseline | Weekday Trend |
|----------|----------:|----------:|
| T-0 | 1.176% | 1.176% |
| T-1 | 4.508% | 4.508% |
| T-2 | 5.742% | 5.742% |

---

## Interpretation

The feature produced almost no measurable uplift.

This does not indicate that weekday behaviour is unimportant.

Weekday behaviour had already been proven repeatedly throughout:

```text
Historical Analysis
Generation 1
Generation 2
```

Instead, Generation 2.1 demonstrated that the existing framework had already captured most of the available weekday information.

---

## Decision

❌ Not Adopted

No consistent forecasting benefit was observed.

---

# 26. Historical Window Selection

## Hypothesis

Generation 2 typically relied on:

```text
Four Same Weekdays
```

for historical forecasting.

Generation 2.1 investigated whether this assumption was optimal.

---

## Method

Five historical windows were tested:

```text
1 Week
2 Weeks
4 Weeks
6 Weeks
8 Weeks
```

The optimal window was selected independently within each rolling fold.

---

## Evidence

Example:

### Entry T-0

| Window | Validation WAPE |
|----------|----------:|
| 1 Week | 28.88% |
| 2 Weeks | 28.50% |
| 4 Weeks | 21.24% |
| 6 Weeks | 19.57% |
| 8 Weeks | 20.12% |

---

## Interpretation

The previously assumed:

```text
4 Week History
```

was frequently not optimal.

The optimal history length varied by:

```text
Horizon
Demand Type
Forecast Context
```

---

## Decision

✅ Adopted

Historical Window Optimisation became a direct input into:

```text
forecast_parameters.csv
```

and later Production Forecast configuration.

---

# 27. Overall Results

## Feature Ranking

| Feature | Evidence Strength | Decision |
|----------|----------|----------|
| Historical Window Selection | Strong | Adopted |
| Booking Pace | Strong | Influenced Production |
| Cancellation Behaviour | Moderate | Influenced Production |
| Calendar Effects | Weak | Rejected |
| Booking Regimes | Weak | Rejected |
| Weekday Trend | Weak | Rejected |

---

# 28. Major Discovery

Generation 2.1 demonstrated:

```text
Generation 2 Was Already Highly Effective.
```

Most behavioural enhancements failed to consistently outperform the Generation 2 baseline.

The strongest evidence was found for:

```text
Historical Window Optimisation

Booking Pace

Cancellation Behaviour
```

The most important operational output of Generation 2.1 was therefore not a new forecasting model.

It was a deeper understanding of:

```text
Forecast Parameters
Forecast Context
Forecast Stability
```

which directly informed Production Forecast design.

---

# 29. Generation 2.1 Conclusions

Generation 2 established:

```text
Forecast Weights
```

Generation 2.1 established:

```text
Forecast Parameters
```

The key conclusion was:

```text
Historical Analysis
↓
Generation 1
↓
Generation 2
↓
Generation 2.1
```

had now identified:

- the forecasting signals,
- the forecasting models,
- the signal weights,
- and the optimal operating parameters.

The final remaining challenge was to combine these findings into a single repeatable operational forecasting framework.

This directly motivated the development of the:

```text
Production Forecast
```