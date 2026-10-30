# FastPark Forecasting Framework: A Detailed Development Journey

## 1. Executive Summary

This document comprehensively details the development of the FastPark forecasting framework. Beginning with an initial passenger-penetration approach, the framework evolved through rigorous historical analysis and iterative model generations. This journey culminated in a sophisticated, horizon-specific 
forecasting system that dynamically weights multiple signals to accurately predict FastPark demand.

The development process, driven by empirical evidence, progressed through distinct stages:

*   **Original Passenger Forecast:** The baseline approach.
*   **Historical Analysis:** Deep dive into demand drivers.
*   **Generation 1 (G1):** Extensive testing of candidate forecasting models.
*   **Generation 2 (G2):** Development of ensemble learning and signal weighting.
*   **Generation 2.1 (G2.1):** Behavioral feature engineering and non-price optimization.
*   **Production Forecast:** The final, implemented framework.

The primary objective extended beyond merely building a forecast; it aimed to fundamentally understand the true drivers of FastPark demand to inform the most effective forecasting methodology. The guiding principle throughout was "Evidence before Implementation," meaning no forecasting component was adopted unless historical analysis and subsequent simulations demonstrated measurable predictive value.

---

## 2. The Initial Challenge: Limitations of the Original Forecast

### 2.1 Business Problem

Prior to redevelopment, FastPark forecasts were predominantly based on passenger demand. While current bookings for the next two weeks were considered, a significant disconnect often existed between short-term and longer-term predictions.

The original forecasting approach operated on a simple assumption:
*   **Entry Forecasting:** `Future Departing Passengers` × `Historical Entry Penetration`
*   **Exit Forecasting:** `Future Arriving Passengers` × `Historical Exit Penetration`

Despite its simplicity and reliance on readily available airport forecasts, this approach had notable shortcomings:
*   **Ignored Available Data:** The original forecast largely overlooked crucial, pre-demand information such as known durations, entry dates, and exit dates.
*   **Limited Booking Utilization:** Future booking information, though possessed by the business, was primarily used for the initial two weeks and then largely disregarded. There was limited evidence supporting the chosen "uplift numbers" or their adjustment for longer terms.
*   **Missed Operational Behavior:** Critical observable behaviors like `Expected ReturnDate`, `Historical Duration Behaviour`, and `Historical Return Behaviour` were not utilized.
*   **Absence of Explicit Visibility Measurement:** The forecast did not quantify "how much future demand is already known," a question that became a central focus of the project.

### 2.2 Core Research Questions

The project was initiated to address these limitations, starting with fundamental questions:
*   Are passengers actually the strongest predictor of future FastPark demand?
*   Does the operational booking system already contain stronger forecasting signals?

To systematically answer these, six major research objectives were defined:
1.  Determine how much future demand is visible in bookings.
2.  Measure cancellation and no-show behavior.
3.  Determine whether duration explains future exits.
4.  Determine whether `Expected ReturnDate` explains future exits.
5.  Measure the relationship between passenger demand and FastPark demand.
6.  Determine whether multiple forecasting signals can outperform single-model forecasts.

---

## 3. Phase 1: Historical Analysis - Uncovering Key Signals

### 3.1 Purpose

The Historical Analysis phase aimed to understand FastPark's actual behavior before developing any forecasting models. This phase intentionally focused on "Understanding" rather than "Prediction." The goal was to identify observable variables that could explain future entries and exits, were available before demand occurred, and held potential as forecasting signals.

### 3.2 Data Sources

The analysis utilized comprehensive datasets:
*   **Booking Data:** Sourced from `AirportX.v_Bookings`.
    *   Total Bookings: **393,381**
    *   Valid Bookings (B): **366,584 (93.19%)**
    *   Cancelled Bookings (CX): **26,487 (6.73%)**
    *   Status F Bookings: **310 (0.08%)**
    This demonstrated a highly clean booking population with a large volume of potentially usable forecasting information.
*   **Operational Dataset:** Sourced from `FastPark.v_EntryAndExits`, comprising **685,225** operational records. This dataset provided actual entries, actual exits, `Expected ReturnDate`, and return flight information, acting as the source of operational truth.
*   **Daily Demand Dataset:** Generated over **896** observation days, containing entries, exits, movements, and net flow, which later became the primary forecasting target.
*   **Hourly Demand Dataset:** Generated from **20,319** hourly observations, containing hourly entries, exits, and movements, forming the foundation for hourly forecasting.

### 3.3 Key Findings & Decisions

**Finding 1: Booking Visibility**
*   **Research Question:** How much future FastPark demand is visible before it occurs, and can bookings provide a measurable forward-looking signal?
*   **Method:** Entry and exit booking visibility curves were constructed by comparing `Booking Creation Timestamp` with `Planned Entry/Exit Date` against `Actual Operational Entries/Exits`. Visibility was measured at **12 horizons** (T-56 to T-0), generating **10,368** booking visibility observations.
*   **Evidence:**
    *   At **T-56**, approximately **23.11%** of future entries and **28.65%** of future exits were already visible.
    *   Visibility increased consistently, reaching **92.37%** for entries and **103.53%** for exits by **T-1**.
    *   Exit demand consistently exhibited higher visibility than entry demand. For instance, at T-28, entry visibility was **42.6%** while exit visibility was **53.3%**.
*   **Interpretation:** This demonstrated that "substantial future demand exists within the booking system well before operational demand occurs." Demand is progressively revealed through bookings, directly challenging the original forecasting philosophy.
*   **Decision:** Booking visibility was identified as a primary forecasting signal for Generation 1.

**Finding 2: Cancellation Behavior**
*   **Research Question:** Do cancellations materially affect future demand, and are there identifiable patterns?
*   **Method:** Analysis of all cancelled bookings, investigating frequency, timing, and lead times.
*   **Evidence:** Of **393,381** total bookings, **26,487 (6.73%)** were cancelled, meaning approximately 1 in every 15 bookings was ultimately cancelled. Most cancellations were known well before arrival, with an Average Days Before Entry of **40.42 days** and an Average Time Between Booking Creation And Cancellation of **33.22 days**. Customers booking further in advance were more likely to cancel, and the highest cancellation rates were observed for very short-duration stays (**9.27%** for 0-1 days).
*   **Interpretation:** Cancellation behavior is not random; its probability varies significantly by lead time, duration, and customer behavior. A booking population should not be automatically interpreted as future demand, as a measurable proportion disappears before arrival.
*   **Decision:** Cancellation behavior was identified as a future forecasting experiment.

**Finding 3: No-Show Behavior**
*   **Research Question:** How many valid bookings fail to become operational demand?
*   **Method:** No Show was defined as `Valid Booking + No Operational Check-In`. Analysis focused on overall no-show behavior, and by lead time and duration.
*   **Evidence:** Out of **366,584** valid historical bookings, **11,428** were no-shows, resulting in an Overall No Show Rate of **3.12%**. This means approximately 1 in every 32 valid bookings never arrived. Short-duration bookings exhibited significantly higher no-show rates (**38.86%** for 0-1 days) compared to longer durations (**3.02%** for 4-7 days).
*   **Interpretation:** Booking visibility reliability is not constant across the booking population and varies significantly by duration. Not all bookings carry equal forecasting value.
*   **Decision:** Future forecasting should investigate duration-specific visibility, completion factors, and forecasting approaches.

**Finding 4: Duration Behavior**
*   **Research Question:** Can stay duration explain future exits, and is it predictable/observable before exit?
*   **Method:** `Planned Duration` was compared against `Actual Duration` for **355,121** completed stays.
*   **Evidence:** The planned duration and actual duration were almost identical. The Average Difference was **-0.028 Days** (approximately **-40 Minutes**), and the Median Difference was **-0.017 Days** (approximately **-25 Minutes**).
*   **Interpretation:** Duration is both Observable Before The Event and Predictive Of The Event, making it one of the strongest forecasting candidates identified during Historical Analysis.
*   **Decision:** Duration was identified as a primary forecasting signal for exits and slated for direct testing in Generation 1.

**Finding 5: Expected Return Behavior**
*   **Research Question:** Can `Expected ReturnDate` explain future exits?
*   **Method:** `Expected ReturnDate` was compared against `Actual Exit Timestamp` for **355,123** return records, measuring deviation.
*   **Evidence:** **50%** of customers returned within **44 minutes** of their expected return time, and **75%** returned within **70 minutes**.
*   **Interpretation:** While `Expected ReturnDate` is not perfectly accurate, it demonstrates a much stronger relationship with reality than a purely seasonal estimate.
*   **Decision:** Expected return behavior was identified as a dedicated forecasting candidate.

**Finding 6: Passenger Relationships**
*   **Research Question:** Do passengers explain FastPark demand?
*   **Method:** Analysis of correlations between entries/exits and passenger demand, considering passenger mix, bands, and penetration.
*   **Evidence:** The Strongest Entry Driver was `International Departing Share` with a correlation of **0.633**. The Strongest Exit Driver was `International Arriving Passengers` with a correlation of **0.644**. However, penetration was not stable and varied materially across passenger-volume bands (e.g., `Lowest Demand Band` had **2.39%** entry penetration, while `Highest Demand Band` had **1.68%**; similarly for exits, **2.23%** vs. **1.74%**). Passenger-band forecasts showed Mean Absolute Error (MAE) values of **70.85** for entries and **67.04** for exits.
*   **Interpretation:** Passenger demand clearly influences FastPark demand and likely contains forecasting value. However, Historical Analysis alone could not determine whether passengers outperform booking visibility.
*   **Decision:** Passenger-based forecasting was slated for direct testing in Generation 1.

### 3.4 Historical Analysis Summary

**Strong Candidate Signals:**
*   Booking Visibility
*   Duration (Exits)
*   Expected Return Behavior (Exits)
*   Passenger Demand

**Candidate Future Enhancements:**
*   Same-Weekday Behaviour
*   Seasonality
*   Cancellation Behaviour
*   No Show Behaviour
*   Duration Segmentation
*   Calendar Effects
*   Booking Regimes
*   Hourly Structures

**Outcome:** Historical Analysis successfully identified the information that appears to drive FastPark demand. The next critical question was: "Which forecasting models can exploit these signals most effectively?" This directly led to the development of Generation 1.

---

## 4. Phase 2: Generation 1 - Testing Forecasting Models

### 4.1 Objective

Generation 1 was designed to empirically test the candidate forecasting signals identified during Historical Analysis and determine "Which forecasting methodologies actually work?"

### 4.2 Design Principles: No Look-Ahead Bias

A core principle of Generation 1 was its design as a genuine forecasting exercise to avoid look-ahead bias. Models were only allowed to use information known at the specific forecast point (e.g., T-28). This included bookings, cancellations, historical actuals, and historical booking curve behavior known by T-28. Crucially, future bookings, cancellations, demand, or passengers were explicitly excluded. This ensured that every forecast represented a legitimate historical forecasting scenario.

### 4.3 Test Scope

Generation 1 involved extensive testing across:
*   **Target Dates:** 182 dates.
*   **Forecast Horizons:** 15 horizons (from T-0 to T-56, including T-1, T-2, T-3, T-4, T-5, T-6, T-7, T-10, T-14, T-21, T-28, T-35, T-42, T-49, T-56).
*   **Models Tested:** A total of **15 entry models (E1-E15)** and **17 exit models (X1-X17)** were tested, encompassing families such as Historical Tendency, Weighted Historical, Passenger Models, Booking Visibility Models, Hybrid Models, Seasonal Models, Duration Models, Return Behavior Models, and Cohort Models.

### 4.4 Key Findings

**Entry Forecasting Results:**

*   **Short Horizons (T-0 to T-6):** Booking Visibility (Model E11) consistently dominated.
    *   **T-0 Winner (E11 Booking Visibility Curve):** WAPE of **1.11%**, MAE of **5.04**, RMSE of **6.74**, Bias of **+0.67**. This was one of the strongest forecasting results observed anywhere in the project.
    *   E11 remained the winner up to T-6, with WAPE gradually increasing (e.g., T-1: **3.36%**, T-6: **6.96%**).
*   **Medium Horizons (T-7 to T-21):** Booking visibility alone became sub-optimal. The winning models combined Booking Visibility with Historical Behavior, specifically `Same Weekday` (Model E12).
    *   **T-7 Winner (E12 Booking Curve + Same Weekday):** WAPE of **7.68%**.
    *   E12 continued to win at T-14 (**8.96%** WAPE) and T-21 (**10.65%** WAPE).
*   **Long Horizons (T-28 to T-56):** As visibility further weakened, seasonality became the dominant factor.
    *   **T-28 Winner (E15 Weekday-Month + Booking Curve):** WAPE of **12.11%**.
    *   From T-35 to T-56, E14 (Weekday-Month Historical) was the winner, consistently achieving a WAPE of **13.04%**.
*   **Overall Entry Ranking (across all horizons):**
    | Rank | Model | Family | WAPE |
    |---|---|---|---:|
    | 1 | E15 Weekday-Month + Booking Curve | Seasonal Hybrid | **10.23%** |
    | 2 | E12 Booking Curve + Same Weekday | Booking Hybrid | **10.60%** |
    | 3 | E13 Booking + Passenger Hybrid | Hybrid | **10.80%** |
*   **Interpretation:** The best overall models shared a common characteristic: Multiple Signals performed better than Single Signals.
*   **Passenger Model Results:** Passenger forecasting models (E9, E10) contained useful information, but their performance (E10 WAPE: **16.94%**, E9 WAPE: **17.14%**) was substantially weaker than Booking Visibility at most forecasting horizons.
*   **Incremental Benefit Analysis:** Historical behavior became increasingly valuable as booking visibility weakened. For example, at T-14, adding `Weekday` to a pure booking curve model improved WAPE from **12.91%** to **8.96%**, a **3.95 percentage point** improvement. This improvement became even more pronounced at T-56, where the gain was **13.19 percentage points**.

**Exit Forecasting Results:**

*   **Short Horizons (T-0 to T-2):** Exit Booking Visibility (Model X11) was the dominant forecasting signal.
    *   **T-0 Winner (X11 Exit Booking Visibility Curve):** WAPE of **2.87%**, MAE of **13.08**, RMSE of **16.31**, Bias of **+0.33**. This validated the Historical Analysis finding that "Future exit demand is highly visible before the exit occurs."
    *   X11 remained dominant up to T-2, with WAPE values of **2.96%** (T-1) and **3.15%** (T-2).
*   **Medium Horizons (T-3 to T-14):** Once horizons extended beyond T-2, the dominant signal shifted to a combination of Exit Visibility + Duration (Model X12).
    *   **T-3 Winner (X12 Exit Curve + Duration):** WAPE of **3.22%**, MAE of **14.66**, RMSE of **20.15**, Bias of **-1.18**.
    *   X12 remained the winner up to T-14 (WAPE: **7.64%**).
*   **Long Horizons (T-21 to T-56):** Hybrid models like X15 (Exit Booking + Passenger Hybrid) and X17 (Weekday-Month Historical) performed best as the forecast horizon extended.
    *   **T-21 Winner (X15 Exit Booking + Passenger Hybrid):** WAPE of **9.96%**.
    *   **T-49 and T-56 Winner (X17 Weekday-Month Historical):** WAPE of **14.85%**.
*   **Duration Contribution:** The repeated appearance of X12 among winning models demonstrated that duration is not simply a descriptive operational metric, but directly improves forecasting accuracy. This was a strong validation of the Historical Analysis.
*   **Passenger Contribution:** Passenger demand contained useful information but substantially less forecasting information than Booking Visibility and Duration. It was concluded that passenger demand should remain a supporting signal rather than a primary production forecasting signal.
*   **Return Behavior Contribution:** While `Expected Return Behaviour` contains forecasting information, the models (X13, X14) did not consistently outperform Booking Visibility + Duration, and thus it remained a candidate signal rather than a dominant framework.

### 4.5 Generation 1 Overall Conclusions

Generation 1 successfully provided empirical answers to "Which forecasting methods work?"

*   **Entry Forecasting:**
    *   **Short Horizons:** Booking Visibility.
    *   **Medium Horizons:** Booking Visibility + Historical Behaviour.
    *   **Long Horizons:** Seasonality.
*   **Exit Forecasting:**
    *   **Short Horizons:** Exit Visibility + Duration.
    *   **Long Horizons:** Seasonality + Hybrid Forecasts.

**What Generation 1 Could Not Answer:** Generation 1 identified the best individual models but raised a new question: "Instead of selecting a single winner, can we build a weighted combination of the strongest signals?" Different models captured different useful information, indicating that No Single Model was optimal across every forecasting horizon.

### 4.6 Generation 1 Final Decision

The key insight was that instead of choosing a single best model, the logical next step was to Combine The Strongest Signals in a structured and evidence-based manner. This directly motivated the development of Generation 2.

---

## 5. Phase 3: Generation 2 - Ensemble Learning & Optimization

### 5.1 Motivation

Generation 1 confirmed that different horizons required different forecast logic. The forecasting problem was no longer "Which model wins?" but "What combination of signals wins?" Generation 2 was designed to address this by attempting to learn the optimal combination of forecasting signals at each forecast horizon.

### 5.2 Research Question

The central Generation 2 question was: "What is the optimal combination of forecasting signals at each forecast horizon?" Rather than selecting a single model, G2 aimed to learn the optimal weights for key signals (Booking Weight, Weekday Weight, Seasonality Weight, Trend Weight) independently for each horizon.

### 5.3 Methodological Improvements

Generation 2 introduced four major changes to enhance the robustness and realism of the simulation:
1.  **All-Date Testing:** Instead of testing only specific days of the month (G1 tested Days 1-7, 14-20 of each month), G2 tested Every Calendar Day within the simulation period. This significantly increased the sample size and reduced date-selection bias.
2.  **Rolling-Origin Validation:** G2 introduced a rigorous validation methodology using rolling monthly folds for Training, Validation, and Test sets (e.g., Training Jul-Dec 2025, Validation Jan 2026, Test Feb 2026). This ensured Future periods could not select their own weights and produced a much more realistic estimate of production performance.
3.  **Signal Reduction:** G1 tested 32 forecasting models. G2 reduced the problem to its strongest independent forecasting signals.
    *   **Entry Components:** Booking Visibility, Same Weekday, Weekday-Month, Trend-Adjusted Month.
    *   **Exit Components:** Booking Visibility, Duration, Same Weekday, Trend-Adjusted Month.
    Only signals that repeatedly demonstrated forecasting value in G1 were retained.
4.  **Booking Shrinkage:** G2 introduced booking shrinkage to reduce instability at long horizons. When booking visibility is weak, the forecast partially reverts toward Historical Behaviour rather than fully trusting sparse booking information.

### 5.4 Key Findings

**Entry Forecast Results:**
Generation 2 determined recommended weights for each signal at various horizons:
*   **T-0 Entry Forecast:** Booking signal received **100.0%** weight, with Weekday, Month, and Trend at **0.0%**. Validation WAPE was **0.88%**. This re-confirmed G1's finding that "T-0 Forecasting = Booking Visibility Problem" and "No historical component materially improved the forecast."
*   **T-14 Entry Forecast:** Booking remained dominant at **77.5%**, but Month-Specific Seasonality began contributing significantly at **22.5%**. Validation WAPE was **9.56%**.
*   **T-21 Entry Forecast:** Booking weight decreased to **62.5%**, while Month contribution rose to **37.5%**. Validation WAPE was **11.27%**. Interpretation: Seasonality was "almost as important as Booking Visibility."
*   **T-35 Entry Forecast:** Booking was **62.5%**, and Weekday contributed **37.5%**. Validation WAPE was **8.82%**. Interpretation: Historical weekday structure was now contributing significant forecasting value, confirming G1 findings that Weekday Behaviour "remains predictive beyond booking visibility."
*   **T-42 Entry Forecast:** Trend became the largest signal at **42.5%**, Booking was **37.5%**, and Weekday **20.0%**. Validation WAPE was **10.02%**. Interpretation: "Recent Demand Behaviour contains more forecasting information than Current Booking Position at this horizon."
*   **T-49 Entry Forecast:** Trend dominated at **60.0%**, with Booking at **22.5%** and Weekday at **17.5%**. Validation WAPE was **11.24%**.
*   **T-56 Entry Forecast:** Weekday became the largest signal at **50.0%**, Booking at **35.0%**, and Month/Trend at **7.5%** each. Validation WAPE was **9.95%**.

**Evolution of Entry Forecasting Philosophy:**
| Horizon | Booking | Historical Structure |
| :--- | :---: | :---: | 
| T-0 | 100% | 0% | 
| T-14 | 77.5% | 22.5% |
| T-21 | 62.5% | 37.5% |
| T-42 | 37.5% | 62.5% |
| T-49 | 22.5% | 77.5% |
| T-56 | 35.0% | 65.0% |

**Conclusion on Entry Forecasting:** G2 proved that Short Horizons = Visibility Problem and Long Horizons = Historical Behaviour Problem.

**Entry Weight Stability:** Average weights across all folds showed strong stability, indicating that the strongest conclusions were robust. For instance, at T-0, Booking had an average weight of **99.17%**. At T-14, Booking was **58.75%**, Month **21.67%**, and Weekday **19.58%**. At T-56, Month dominated at **69.58%**, with Booking at **16.25%** and Weekday at **12.92%**. This provided strong evidence that Booking Visibility, Seasonality, and Trend are genuinely useful forecasting signals rather than statistical noise.

**Exit Forecast Results:**
G2 quantified how important each signal was across the forecast horizon. The progression suggested by G1 (Visibility → Visibility + Duration → Visibility + Hybrid Signals → Seasonality) was tested through signal weighting.
*   **T-0 Exit Forecast:** Exit Booking was heavily weighted at **87.5%**, with Duration at **2.5%**, Weekday at **7.5%**, and Trend at **2.5%**. Validation WAPE was **2.66%**. Interpretation: Short-horizon exits remained primarily a "Booking Visibility Problem."
*   **T-3 Exit Forecast:** This was a major discovery. The optimizer completely discarded booking visibility (0.0% weight) and "overwhelmingly preferred Duration" (**90.0%** weight), with Weekday at **10.0%**. Validation WAPE was **3.51%**. This was powerful evidence that Duration Is Not Merely Descriptive, but a primary forecasting signal.
*   **T-7 Exit Forecast:** Duration remained dominant at **70.0%**, with Exit Booking at **25.0%** and Weekday at **5.0%**. Validation WAPE was **4.36%**. Interpretation: Even one week before demand, Duration remained the dominant forecasting signal, though Booking Visibility contributed.
*   **T-14 Exit Forecast:** Duration continued to be the primary signal at **77.5%**, with Weekday at **15.0%** and Exit Booking at **7.5%**. Validation WAPE was **7.15%**. This was a striking contrast with entry forecasting, where booking visibility remained dominant at similar horizons.
*   **T-21 Exit Forecast:** No single signal dominated, with Exit Booking at **40.0%**, Duration at **25.0%**, and Weekday at **35.0%**. Validation WAPE was **7.67%**. Interpretation: The optimizer began blending Visibility, Duration, and Historical Behaviour, creating a more balanced forecast.
*   **T-28 Exit Forecast:** Duration disappeared entirely (0.0% weight). Weekday became the dominant signal at **57.5%**, with Exit Booking at **37.5%** and Trend at **5.0%**. Validation WAPE was **6.32%**. Interpretation: This was a surprising result, demonstrating that "different horizons contain fundamentally different forecasting information."
*   **T-42 Exit Forecast:** Trend became the largest forecasting signal at **57.5%**, with Duration at **42.5%**. Validation WAPE was **6.97%**. Interpretation: The optimizer was effectively using "Recent Exit Behaviour rather than Current Exit Visibility to predict future demand."
*   **T-49 Exit Forecast:** Trend and Duration shared dominance at **50.0%** each. Validation WAPE was **7.13%**.
*   **T-56 Exit Forecast:** Duration re-emerged as the dominant signal at **82.5%**, with Trend at **12.5%** and Weekday at **5.0%**. Validation WAPE was **7.08%**. Interpretation: Even at the longest forecast horizon tested, Duration remained the dominant forecasting signal, a "strongest validation of the Historical Analysis duration findings."

**Exit Weight Evolution:** The evolution of exit weights told a fundamentally different story from entries.
| Horizon | Exit Booking | Duration | Weekday | Trend |
|:--- | :---: | :---: | :---: | :---: |
| T-0 | 87.5% | 2.5% | 7.5% | 2.5% |
| T-3 | 0.0% | 90.0% | 10.0% | 0.0% |
| T-28 | 37.5% | 0.0% | 57.5% | 5.0% |
| T-56 | 0.0% | 82.5% | 5.0% | 12.5% |

**Major Discovery:** Generation 2 demonstrated that "Entry Forecasting = Visibility Problem" whereas "Exit Forecasting = Visibility + Duration + Historical Behaviour Problem." This indicated that the two demand streams behave fundamentally differently.

**Exit Weight Stability:** Weight stability confirmed that the optimizer repeatedly reached similar conclusions across independent validation folds. For example, at T-0, Exit Booking had an average weight of **70.83%**, Duration **23.75%**, Weekday **4.58%**, and Trend **0.83%**. At T-14, Duration dominated with an average weight of **65.83%**. At T-56, Weekday took the lead with **40.42%**, followed by Duration at **26.25%**, Trend at **22.08%**, and Exit Booking at **11.25%**. This strongly indicated that Duration Is A Genuine Forecast Driver rather than a statistical artefact.

---

## 6. Phase 4: Generation 2.1 - Behavioral Feature Engineering and Non-Price Optimisation

### 6.1 Motivation

Generation 2 successfully identified the optimal weighting of forecasting signals across all forecast horizons. However, a further question remained: "Can forecasting accuracy be improved through behavioural and operational context?" While G2 used Booking Visibility, Weekday Behaviour, Seasonality, Trend, and Duration, it treated many operational scenarios identically. For instance, **100 bookings** were treated the same regardless of whether bookings were accelerating rapidly or barely growing, or if it was a holiday period versus a normal trading period, or a period of high cancellations versus a normal environment.

### 6.2 Research Question

Generation 2.1 investigated whether additional behavioural and operational features could improve the already optimised Generation 2 framework.

### 6.3 Features Tested

Six feature groups were evaluated independently against the final Generation 2 baseline:
*   **Booking Pace:** Measures booking growth velocity.
*   **Historical Window Selection:** Optimizes same-weekday lookback periods.
*   **Calendar Effects:** Models holidays and events.
*   **Booking Regimes:** Models demand environments.
*   **Weekday-Specific Trend:** Models weekday-level momentum.
*   **Cancellation Behaviour:** Models real-time cancellation activity.

### 6.4 Key Findings

**Booking Pace (Hypothesis: Booking Growth Rate contains additional forecasting information beyond Current Booking Position):**
*   **Method:** Booking pace was calculated using booking growth between forecast horizons, categorizing each forecast into Very Low, Low, Normal, High, and Very High pace environments.
*   **Entry Results:** Booking Pace showed improvements. At T-1, WAPE improved from **4.508%** (Baseline) to **4.322%** (Booking Pace), a **0.186 percentage point** improvement with a **66.7%** win rate. At T-2, WAPE improved from **5.742%** to **5.470%**, a **0.272 percentage point** improvement with an **83.3%** win rate. Improvements were also visible at longer horizons (e.g., T-42: **12.09%** to **10.73%**).
*   **Exit Results:** Performance gains were generally smaller but remained positive at selected horizons. Booking pace contributed useful information beyond Exit Visibility and Duration, particularly when visibility was still evolving.
*   **Interpretation:** Booking pace captures something fundamentally different from booking visibility. Generation 2 answers "Where demand currently is," while booking pace answers "Where demand is moving."
*   **Decision:** **Adopted.** Booking Pace consistently demonstrated forecasting value across multiple horizons and multiple demand streams. Unlike booking visibility, which measures current known demand, booking pace measure the rate at which demand is developing. The feature repeatedly improved forecast accuracy and was retained as a production contextual adjustment. 

**Cancellation Behaviour (Hypothesis: Unusual cancellation activity provides additional forecasting information):**
*   **Method:** Three cancellation windows (1 Day, 3 Day, 7 Day) were tested, classifying cancellation behavior relative to historical norms (Very Low, Low, Normal, High, Very High). Only cancellations known at the historical forecast cutoff were included.
*   **Entry Results:** At T-1, WAPE improved from **4.508%** (Baseline) to **4.406%** (Cancellation), a **0.102 percentage point** improvement with a **50.0%** win rate. At T-2, WAPE improved from **5.742%** to **5.625%**, a **0.117 percentage point** improvement with an **83.3%** win rate. Improvements were also seen at longer horizons (e.g., T-28: **11.46%** to **11.02%**).
*   **Exit Results:** Cancellation behavior produced mixed but occasionally useful improvements (e.g., T-14: **7.78%** to **7.72%**).
*   **Interpretation:** Cancellation behavior asks "How reliable are those visible bookings?" This was identified as a real forecasting signal, but not a dominant one.
*   **Decision:** **Adopted as a Contextual Adjustment.** Cancellation behaviour demonstrated useful forecasting infomration across a range of forecast horizons. The feature did not justify becoming a standalone forecast componenet. However, it is consistently provided information about the reliability of visible bookings and was therefore retained as a contextural adjustment within the production forecasting framework. 
**Calendar Effects (Hypothesis: Demand is affected by Public Holidays, School Holidays, Easter, Christmas/New Year, and Major Events):**
*   **Method:** Calendar categories were manually maintained and supplied to the simulation. Historical behavior within each category was used to adjust forecasts.
*   **Entry Results:** Calendar effects did not provide consistent improvement. For example, at T-0, Baseline and Calendar WAPE were both **1.176%**. At T-2, Calendar WAPE was **5.849%** while Baseline was **5.742%**.
*   **Exit Results:** Similar mixed results were observed. At T-14, WAPE slightly worsened from **7.78%** to **7.96%**. At T-28, it improved from **10.74%** to **9.79%**.
*   **Interpretation:** Calendar effects did not provide consistent improvement. Instead, Generation 2's existing `Weekday Structure`, `Month Seasonality`, and `Trend Components` already captured much of the available calendar information.
*   **Decision**: **Partially Adopted**. Calendar effects were not sufficiently strong or consistent to justify becoming a standalone forecasting component within the Generation 2
ensemble.However, the experiments demonstrated that certain calendar-driven demand
patterns remained visible at longer forecast horizons, particularly for
exit demand. Calendar information therefore informed production forecast
design indirectly through historical window selection, seasonal context,
and parameter calibration rather than through explicit calendar-based
forecast weighting.

**Booking Regimes (Hypothesis: Forecasts may behave differently during Low and High Demand Environments):**
*   **Method:** Demand was categorized into five booking-position bands (Q1 to Q5) using rolling training-period quantiles.
*   **Entry Examples:** Performance was mixed. At T-1, Regime WAPE was **4.553%** compared to Baseline **4.508%**. At T-56, Regime WAPE was **11.58%** compared to Baseline **12.20%**.
*   **Exit Examples:** Performance was also mixed. At T-42, Regime WAPE was **15.18%** compared to Baseline **15.43%**. At T-56, Regime WAPE was **15.73%** compared to Baseline **14.65%**.
*   **Interpretation:** Demand environments clearly exist. However, they could not be translated into consistently improved forecasts.
*   **Decision:** **Not Adopted.** Results lacked stability and repeatability.

**Weekday-Specific Trend (Hypothesis: Recent weekday behavior outperforms general trend behavior):**
*   **Method:** Generation 2.1 investigated whether `Recent Fridays` versus `Historical Fridays` outperformed `General Trend Behaviour`.
*   **Entry Results:** The feature produced almost no measurable uplift. At T-0, T-1, and T-2, Baseline WAPE and Weekday Trend WAPE were identical (**1.176%**, **4.508%**, **5.742%** respectively).
*   **Interpretation:** This does not indicate that weekday behavior is unimportant, as `Weekday behaviour` had already been proven repeatedly throughout Historical Analysis, Generation 1, and Generation 2. Instead, Generation 2.1 demonstrated that the existing framework had already captured most of the available weekday information.
*   **Decision:** **Not Adopted.** No consistent forecasting benefit was observed.

**Historical Window Selection (Hypothesis: The assumed fixed historical window (e.g., Four Same Weekdays) was not optimal):**
*   **Method:** Five historical windows were tested (1 Week, 2 Weeks, 4 Weeks, 6 Weeks, 8 Weeks). The optimal window was selected independently within each rolling fold.
*   **Evidence (Entry T-0):** The assumed 4 Week History (WAPE **21.24%**) was frequently not optimal compared to 6 Weeks (WAPE **19.57%**). The optimal history length varied by `Horizon`, `Demand Type`, and `Forecast Context`.
*   **Interpretation:** The previously assumed fixed historical window was frequently not optimal.
*   **Decision:** **Adopted.** Historical Window Optimization became a direct input into `forecast_parameters.csv` and later Production Forecast configuration.

### 6.5 Overall Results: Feature Ranking

| Feature | Evidence Strength | Decision |
| Historical Window Selection | Strong | Adopted |
| Booking Pace | Strong | Adopted |
| Cancellation Behaviour | Moderate | Adopted as Contextual Adjustment |
| Calendar Effects | Mixed | Indirectly Incorporated |
| Booking Regimes | Weak | Rejected |
| Weekday Trend | Weak | Rejected |


Behavioural Feature Hierarchy

Strong Evidence
---------------
• Historical Window Selection
• Booking Pace

Moderate Evidence
-----------------
• Cancellation Behaviour
• Calendar Effects

Weak Evidence
-------------
• Booking Regimes
• Weekday-Specific Trend

Generation 2.1 demonstrated that the strongest
behavioural improvements came from features that
improved the interpretation of existing forecasts
rather than replacing them.

The evidence suggested that booking pace and
cancellation context added value by explaining how
the visible booking position was evolving and how
reliable that visible booking position was likely
to be.

### 6.6 Results

Generation 2.1 demonstrated that Generation 2 Was Already Highly Effective. Most behavioral enhancements failed to consistently outperform the Generation 2 baseline. The strongest evidence was found for:
*   Historical Window Optimisation
*   Booking Pace
*   Cancellation Behaviour

The most important operational output of Generation 2.1 was therefore not a new forecasting model, but a deeper understanding of:
*   Forecast Parameters
*   Forecast Context
*   Forecast Stability

This deeper understanding directly informed Production Forecast design.

### 6.7 Generation 2.1 Conclusions

Generation 2 established `Forecast Weights`. Generation 2.1 established `Forecast Parameters`. The key conclusion was:

Historical Analysis > Generation 1 > Generation 2 > Generation 2.1

had now identified:
*   The forecasting signals,
*   The forecasting models,
*   The signal weights,
*   And the optimal operating parameters.

The final remaining challenge was to combine these findings into a single repeatable operational forecasting framework. This directly motivated the development of the Production Forecast.

---

## 7. What Entered Production

Generation 2 directly contributed several key elements to the production framework:
*   Rolling-Origin Validation
*   Ensemble Forecasting
*   Horizon-Specific Weighting
*   Booking Shrinkage
*   Weight Stability Monitoring
*   Signal Weight Learning

The direct output became `forecast_weights.csv`, which forms the basis of the Production Forecast.

### 7.1 What Generation 2 Could Not Answer

Generation 2 optimized `Signal Weighting` but treated several behavioral variables as static. It did not investigate:
*   Booking Pace
*   Cancellation Behaviour
*   Calendar Effects
*   Booking Regimes
*   Alternative Historical Windows
*   Weekday-Specific Trend Effects

These remained unresolved by Generation 2, leading to Generation 2.1.

---

## 8. Production Forecast: Selection and Rationale

The culmination of this extensive research and development journey is the selection of a robust, dynamic forecasting framework designed for optimal accuracy and adaptability across varying horizons.

### 8.1 Chosen Forecast Framework

The selected Production Forecast is an **ensemble model** that leverages **horizon-specific signal weighting** and incorporates **booking shrinkage**, dynamically adjusting its reliance on various signals based on the forecast horizon. This framework is informed by the insights gained from Generation 2 and enhanced by the behavioral feature engineering from Generation 2.1.

The core components of the Production Forecast are:

*   **Entry Forecast:** A blended model that transitions from pure Booking Visibility at short horizons (e.g., T-0, 100% Booking) to incorporating Historical Behaviour (Weekday, Month) at medium horizons, and finally to a dominance of Seasonality (Month, Weekday) and Trend at long horizons (e.g., T-56, 65% Historical Structure).
*   **Exit Forecast:** A more complex blended model that rapidly shifts dominance from Booking Visibility at very short horizons (e.g., T-0, 87.5% Exit Booking) to Duration at short-to-medium horizons (e.g., T-3, 90% Duration; T-14, 77.5% Duration). At longer horizons, it blends Weekday and Trend, with Duration re-emerging as a significant factor even at T-56 (82.5% Duration).
*   **Dynamic Signal Weights:** The framework utilizes a continuously updated `forecast_weights.csv` derived from rolling-origin validation, ensuring that the optimal blend of signals is applied for each specific horizon.
*   **Booking Shrinkage:** This mechanism is active, particularly for longer horizons where booking visibility is sparse, ensuring that the forecast appropriately reverts to more stable historical behavior rather than over-relying on potentially volatile early booking data.
*   **Optimized Historical Window Selection:** The system dynamically selects the optimal historical lookback window for each forecast, moving beyond fixed assumptions.
*   **Influenced by Booking Pace & Cancellation Behavior:** While not standalone components, the understanding gained from Booking Pace and Cancellation Behavior informs the model's design and interpretation, particularly in scenarios where demand is rapidly changing or cancellation risk is high.

### 8.2 Rationale for Selection

This comprehensive framework was chosen for the following reasons:

1.  **Empirical Validation:** Every component and its contribution has been rigorously tested and validated through extensive historical simulations (Generation 1 and Generation 2), demonstrating measurable predictive value. This adheres strictly to the "Evidence before Implementation" principle.
2.  **Horizon-Specificity:** The framework acknowledges and explicitly addresses the fundamental discovery that "Different Horizons Require Different Forecast Logic." A single model or static weighting scheme proved sub-optimal across the full forecast range. The dynamic weighting system ensures the most relevant signals are prioritized at each specific horizon.
3.  **Robustness and Stability:** The rolling-origin validation and weight stability analysis in Generation 2 demonstrated that the identified signal combinations and their weights are robust and consistently achieved across independent folds, reducing the risk of overfitting or statistical noise.
4.  **Adaptability:** The ensemble approach allows the forecast to adapt to changing market conditions by adjusting the influence of various signals. For example, as booking visibility decreases, the model intelligently shifts reliance towards more stable historical patterns and seasonality.
5.  **Addressing Demand Stream Differences:** The framework explicitly recognizes that "Entry Forecasting" and "Exit Forecasting" behave fundamentally differently. Separate, tailored approaches, particularly the strong emphasis on Duration for exits, are integrated to maximize accuracy for both streams.
6.  **Continuous Improvement Potential:** The framework is designed with an understanding of its current limitations (identified in Generation 2.1, such as the need for further investigation into Booking Regimes and Weekday-Specific Trends), providing clear pathways for future enhancements and continuous refinement.
7.  **Actionable Insights:** The granular understanding of signal contributions provides clear insights into the drivers of demand at different lead times, which can inform operational decisions and strategic planning beyond just providing a number.

This Production Forecast represents a significant advancement over the original passenger-penetration approach, offering a more accurate, reliable, and interpretable prediction of FastPark demand.