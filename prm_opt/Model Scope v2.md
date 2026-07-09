# Model Scope v2 (Current Implementation)

This document consolidates the full mathematical model scope for the current PRM optimization implementation.

Implementation alignment:
- Core model: [pyomo_model_v2.py](pyomo_model_v2.py)
- Parameters: [params.py](params.py)
- Configuration and business rules: [config.py](config.py)
- Canonical job build: [build_jobs.py](build_jobs.py)
- Scenario runners: [run_s25.py](run_s25.py), [run_s26.py](run_s26.py)

## Outputs Wanted

1. Fleet deployment by flight and time bucket.
2. Peak concurrent resource requirements:
- ambulifts
- minibuses
- drivers
- vehicle-agents
- pushers
3. Mode decisions per job:
- Amb
- Mini
- Push
4. SLA diagnostics:
- sla_percent
- allowed_breaches
- actual_breaches
- sla_floor_slack (if enabled)
5. Monthly and hourly diagnostics from wrappers.

## 1. Sets and Indices

### 1.1 Time

- $b \in B$: time buckets (fixed 15-minute discretization).

### 1.2 Demand entities

- $j \in J$: PRM jobs.
- $f \in F$: flights.
- $J_f \subseteq J$: jobs mapped to flight $f$.

### 1.3 Mode and fleet

- $m \in M = \{Amb, Mini, Push\}$: horizontal service modes.
- $(v,c) \in VC$: vehicle type and class pair.
- $(j,b) \in JB$: feasible job start pairs.
- $(f,b) \in FB$: feasible flight activity pairs.

### 1.4 Derived indicator sets

- $J_{lift}$: wheelchair jobs requiring lift-gate flow.
- $F_{vert}$: flights with at least one vertical-demand job.

## 2. Parameters (Inputs)

### 2.1 Job-level fields

For each job $j$:
- release bucket index: $rel_j$
- SLA anchor bucket index: $slaStart_j$
- SLA allowance (mins): $L_j$
- hard deadline index (departures where defined): $ddl_j$
- mode eligibility attributes (stand, sector, airline, vertical, wheelchair flags)

### 2.2 Fleet and capacity

For each vehicle class $(v,c)$:
- seat capacity: $seatcap_{v,c}$
- wheelchair capacity: $wccap_{v,c}$
- available count: $count_{v,c}$
- hourly capex proxy: $capex_{v,c}$

Aggregate:
- $N_{Amb}$ = total ambulift count
- $N_{Mini}$ = total minibus count

### 2.3 SLA policy parameters

- Target SLA rate: $\rho$ (from planning toggles).
- Allowed breaches:
$$
allow =
\begin{cases}
\lceil (1-\rho)|J| \rceil & \text{if hard SLA floor enabled} \\
\lfloor (1-\rho)|J| \rfloor & \text{legacy soft-floor mode}
\end{cases}
$$
- Big-M for SLA linkage: $M_{big}$.

### 2.4 Time-consumption coefficients

Precomputed from jobs and trip-time builders:
- trip spill vectors for ambulift/minibus minutes
- standby spill vectors for combined vertical and horizontal operations
- spin lock removal term by bucket: $spinRemoved_b$

### 2.5 Lift bottleneck parameters

- Lift cycle minutes per wheelchair movement: $g$.
- Available lift minutes per bucket: $Lcap_b$.

## 3. Decision Variables

### 3.1 Passenger assignment and SLA

- $x_{j,m} \in \{0,1\}$: mode chosen for job $j$.
- $A_{j,b} \in \{0,1\}$ for $(j,b) \in JB$: service start bucket.
- $y_j \in \{0,1\}$: SLA breach indicator.
- $U_{j,b,m} \in \{0,1\}$: linearization of $A_{j,b} \land x_{j,m}$.

### 3.2 Flight-bucket deployment and logic indicators

- $k_{v,c,f,b} \in \mathbb{Z}_{\ge 0}$: vehicles allocated by class.
- $V_{f,b} \in \{0,1\}$: vertical visit anchor.
- $Z^{Mini}_{f,b}, Z^{Push}_{f,b}, Z^{NotAmb}_{f,b} \in \{0,1\}$.
- $W^{CombAny}_{f,b}, W^{CombMini}_{f,b}, W^{CombPush}_{f,b} \in \{0,1\}$.

### 3.3 Staffing variables

- $H^{drv}_b, H^{vehag}_b, H^{push}_b \in \mathbb{Z}_{\ge 0}$.

### 3.4 SLA aggregate helper variables

- $slaExcess \ge 0$.
- $slaFloorSlack \ge 0$ when hard floor slack is enabled, else fixed to zero.

## 4. Objective Function

Minimize total operating and service-risk cost:

$$
\min Z = Z_{trip} + Z_{soft} + Z_{sla} + Z_{excess} + Z_{floor} + Z_{staff} + Z_{capex}
$$

where:

$$
Z_{trip} = w_{trip} \sum_{(v,c)\in VC}\sum_{(f,b)\in FB} k_{v,c,f,b}
$$

$$
Z_{soft} = \sum_{j\in J}\left(p_{amb}\,x_{j,Amb} + p_{push}\,x_{j,Push} + p_{trans}\,\mathbb{1}_{vert,j}\,x_{j,Mini}\right)
$$

$$
Z_{sla} = w_{sla}\sum_{j\in J} y_j
$$

$$
Z_{excess} = w_{excess}\,slaExcess
$$

$$
Z_{floor} = w_{floor}\,slaFloorSlack \quad (\text{active only when enabled})
$$

$$
Z_{staff} = \sum_{b\in B}\left(H^{drv}_b + H^{vehag}_b + H^{push}_b\right)
$$

$$
Z_{capex} = \sum_{(v,c)\in VC} capex_{v,c}\sum_{(f,b)\in FB} k_{v,c,f,b}
$$

## 5. Constraints

### 5.1 Mode choice and service start

1. One mode per job:
$$
\sum_{m\in M} x_{j,m} = 1 \quad \forall j\in J
$$

2. Serve once in feasible window:
$$
\sum_{b:(j,b)\in JB} A_{j,b} = 1 \quad \forall j\in J
$$

3. Linearization $U = A \land x$:
$$
U_{j,b,m} \le A_{j,b}
$$
$$
U_{j,b,m} \le x_{j,m}
$$
$$
U_{j,b,m} \ge A_{j,b} + x_{j,m} - 1
$$
for all $(j,b)\in JB, m\in M$.

### 5.2 SLA definition and policy

4. Job-level SLA breach linkage:
$$
\sum_{b:(j,b)\in JB} (idx(b)-slaStart_j)\Delta\,A_{j,b} \le L_j + M_{big}y_j
$$
where $\Delta=15$ minutes.

5. Excess-over-target definition:
$$
slaExcess \ge \sum_{j\in J} y_j - allow
$$

6. Optional hard SLA floor:
$$
\sum_{j\in J} y_j \le allow + slaFloorSlack
$$
(strict version sets $slaFloorSlack=0$).

### 5.3 Eligibility and policy rules

7. Safety stand vertical jobs (non-exempt airline) cannot use push.

8. Remote stand jobs cannot use push:
$$
x_{j,Push}=0 \quad \forall j \text{ on remote stand}
$$

9. Domestic arrivals cannot use ambulift horizontally:
$$
x_{j,Amb}=0 \quad \forall j: sector_j=Domestic, dir_j=Arrival
$$

### 5.4 Vertical visit anchor logic

10. Visit count by flight:
$$
\sum_{b:(f,b)\in FB}V_{f,b}=
\begin{cases}
0, & f\notin F_{vert} \\
MAX\_DOCKED\_AMB\_PER\_FLIGHT, & f\in F_{vert}
\end{cases}
$$

11. Visit requires ambulift presence:
$$
\sum_{c:(Amb,c)\in VC}k_{Amb,c,f,b} \ge V_{f,b} \quad \forall (f,b)\in FB
$$

12. Vertical service starts only after a visit anchor is available:
$$
A_{j,b} \le \sum_{b'\le b}V_{f(j),b'} \quad \forall j\text{ vertical}, (j,b)\in JB
$$

### 5.5 Flight-bucket seat and wheelchair capacity

13. Minibus seat capacity:
$$
\sum_{j\in J_f}U_{j,b,Mini} \le \sum_{c:(Mini,c)\in VC} seatcap_{Mini,c}\,k_{Mini,c,f,b}
$$

14. Minibus wheelchair capacity:
$$
\sum_{j\in J_f} needsWC_j\,U_{j,b,Mini} \le \sum_{c:(Mini,c)\in VC} wccap_{Mini,c}\,k_{Mini,c,f,b}
$$

15. Ambulift horizontal seat and wheelchair capacities apply to non-vertical horizontal ambulift assignments.

### 5.6 Fleet availability and reserves

16. Fleet exclusivity by class and bucket:
$$
\sum_{f:(f,b)\in FB}k_{v,c,f,b} \le count_{v,c} \quad \forall (v,c), b
$$

17. Total minibus cap with reserve:
$$
\sum_{c,f}k_{Mini,c,f,b} \le \sum_c count_{Mini,c} - reserve^{ferry}_b
$$

### 5.7 Time-capacity (trip spill and standby spill)

18. Ambulift time-capacity:
$$
UsedAmb_b \le 15\,N_{Amb} - spinRemoved_b
$$

19. Minibus time-capacity:
$$
UsedMini_b \le 15\,N_{Mini}
$$

Both used terms are convolution sums over prior buckets using precomputed spill vectors and standby indicators.

### 5.8 Staffing constraints

20. Driver staffing:
$$
H^{drv}_b \ge AmbUsed_b + MiniUsed_b + ferryDrvReserve_b
$$

21. Vehicle-agent staffing:
$$
H^{vehag}_b \ge AmbUsed_b + MiniUsed_b
$$

22. Pusher staffing:
$$
H^{push}_b \ge \sum_{j:(j,b)\in JB}U_{j,b,Push}
$$

### 5.9 Lift bottleneck

23. Lift throughput cap:
$$
\left(\sum_{j\in J_{lift}} A_{j,b}\right)\,g \le Lcap_b \quad \forall b\in B
$$

## 6. Scenario Definitions

### Scenario 1 (policy baseline)

- Mode decisions are fixed by policy logic in [policy_s1.py](policy_s1.py).
- No optimization solve.

### Scenario 2 (current optimization)

- Full MILP in [pyomo_model_v2.py](pyomo_model_v2.py).
- Optimizes mode choice, timing, deployment, and staffing under constraints.

### Scenario 3 (batching concept)

- Present in historical scope language.
- Not the active structure in current Scenario 2 implementation.

## 7. Core Assumptions (State Explicitly)

1. Time is discretized into 15-minute buckets.
2. Within a bucket, sequencing is not explicitly modeled.
3. Each job is served exactly once.
4. Feasible service windows are preconstructed through sparse sets JB and FB.
5. Delay is measured bucket-wise from SLA anchor.
6. The run is deterministic once inputs/toggles are fixed.
7. Uncertainty is represented by input assumptions, not explicit stochastic recourse variables.
8. Fleet is aggregate class-based deployment, not explicit routed vehicles.
9. Staffing is aggregate bucket-level coverage, not shift-roster optimization.
10. Lift capacity is represented as throughput cap, not queue simulation.
11. Spin lock is represented as removed ambulift minutes by bucket.
12. Hard SLA floor can be strict or slack-enabled via toggles.

## 8. Limitations (Current Model Boundary)

1. No explicit vehicle routing or travel-path sequencing.
2. No explicit staff shift legality, breaks, or handover rosters.
3. No explicit stochastic objective over multiple future scenarios in one solve.
4. No explicit queue-state simulation at gates/lifts beyond throughput cap.
5. Service-time uncertainty is embedded in precomputed assumptions, not endogenous.
6. Potential objective-weight sensitivity; outputs depend on calibration of penalty weights.
7. Operational realism for rare edge-cases depends on quality of upstream stand and timing inputs.

## 9. Data and Calibration Dependencies

1. Job construction quality in [build_jobs.py](build_jobs.py) (release, SLA anchors, flags).
2. Tau and spin preprocessing in [params.py](params.py).
3. Stand and remote/contact classification in [config.py](config.py).
4. S25/S26 ingest and assumptions quality in [ingest_s25.py](ingest_s25.py), [ingest_s26.py](ingest_s26.py), [build_s26_assumptions.py](build_s26_assumptions.py).

## 10. Solver and Practical Interpretation Notes

1. Time limits can return strong incumbent solutions without proven optimality.
2. Hard SLA floor can force infeasibility when policy targets exceed physical capacity.
3. Compare mode (p90 vs p100) should be interpreted as planning stress test, not probabilistic confidence interval.
4. Peak jobs day and true peak resource day can differ.

## 11. Recommended Reporting Pack

For each run/month, report:
1. Solve status, termination condition, and mip gap if relevant.
2. SLA diagnostics: sla_percent, allowed_breaches, actual_breaches, sla_floor_slack.
3. Peak resources: PeakAmb, PeakMini, PeakDrivers.
4. Comparison deltas for p90 vs p100 where enabled.
5. Peak-day hourly table used for operational staffing decisions.

---

If you need a shorter executive version, create a one-page summary from Sections 7, 8, and 11 only.
