# scripts/prm_opt/config.py

"""
Configuration aligned to Model Scope document

Contains:
- sets and constants needed to build parameters and constraints
- Forecast assumptions needed to generate S26 jobs:
    penetration rates/ SSR mix / own-chair prob/ stand probability / zones
"""


from dataclasses import dataclass, field
from typing import Dict, Set, Tuple, List



# =========================================================
# Planning toggles
# =========================================================

@dataclass(frozen=True)
class PlanningToggles:
    sla_buffer_mins: int = 0
    handover_mins: int = 10   # transfer coordination overhead
    
    # Preposition buffers (minutes)
    # These shift the release bucket earlier, enabling sensitivity tests:
    preposition_arrival_mins: int = 5
    preposition_departure_mins: int = 5

    
       
    # Spillover control: max number of buckets a single job’s minutes can span
    spill_bucket_cap: int = 8   # 8 * 15min = 2 hours

    
    vertical_cycle_mins: float = 0.0     
    vertical_wccap: int | None = None    # optional override
    max_docked_amb_per_flight: int = 1


    # Standby (handover) minutes — applied only for combined jobs (NOT if horizontal is Amb)
    standby_dep_vert_mins: float = 5.0   # departures: standby on vertical side
    standby_arr_horiz_mins: float = 5.0  # arrivals: standby on horizontal side

    max_late_mins: int = 180

    
    sla_target_rate: float = 0.98

    obj_trip_weight: float = 50.0
    obj_sla_weight: float = 200_000.0
    obj_sla_excess_weight: float = 5_000_000.0

    M_BIG: int | None = None


    spin_turnaround_threshold_mins: int = 60  

    # OPTIONAL placeholders for ferrying/break inefficiency later
    # {bucket_timestamp: 1} means reserve 1 unit in that bucket
    ferry_mini_reserved: dict = field(default_factory=dict)
    ferry_drv_reserved: dict = field(default_factory=dict)





# =========================================================
# Airline / stand logic
# =========================================================


NO_JETBRIDGE_AIRLINES: List[str] = [
    "2S","3V","6I","8H","AP","BE","BGH","BT","BY","C3","D0","D8","DS","DY",
    "E4","E9","EA","EC","ED","EJU","EVE","EW","FH","FHY","FI","FR","FX","GR",
    "HAT","HV","LM","NPT","OS","QS","RC","RK","SK","SRR","T3","TO","TP",
    "U2","V3","W6","WF","WK","XC","XQ","ZT","LO"
]


RYANAIR_CODES: Set[str] = {"FR", "RY"}

SAFETY_STANDS: Set[str] = {"15", "15A", "15B", "17"}

# Explicit vertical exceptions (even if contact gate)
VERTICAL_EXCEPTIONS: Set[Tuple[str, str]] = {
    ("UA", "10"),   # United stand 10
}


# =========================================================
# Gate 7 / 8 lift constraint
# =========================================================

LIFT_STANDS: Set[str] = {"7", "8"}

LIFT_CYCLE_MINS = 3.0          # minutes per WCH
LIFT_CAPACITY_MINS = 60.0      # per hour bucket


# =========================================================
# Probabilistic assumptions (S26)
# =========================================================

WCHS_OWN_CHAIR_PROB: float = 0.109




# =========================================================
# Fleet registry (THIS mirrors your real assets)
# =========================================================

# Each entry = one vehicle model that the solver can count
# type ∈ {"Amb", "Mini"}

VEHICLE_MODELS: Dict[str, Dict] = {

    # ---------------- Ambulifts (current fleet) ----------------
    "AMB_14111": {"type": "Amb", "seatcap": 6, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_14112": {"type": "Amb", "seatcap": 6, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_OMN135": {"type": "Amb", "seatcap": 3, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_OMN079": {"type": "Amb", "seatcap": 6, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_OMN080": {"type": "Amb", "seatcap": 6, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_14161": {"type": "Amb", "seatcap": 7, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_14162": {"type": "Amb", "seatcap": 7, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_14163": {"type": "Amb", "seatcap": 7, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_79803": {"type": "Amb", "seatcap": 7, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_79802": {"type": "Amb", "seatcap": 7, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_79801": {"type": "Amb", "seatcap": 7, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_79804": {"type": "Amb", "seatcap": 7, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_79805": {"type": "Amb", "seatcap": 7, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},
    "AMB_79900": {"type": "Amb", "seatcap": 7, "wccap": 1, "staff": 2, "capex_hr": 500000 / (5 * 365 * 24), "is_future": False},

    # ---------------- Minibuses (current fleet) ----------------
    "MB_WJ160DR": {"type": "Mini", "seatcap": 6, "wccap": 2, "staff": 2, "capex_hr": 80000 / (5 * 365 * 24), "is_future": False},
    "MB_WJ160DT": {"type": "Mini", "seatcap": 6, "wccap": 2, "staff": 2, "capex_hr": 80000 / (5 * 365 * 24), "is_future": False},
    "MB_WJ160FD": {"type": "Mini", "seatcap": 6, "wccap": 2, "staff": 2, "capex_hr": 80000 / (5 * 365 * 24), "is_future": False},

    # ---------------- Future purchase options ----------------
    "MB_EV_10": {"type": "Mini", "seatcap": 10, "wccap": 1, "staff": 2,
                 "capex_hr": 80000 / (5 * 365 * 24), "is_future": True},

    "MB_EV_18": {"type": "Mini", "seatcap": 18, "wccap": 1, "staff": 2,
                 "capex_hr": 80000 / (5 * 365 * 24), "is_future": True},
}



# =========================================================
# Soft preference penalties
# =========================================================
# These DO NOT force behaviour.
# They just bias decisions when capacity allows.

PENALTY = {
    "AMB_HORIZONTAL": 4.0,   # discourage using ambulift horizontally
    "TRANSFER": 1.0,         # discourage handover unless helpful
    "PUSH": 3.0,             # pusher is least preferred
}

# =========================================================
# Stand Zones for batching
# =========================================================


STAND_ZONES = {
    # Zone 1
    "1": "Z1", "1A": "Z1", "1B": "Z1", "2": "Z1", "3": "Z1",
    "4": "Z1", "4A": "Z1",
    "99": "Z1", "100": "Z1", "101": "Z1", "102": "Z1",
    "103": "Z1", "104": "Z1", "105": "Z1", "106": "Z1",

    # Zone 2
    "6": "Z2", "7": "Z2", "8": "Z2", "9": "Z2",
    "10": "Z2", "11": "Z2", "12": "Z2", "14": "Z2",

    # Zone 3
    "51": "Z3", "51A": "Z3", "51B": "Z3",
    "16": "Z3", "16A": "Z3", "16B": "Z3", "17": "Z3",
    "18": "Z3", "19": "Z3",
    "20": "Z3", "21": "Z3", "22": "Z3", "23": "Z3",
    "24": "Z3", "25": "Z3",

    # Zone 4
    "26": "Z4", "27": "Z4", "28": "Z4", "29": "Z4",
    "30": "Z4", "31": "Z4", "32": "Z4", "33": "Z4", "34": "Z4",

    # Zone 5
    "308": "Z5", "309": "Z5", "310": "Z5", "310R": "Z5",
    "311": "Z5", "311R": "Z5", "311L": "Z5",
    "312": "Z5", "312L": "Z5",
    "313": "Z5", "314": "Z5", "315": "Z5",
    "316": "Z5", "317": "Z5", "317A": "Z5",

    # Zone 6
    "210": "Z6", "211": "Z6", "212": "Z6",

    # Zone 7
    "200": "Z7", "201": "Z7", "202": "Z7", "203": "Z7",
    "204": "Z7", "205": "Z7", "206": "Z7", "207": "Z7", "208": "Z7",
}
