#modules/config.py
import os
from dotenv import load_dotenv

#Load environment variables from .env if present
load_dotenv()

#------------------------------------------------------------------
# DATABASE CONFIGURATION
#------------------------------------------------------------------

DSN = os.getenv("DSN", "AzureConnection")
"""
str: Default ODBC DSN name, overridden by the DSN environment variable if set.
"""

USERNAME = os.getenv("USERNAME", "")
"""
str: Default username for DSN authentication, overridden by the USERNAME environment variable if set.
"""

# --------------------------
# IMMIGRATION SEASON CONFIG
# --------------------------

# Summer season start/end (local time)
IMM_SEASON_SUMMER_START = "2026-03-29 07:00"
IMM_SEASON_SUMMER_END   = "2026-10-25 01:00"

# Summer IA1 weekday schedule.
# Keys: 0=Mon, ..., 6=Sun
# Each value is a list of (start_hour, end_hour) tuples.
IA1_SUMMER_HOURS = {
    6: [(7, 16), (22, 24), (0, 1)],   # Sunday
    0: [(7, 16), (22, 24), (0, 1)],   # Monday
    1: [(7, 16), (22, 24), (0, 1)],   # Tuesday
    2: [(7, 16), (22, 24), (0, 1)],   # Wednesday
    3: [(7, 17), (22, 24), (0, 1)],   # Thursday
    4: [(7, 17), (22, 24), (0, 1)],   # Friday
    5: [(7, 17), (22, 24), (0, 1)],   # Saturday
}

# Baseline IA1 hours (used outside summer season)
IA1_BASELINE_OPEN_DAYS = {4, 5, 6, 0}   # Fri, Sat, Sun, Mon
IA1_BASELINE_DAY_HOURS = [(11, 15)]     # 11:00–15:00
IA1_BASELINE_NIGHT_HOURS = [(22, 24), (0, 1)]  # 22:00–01:00

# IA2 is always 24 hours
IA2_ALWAYS_OPEN = True


# --------------------------
# IMMIGRATION THROUGHPUT & CAPACITY
# --------------------------
IA1_TPH = 834
IA2_TPH = 1304
IA1_CAX = 569
IA2_CAX = 754


#--------------------------
# SEASON WINDOWS (used by Tactical Readiness)
# --------------------------
# Use date-only strings here; we don't need time-of-day for these windows.
SUMMER_START = "2026-03-29"
SUMMER_END   = "2026-10-24"

# --------------------------
# SECURITY CAPACITY (for charts)
# --------------------------
# Security rolling-hour capacity reference line used in plots.
SECURITY_CAX = 4240


