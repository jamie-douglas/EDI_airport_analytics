import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import argparse
import time
import pandas as pd
from pathlib import Path

#utils
from modules.utils.query import query
from modules.utils.dates import to_datetime, add_date_parts



def load_prm_data_recompile(start: str, end: str) -> pd.DataFrame:
    """ 
    Load PRM Data where Billing PRM = 1, for time period, but force a fresh plan
    via OPTION (RECOMPILE) to test parameter-sniffing/plan issues.

    Parameters
    ----------
    start : str  (inclusive, 'YYYY-MM-DD')
    end   : str  (exclusive, 'YYYY-MM-DD')

    Returns
    -------
    pandas.DataFrame with:
        ['Job ID', 'Passenger ID', 'Operation Date', 'A/D', 'Vehicle Type',
         'Adhoc Or Planned', 'Pickup Location', 'Destination Location', 'SSR Code',
         'Day', 'Year']
    """
    # Keep the date key as an 8-char text (the column is text/'Abc')
    start_op = start.replace("-", "")   # "2025-01-01" → "20250101"
    end_op   = end.replace("-", "")     # "2025-01-03" → "20250103"

    # Use a raw SQL string so we can append OPTION (RECOMPILE)
    from modules.utils.db import get_engine
    import pandas as pd
    from sqlalchemy import text

    sql = """
    SELECT
        RequestID               AS [Job ID],
        PassengerID             AS [Passenger ID],
        Operation_DateID_Local  AS [Operation Date],
        ArrDep                  AS [A/D],
        VehicleTypeName         AS [Vehicle Type],
        adhocOrPlanned          AS [Adhoc Or Planned],
        actualPickupLocation    AS [Pickup Location],
        actualDestinationLocation AS [Destination Location],
        currentSSRCode          AS [SSR Code]
    FROM PRM.CompletedServicesByJob
    WHERE BillingPRM = 1
      AND Operation_DateID_Local >= :start_op
      AND Operation_DateID_Local <  :end_op
    OPTION (RECOMPILE);
    """

    eng = get_engine()
    df = pd.read_sql(
        text(sql),
        con=eng,
        params={"start_op": start_op, "end_op": end_op},
    )

    # Keep your post-processing identical
    df = to_datetime(df, "Operation Date")
    df = add_date_parts(df, "Operation Date", day=True, year=True)
    return df


def time_it(label, fn, *args, **kwargs):
    t0 = time.perf_counter()
    out = fn(*args, **kwargs)
    print(f"{label} took {time.perf_counter() - t0:.2f}s  (rows={len(out):,})")
    return out

# EXACT same window you measured
df_rc = time_it(
    "PRM load with OPTION(RECOMPILE)",
    load_prm_data_recompile,
    "2025-01-01", "2026-01-01"
)

