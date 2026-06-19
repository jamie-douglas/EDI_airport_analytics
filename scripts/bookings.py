import sys
import pathlib
from pathlib import Path

# Add parent directory to path so custom modules can be imported
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from modules.utils.query import query
import pandas as pd


def load_fastpark_bookings(start: str, end: str, ) -> pd.DataFrame:

    df = query(
        table="AirportX.v_Bookings",
        columns=[
            "*"
        ],
        where=["assetName = 'FastPark'"],
        date_column="entryDate",
        start=start,
        end=end,
    )

    return df

bookings_df = load_fastpark_bookings("2026-06-02", "2026-06-16")
print(bookings_df.head(50))

# ==========================================================
# EXPORT (SAFE - PRESERVES EXISTING SHEETS)
# ==========================================================

output_path = Path(__file__).resolve().parents[1] / "outputs" / "bookings.xlsx"
output_path.parent.mkdir(parents=True, exist_ok=True)

writer_kwargs = {"engine": "openpyxl"}

if output_path.exists():
    writer_kwargs.update({
        "mode": "a",
        "if_sheet_exists": "replace"
    })
else:
    writer_kwargs.update({
        "mode": "w"
    })

with pd.ExcelWriter(output_path, **writer_kwargs) as writer:
    bookings_df.to_excel(writer, sheet_name="FastPark Bookings", index=False)

print(f"Saved Excel file to: {output_path}")