"""Central path definitions.

Every path that points outside the repo lives here, so a moved file only
needs fixing in one place. Set the DOCS_ROOT environment variable to point
at a different Documents folder (for example on another person's machine).
"""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

DOCS_ROOT = Path(
    os.environ.get(
        "DOCS_ROOT",
        Path.home() / "OneDrive - Edinburgh Airport Limited" / "Documents",
    )
)
PROJECTS = DOCS_ROOT / "01 Projects"
RESTRICTED = DOCS_ROOT / "02 Restricted Data"

# --- Project files (outside the repo) ---------------------------------------
PRM_DATA_XLSX = PROJECTS / "Assisted Travel (PRM)" / "PRM Reports" / "Data" / "PRM_Data.xlsx"
TRANSACTION_FORECAST = PROJECTS / "FastPark" / "Transaction Model" / "transaction_forecast.xlsx"
TRANSACTION_BREAKDOWN = PROJECTS / "FastPark" / "Transaction Model" / "transaction_breakdown_oct26.xlsx"
WASHUP_S26_DIR = PROJECTS / "FastPark" / "Forecasting and Accuracy" / "Summer 2026 Wash-up"

# --- Restricted (personal) data: never inside the repo ----------------------
FASTPARK_MOVEMENTS_DIR = RESTRICTED / "FastPark" / "Movements"
FASTPARK_ONSITE_DUE_IN_DIR = RESTRICTED / "FastPark" / "Onsite and Due In"
FASTPARK_ANPR_PHOTOBOOTH_DIR = RESTRICTED / "FastPark" / "ANPR Photobooth"
PRM_ECAC_DIR = RESTRICTED / "PRM Minibuses" / "ECAC"


def stage_dirs(script_file):
    """Return (inputs, outputs) folders next to the calling script.

    Usage:  INPUT_DIR, OUTPUT_DIR = stage_dirs(__file__)
    The outputs folder is created if it does not exist.
    """
    stage = Path(script_file).resolve().parent
    (stage / "outputs").mkdir(exist_ok=True)
    return stage / "inputs", stage / "outputs"
