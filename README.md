# EDI Airport Analytics

Analytics code for Edinburgh Airport: FastPark forecasting and operations, PRM (assisted travel) reporting and fleet optimisation, and terminal operations readiness.

---

# Layout

```text
EDI_airport_analytics/
├── FastPark/            FastPark car park: short-term forecast, reports, capacity, movements
├── PRM/                 Assisted travel: reports, minibus analysis, fleet optimisation
├── Operations/          Tactical readiness, check-in, schedule checks
├── modules/             Shared helpers (database, queries, dates, Excel, charts)
├── prm_opt/             PRM fleet optimisation library (used by PRM/03)
├── Archive/             Superseded code, kept for reference only
├── pyproject.toml       Makes modules/ and prm_opt/ installable
└── requirements.txt     Python packages the code needs
```

Each project folder is split into numbered **stages**. Every stage has the same shape:

```text
NN Stage Name/
├── README.md     what the stage does, how to run it, inputs and outputs
├── script.py     the code
├── inputs/       files the code reads   (not committed)
└── outputs/      files the code writes  (not committed)
```

Scripts find their own `inputs/` and `outputs/` folders relative to the script file, so they work whichever folder you run them from.

---

# Setup

1. Create or activate a Python environment (3.11 or later).
2. Install the packages:

```powershell
python -m pip install -r requirements.txt
```

3. Optional: install the repo itself, so `modules` and `prm_opt` import from anywhere (including notebooks opened outside the repo):

```powershell
python -m pip install -e .
```

Scripts already add the repo root to `sys.path` themselves, so this step is a convenience rather than a requirement.

4. Database access uses the ODBC DSN `AzureConnection` and your Windows username. Override either in a `.env` file in the repo root (`DSN=...`, `USERNAME=...`). `.env` is never committed.

---

# Data rules

- **Inputs and outputs are never committed.** `.gitignore` excludes every `inputs/`, `outputs/` and `Outputs/` folder, plus all `.xlsx`, `.xls`, `.csv`, `.parquet`, `.pkl` and `.log` files. The only exception is `config/*.csv` (small model configuration such as forecast weights).
- **Personal data stays outside the repo**, in `Documents/02 Restricted Data/`. This covers movement extracts with driver names, ANPR scans, onsite/due-in exports and ECAC lists. Scripts reach it through `modules/paths.py`.
- **Paths outside the repo live in one place:** `modules/paths.py`. If a file moves, change it there, not in the scripts. Set the `DOCS_ROOT` environment variable if your Documents folder is somewhere else.
- **Clear notebook outputs before committing** (Jupyter: *Edit → Clear All Outputs*). Printed tables can contain passenger-level rows.

---

# Projects

| Folder | Stages |
|---|---|
| [FastPark](FastPark/README.md) | 01 Short Term Model · 02 Reports · 03 Uplift and Diagnostics · 04 Capacity · 05 Movements and Productivity · 06 Christmas Analysis · 07 S26 Wash-up |
| [PRM](PRM/README.md) | 01 Reports · 02 Minibus Analysis · 03 Fleet Optimisation · 04 Minibus Trial · 05 Penetration and Demand |
| [Operations](Operations/README.md) | 01 Tactical Readiness · 02 Check-in · 03 Schedule Analysis |
