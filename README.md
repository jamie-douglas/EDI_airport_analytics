# EDI_airport_analytics

This repository contains helper packages for generating various airport analytics reports.  Two sample packages live under `scripts/`:

* `scripts/checkin` – check‑in performance calculations, originally in `checkin.py`.
* `scripts/prm` – PRM demand & budget reporting, originally in `PRM report.py`.

Each package exposes a `run()` function; start either from the command line with:

```powershell
python -m scripts.checkin.main    # run check‑in report
python -m scripts.prm.main        # run PRM report
```

Because logic is split into small modules (``db``, ``loaders``, ``metrics``/``calculations`` etc.) it's easier to test and reuse individual pieces.
