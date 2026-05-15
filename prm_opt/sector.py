#prm_opt.sector.py

def normalise_sector(sector: str) -> str:
    s = str(sector).strip().upper()

    if s in {"DOMESTIC", "NIRISH", "DOM"}:
        return "Domestic"
    if s in {"INTERNATIONAL", "INT"}:
        return "International"
    if s in {"CTA", "IRISH"}:
        return "CTA"

    raise ValueError(f"Unknown sector value: {sector}")