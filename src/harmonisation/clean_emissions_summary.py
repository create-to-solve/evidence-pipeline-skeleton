# src/harmonisation/clean_emissions_summary.py

from __future__ import annotations
import pandas as pd
from pathlib import Path
from src.metadata.metadata_store import MetadataStore


def clean_emissions_summary_2021(
    input_path: str | Path = "data/raw/uk_local_authority_ghg_2005_2021.xlsx",
    output_path: str | Path = "data/processed/emissions_2021_la_totals.csv",
) -> str:

    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    store = MetadataStore()

    # The real header row is 4 (zero-indexed)
    df = pd.read_excel(input_path, sheet_name="2_1", header=4)

    # Drop fully empty rows
    df = df.dropna(how="all")

    # Normalise column names
    cols = {c: str(c).strip() for c in df.columns}
    df = df.rename(columns=cols)

    # Required columns
    code_col = "Local Authority Code"
    name_col = "Local Authority"
    year_col = "Calendar Year"
    total_col = "Grand Total"

    missing = [c for c in [code_col, name_col, year_col, total_col] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    # Filter for 2021
    df_2021 = df[df[year_col] == 2021].copy()

    # Build output frame
    out = pd.DataFrame(
        {
            "local_authority_code": df_2021[code_col].astype(str).str.strip(),
            "local_authority": df_2021[name_col].astype(str).str.strip(),
            "emissions_kt_co2e": pd.to_numeric(df_2021[total_col], errors="coerce"),
            "calendar_year": 2021,
        }
    )

    # Clean anomalies
    out = out[out["emissions_kt_co2e"].notna()]

    out.to_csv(output_path, index=False)

    store.add_event(
        stage="harmonisation",
        action="clean_emissions_summary_2021",
        details={
            "input": str(input_path),
            "output": str(output_path),
            "rows": int(out.shape[0]),
            "columns": int(out.shape[1]),
        },
    )

    return str(output_path)
