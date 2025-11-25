# src/harmonisation/clean_population.py

from pathlib import Path
import pandas as pd
from src.metadata.metadata_store import MetadataStore


def clean_population_2022(
    input_path: str | Path = "data/raw/population_2022.xlsx",
    output_path: str | Path = "data/processed/population_clean_2022.csv",
) -> str:
    """
    Harmonise the ONS mid-2022 LA population estimates into the standard schema:

    - local_authority_code
    - local_authority
    - calendar_year (2022)
    - population
    """

    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    store = MetadataStore()

    # Read the *actual* table (header row = 7)
    df = pd.read_excel(
        input_path,
        sheet_name="MYE2 - Persons",
        header=7,
    )

    # Select only the needed columns
    if not {"Code", "Name", "All ages"} <= set(df.columns):
        raise ValueError(f"Expected columns not found in population dataset. Found: {df.columns}")

    out = df[["Code", "Name", "All ages"]].copy()

    # Rename to standard schema
    out.rename(
        columns={
            "Code": "local_authority_code",
            "Name": "local_authority",
            "All ages": "population",
        },
        inplace=True,
    )

    # Add calendar year
    out["calendar_year"] = 2022

    # Basic cleanup
    out["local_authority_code"] = out["local_authority_code"].astype(str).str.strip()
    out["local_authority"] = out["local_authority"].astype(str).str.strip()
    out["population"] = pd.to_numeric(out["population"], errors="coerce")

    # Drop blank LA rows (some spreadsheets include summary rows)
    out = out.dropna(subset=["local_authority_code", "population"])

    out.to_csv(output_path, index=False)

    # Log metadata
    store.add_event(
        stage="harmonisation_population",
        action="clean_population_2022",
        details={
            "input": str(input_path),
            "output": str(output_path),
            "rows": int(out.shape[0]),
            "columns": int(out.shape[1]),
        },
    )

    return str(output_path)
