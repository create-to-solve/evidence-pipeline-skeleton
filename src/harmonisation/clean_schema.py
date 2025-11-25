"""Prepares and standardizes dataset schemas for downstream processing."""

import os
import pandas as pd
from pathlib import Path
from src.metadata.metadata_store import MetadataStore


RAW_PATH = "data/raw/ons_co2_emissions.csv"
CLEAN_PATH = "data/processed/clean_emissions.csv"


def normalize_column(name: str) -> str:
    """Normalize column names: lowercase, remove special chars, underscores."""
    name = name.lower()
    name = name.replace("(", "").replace(")", "")
    name = name.replace("/", "_per_")
    name = name.replace(" ", "_")
    name = name.replace("-", "_")
    return name


def clean_schema(input_path=RAW_PATH, output_path=CLEAN_PATH):
    store = MetadataStore()

    try:
        # Load full CSV
        df = pd.read_csv(input_path)

        # Normalize column names
        df.columns = [normalize_column(c) for c in df.columns]

        # Standardize local authority code
        if "local_authority_code" in df.columns:
            df["local_authority_code"] = df["local_authority_code"].astype(str).str.upper()

        # Convert numeric fields where appropriate
        numeric_candidates = [
            "territorial_emissions_kt_co2e",
            "co2_emissions_within_the_scope_of_influence_of_las_kt_co2",
            "mid-year_population_thousands",
            "area_km2",
        ]

        for col in numeric_candidates:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Ensure directory exists
        Path(os.path.dirname(output_path)).mkdir(parents=True, exist_ok=True)

        # Save cleaned dataset
        df.to_csv(output_path, index=False)

        store.add_event(
            stage="harmonisation",
            action="clean_schema",
            details={
                "input": input_path,
                "output": output_path,
                "columns_cleaned": df.shape[1],
                "rows": df.shape[0],
            },
        )

        return output_path

    except Exception as e:
        store.add_event(
            stage="harmonisation",
            action="clean_schema",
            details={"input": input_path, "status": "failed", "error": str(e)},
        )
        raise


def run():
    """Entry point for module."""
    return clean_schema()
