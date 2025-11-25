# src/indicators/emissions_per_capita.py

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.metadata.metadata_store import MetadataStore


def compute_emissions_per_capita_2021(
    emissions_path: str | Path = "data/processed/emissions_2021_la_totals.csv",
    population_path: str | Path = "data/processed/population_clean_2022.csv",
    output_path: str | Path = "data/processed/emissions_per_capita_2021.csv",
) -> str:
    """
    Join 2021 LA territorial CO2 totals (kt) with 2022 population
    to compute per-capita emissions (t CO2 per person).

    Output:
    - local_authority_code
    - local_authority
    - population
    - emissions_kt_co2e
    - per_capita_tonnes
    """
    emissions_path = Path(emissions_path)
    population_path = Path(population_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    store = MetadataStore()

    df_e = pd.read_csv(emissions_path)
    df_p = pd.read_csv(population_path)

    # Normalise codes
    df_e["local_authority_code"] = df_e["local_authority_code"].astype(str).str.strip()
    df_p["local_authority_code"] = df_p["local_authority_code"].astype(str).str.strip()

    # Inner-join on codes (this naturally restricts to E&W)
    merged = df_e.merge(
        df_p,
        on="local_authority_code",
        how="inner",
        suffixes=("_em", "_pop"),
    )

    # Convert kt to tonnes
    merged["emissions_tonnes"] = merged["emissions_kt_co2e"] * 1000.0

    # Compute per-capita
    merged["per_capita_tonnes"] = merged["emissions_tonnes"] / merged["population"]

    out = merged[
        [
            "local_authority_code",
            "local_authority_em",
            "population",
            "emissions_kt_co2e",
            "per_capita_tonnes",
        ]
    ].copy()

    out.rename(
        columns={
            "local_authority_em": "local_authority",
        },
        inplace=True,
    )

    out.to_csv(output_path, index=False)

    store.add_event(
        stage="indicators",
        action="compute_per_capita_2021",
        details={
            "output": str(output_path),
            "rows": int(out.shape[0]),
            "columns": int(out.shape[1]),
        },
    )

    return str(output_path)
