# src/validation/validate_emissions_summary.py

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd

from src.metadata.metadata_store import MetadataStore


def validate_emissions_summary_2021(
    input_path: str | Path = "data/processed/emissions_2021_la_totals.csv",
) -> Dict[str, Any]:
    """
    Basic validation for LA-level 2021 territorial CO2 totals.

    Checks:
    - no missing codes or names
    - non-negative emissions
    - duplicate codes
    """
    input_path = Path(input_path)
    df = pd.read_csv(input_path)

    issues: Dict[str, Any] = {}

    issues["row_count"] = int(df.shape[0])
    issues["missing_codes"] = int(df["local_authority_code"].isna().sum())
    issues["missing_names"] = int(df["local_authority"].isna().sum())
    issues["missing_emissions"] = int(df["emissions_kt_co2e"].isna().sum())
    issues["negative_emissions"] = int((df["emissions_kt_co2e"] < 0).sum())

    # duplicate codes
    dup_codes = df["local_authority_code"].value_counts()
    dup_codes = dup_codes[dup_codes > 1]
    issues["duplicate_codes_count"] = int(dup_codes.shape[0])

    store = MetadataStore()
    store.add_event(
        stage="validation",
        action="validate_emissions_summary_2021",
        details={
            "input": str(input_path),
            "rows": issues["row_count"],
            "issues": issues,
        },
    )

    return issues
