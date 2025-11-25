# src/validation/validate_population.py

from pathlib import Path
from typing import Dict, Any

import pandas as pd
from src.metadata.metadata_store import MetadataStore


def _looks_like_la_code(code: Any) -> bool:
    """
    Very simple LA code heuristic:
    - string
    - length 9
    - starts with E or W (England, Wales)
    """
    if not isinstance(code, str):
        return False
    code = code.strip()
    return len(code) == 9 and code[0] in {"E", "W"}


def validate_population_2022(
    clean_path: str | Path = "data/processed/population_clean_2022.csv",
) -> Dict[str, Any]:
    """
    Validate the harmonised mid-2022 population dataset.

    Checks:
    - missing values
    - invalid LA codes
    - population > 0
    - calendar_year == 2022
    """
    clean_path = Path(clean_path)
    df = pd.read_csv(clean_path)

    missing = df.isnull().sum().to_dict()

    invalid_la_mask = ~df["local_authority_code"].astype(str).map(_looks_like_la_code)
    invalid_la_count = int(invalid_la_mask.sum())

    # population must be positive
    invalid_pop_mask = (df["population"] <= 0) | df["population"].isna()
    invalid_pop_count = int(invalid_pop_mask.sum())

    # year check
    year_values = df["calendar_year"].unique().tolist()
    year_ok = all(y == 2022 for y in year_values)
    year_issue = [] if year_ok else year_values

    issues = {
        "missing_values": missing,
        "invalid_la_code_count": invalid_la_count,
        "invalid_population_count": invalid_pop_count,
        "year_values": year_values,
        "year_expected": 2022,
        "year_mismatch": year_issue,
    }

    store = MetadataStore()
    store.add_event(
        stage="validation_population",
        action="validate_population_2022",
        details={
            "rows": int(df.shape[0]),
            "columns": int(df.shape[1]),
            "issues": issues,
        },
    )

    return issues
