"""Validates processed data to ensure quality and consistency."""

import re
import pandas as pd
from src.metadata.metadata_store import MetadataStore

CLEAN_PATH = "data/processed/clean_emissions.csv"


def validate_data(input_path=CLEAN_PATH):
    store = MetadataStore()
    issues = {}

    try:
        df = pd.read_csv(input_path)

        # --------------------------
        # Required Columns Check
        # --------------------------
        required_cols = [
            "country",
            "country_code",
            "region",
            "region_code",
            "local_authority",
            "local_authority_code",
            "calendar_year",
            "la_ghg_sector",
            "la_ghg_sub_sector",
            "greenhouse_gas",
            "territorial_emissions_kt_co2e",
        ]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            issues["missing_required_columns"] = missing_cols

        # --------------------------
        # Missing Data Check
        # --------------------------
        missing_counts = df.isnull().sum().to_dict()
        issues["missing_values"] = missing_counts

        # --------------------------
        # Local Authority Code Format
        # --------------------------
        la_pattern = re.compile(r"^[A-Z][A-Z0-9]{5,8}$")
        invalid_la = df[
            ~df["local_authority_code"].astype(str).str.match(la_pattern)
        ]
        issues["invalid_la_code_count"] = len(invalid_la)

        # --------------------------
        # Year Range Check
        # --------------------------
        out_of_range_years = df[
            (df["calendar_year"] < 2005) | (df["calendar_year"] > 2022)
        ]
        issues["out_of_range_years"] = len(out_of_range_years)

        # --------------------------
        # Numeric Column Validation
        # --------------------------
        numeric_cols = [
            "territorial_emissions_kt_co2e",
            "co2_emissions_within_the_scope_of_influence_of_las_kt_co2",
            "mid_year_population_thousands",
            "area_km2",
        ]
        invalid_numeric = {}
        for col in numeric_cols:
            if col in df.columns:
                invalid_numeric[col] = df[col].isnull().sum()
        issues["numeric_null_counts"] = invalid_numeric

        # Log metadata
        store.add_event(
            stage="validation",
            action="validate_data",
            details={
                "input": input_path,
                "rows": df.shape[0],
                "columns": df.shape[1],
                "issues": issues,
            },
        )

        return issues

    except Exception as e:
        store.add_event(
            stage="validation",
            action="validate_data",
            details={"input": input_path, "status": "failed", "error": str(e)},
        )
        raise


def run():
    return validate_data()
