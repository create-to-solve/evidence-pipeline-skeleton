"""Classifies rows in the cleaned emissions dataset into meaningful record types."""

import pandas as pd
from pathlib import Path
from src.metadata.metadata_store import MetadataStore


CLEAN_PATH = "data/processed/clean_emissions.csv"
OUTPUT_PATH = "data/processed/classified_emissions.csv"


class RecordClassifierAgent:
    """
    Reads the cleaned emissions dataset and assigns each row a record_type label:
    - local_authority
    - sector
    - subsector
    - regional_aggregate
    - national_aggregate
    - unknown
    """

    def __init__(self, input_path=CLEAN_PATH, output_path=OUTPUT_PATH):
        self.input_path = input_path
        self.output_path = output_path
        self.store = MetadataStore()

    def classify_row(self, row):
        """Apply rule-based classification to a single row."""

        la_code = str(row.get("local_authority_code", "")).strip()
        region = row.get("region")
        country = row.get("country")

        population = row.get("mid_year_population_thousands")
        area = row.get("area_km2")

        sector = row.get("la_ghg_sector")
        subsector = row.get("la_ghg_sub_sector")

        # 1. Local Authority record
        if (
            la_code not in ["", "nan", None]
            and pd.notnull(population)
            and pd.notnull(area)
        ):
            return "local_authority"

        # 2. Subsector-level record
        if pd.notnull(subsector) and pd.isnull(row.get("local_authority_code")):
            return "subsector"

        # 3. Sector-level record
        if pd.notnull(sector) and pd.isnull(row.get("local_authority_code")):
            return "sector"

        # 4. Regional aggregate
        if pd.notnull(region) and pd.isnull(row.get("local_authority_code")):
            return "regional_aggregate"

        # 5. National aggregate
        if (country == "United Kingdom") and pd.isnull(region):
            return "national_aggregate"

        # 6. Unknown fallback
        return "unknown"

    def run(self):
        df = pd.read_csv(self.input_path)

        df["record_type"] = df.apply(self.classify_row, axis=1)

        # Save output
        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_path, index=False)

        # Count
        counts = df["record_type"].value_counts().to_dict()

        # Log metadata
        self.store.add_event(
            stage="agent",
            action="record_classification",
            details={
                "input": self.input_path,
                "output": self.output_path,
                "record_type_counts": counts,
            },
        )

        return self.output_path, counts


def run():
    agent = RecordClassifierAgent()
    return agent.run()
