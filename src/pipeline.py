"""Entry point for coordinating the evidence pipeline stages."""

from src.ingestion.fetch_dataset import fetch_csv
from src.harmonisation.clean_schema import clean_schema
from src.validation.validate_data import validate_data


def run():
    print("Running ingestion...")
    raw_path = fetch_csv()
    print(f"Raw dataset saved to: {raw_path}")

    print("Running harmonisation...")
    clean_path = clean_schema()
    print(f"Clean dataset saved to: {clean_path}")

    print("Running validation...")
    issues = validate_data()
    print("Validation complete.")
    print(issues)
