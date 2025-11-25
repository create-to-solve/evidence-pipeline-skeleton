# src/pipeline_population.py

from src.ingestion.fetch_population import fetch_population_2022
from src.harmonisation.clean_population import clean_population_2022
from src.validation.validate_population import validate_population_2022


def run_population() -> None:
    """
    Run the mid-2022 population (LA-level) mini-pipeline:

    1. Ingestion (download Excel)
    2. Harmonisation (to CSV)
    3. Validation (basic checks)
    """
    print("Running population ingestion (mid-2022)...")
    raw_path = fetch_population_2022()
    print(f"Population raw file saved to: {raw_path}")

    print("Running population harmonisation...")
    clean_path = clean_population_2022(input_path=raw_path)
    print(f"Population clean file saved to: {clean_path}")

    print("Running population validation...")
    issues = validate_population_2022(clean_path)
    print("Population validation complete.")
    print(issues)
