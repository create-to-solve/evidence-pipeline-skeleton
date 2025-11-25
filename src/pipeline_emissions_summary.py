# src/pipeline_emissions_summary.py

from src.ingestion.fetch_emissions_summary import fetch_emissions_summary
from src.harmonisation.clean_emissions_summary import clean_emissions_summary_2021
from src.validation.validate_emissions_summary import validate_emissions_summary_2021
from src.indicators.emissions_per_capita import compute_emissions_per_capita_2021


def run_emissions_summary_pipeline():
    print("Fetching LA GHG summary workbook...")
    raw_path = fetch_emissions_summary()
    print(f"Workbook saved to: {raw_path}")

    print("Harmonising 2021 LA territorial CO2 totals...")
    clean_path = clean_emissions_summary_2021(input_path=raw_path)
    print(f"Harmonised 2021 LA totals saved to: {clean_path}")

    print("Validating 2021 LA totals...")
    issues = validate_emissions_summary_2021(input_path=clean_path)
    print("Validation issues:", issues)

    print("Computing 2021 per-capita emissions (using 2022 population)...")
    out = compute_emissions_per_capita_2021()
    print(f"Per-capita indicator written to: {out}")
