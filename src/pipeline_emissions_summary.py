# src/pipeline_emissions_summary.py

from __future__ import annotations
from pathlib import Path

from src.ingestion.fetch_emissions_summary import fetch_emissions_summary
from src.harmonisation.clean_emissions_summary import clean_emissions_summary_2021
from src.validation.validate_emissions_summary import validate_emissions_summary_2021
from src.indicators.emissions_per_capita import compute_emissions_per_capita_2021

# NEW: import ingestion assistant
from src.agent.ingestion_assistant_agent import analyze_file


def run_emissions_summary_pipeline() -> None:
    print("Fetching LA GHG summary workbook...")
    raw_path = fetch_emissions_summary()
    raw_path = Path(raw_path)

    # ------------------------------------------------------------
    # Step 1: Run ingestion assistant
    # ------------------------------------------------------------
    print("Running ingestion assistant...")
    analysis = analyze_file(raw_path, hint="la_territorial_summary")

    sheet_name = analysis["recommended_sheet"]
    header_row = analysis["recommended_header_row"]

    print(f"Ingestion assistant suggests sheet='{sheet_name}', header_row={header_row}")
    # ------------------------------------------------------------

    print("Harmonising 2021 LA territorial CO2 totals...")
    clean_path = clean_emissions_summary_2021(
        input_path=raw_path,
        sheet_name=sheet_name,
        header_row=header_row,
    )
    print(f"Harmonised 2021 LA totals saved to: {clean_path}")

    print("Validating 2021 LA totals...")
    issues = validate_emissions_summary_2021(clean_path)
    print(f"Validation issues: {issues}")

    print("Computing 2021 per-capita emissions (using 2022 population)...")
    out = compute_emissions_per_capita_2021()
    print(f"Per-capita indicator written to: {out}")
