"""Entry point for coordinating the evidence pipeline stages."""

from src.ingestion.fetch_dataset import fetch_csv
from src.harmonisation.clean_schema import clean_schema
from src.validation.validate_data import validate_data
from src.agent.repair_agent import run as agent_run
from src.agent.record_classifier_agent import run as classify_run
from src.reporting.report_generator import generate_report
from src.visualisation.plot_evidence import generate_all_plots



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

    print("Running diagnostic agent...")
    report = agent_run()
    print("Diagnostic report:")
    print(report)

    print("Running record classifier agent...")
    output_path, counts = classify_run()
    print("Classification complete.")
    print(counts)

    print("Generating evidence report...")
    report_path = generate_report()
    print(f"Evidence report written to: {report_path}")

    print("Generating visualisations...")
    generate_all_plots(
        clean_data_path="data/processed/clean_emissions.csv",
        classified_path="data/processed/classified_emissions.csv",
        output_dir="outputs/visuals/"
    )
    print("Visualisations saved to outputs/visuals/")




