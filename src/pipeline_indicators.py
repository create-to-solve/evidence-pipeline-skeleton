from src.indicators.emissions_per_capita import compute_emissions_per_capita_2022

def run_indicators():
    print("Computing per-capita emissions (2022)...")
    out = compute_emissions_per_capita_2022()
    print(f"Per-capita emissions dataset written to: {out}")
