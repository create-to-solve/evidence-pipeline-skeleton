# src/harmonisation/clean_emissions_summary.py

from __future__ import annotations
import pandas as pd
from pathlib import Path
from src.metadata.metadata_store import MetadataStore


def clean_emissions_summary_2021(
    input_path: str | Path = "data/raw/uk_local_authority_ghg_2005_2021.xlsx",
    output_path: str | Path = "data/processed/emissions_2021_la_totals.csv",
    sheet_name: str | None = None,
    header_row: int | None = None,
) -> str:
    """
    Clean the 2021 territorial CO2 totals from the DESNZ summary workbook.
    Optional sheet_name and header_row parameters allow integration with the
    ingestion assistant agent.
    """

    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    store = MetadataStore()

    # Defaults if agent is not used
    final_sheet = sheet_name or "2_1"
    final_header = header_row if header_row is not None else 4

    # Read the Excel sheet
    df = pd.read_excel(
        input_path,
        sheet_name=final_sheet,
        header=final_header,
    )

    # Drop fully empty rows
    df = df.dropna(how="all")

    # Normalise column names
    cols = {c: str(c).strip() for c in df.columns}
    df = df.rename(columns=cols)

    # Expected columns in DESNZ summary workbook
    code_col = "Local Authority Code"
    name_col = "Local Authority"
    year_col = "Calendar Year"
    total_col = "Grand Total"

    missing = [c for c in [code_col, name_col, year_col, total_col] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    # Filter for 2021 rows only
    df_2021 = df[df[year_col] == 2021].copy()

    # Build output dataframe
    out = pd.DataFrame(
        {
            "local_authority_code": df_2021[code_col].astype(str).str.strip(),
            "local_authority": df_2021[name_col].astype(str).str.strip(),
            "emissions_kt_co2e": pd.to_numeric(df_2021[total_col], errors="coerce"),
            "calendar_year": 2021,
        }
    )

    # Drop rows with NaN emissions
    out = out[out["emissions_kt_co2e"].notna()]

    # Save cleaned output
    out.to_csv(output_path, index=False)

    # Metadata logging
    store.add_event(
        stage="harmonisation",
        action="clean_emissions_summary_2021",
        details={
            "input": str(input_path),
            "output": str(output_path),
            "rows": int(out.shape[0]),
            "columns": int(out.shape[1]),
            "sheet": final_sheet,
            "header_row": final_header,
        },
    )
    store.save()

    return str(output_path)
