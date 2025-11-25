"""Handles retrieval of external datasets for ingestion into the pipeline."""

import os
import requests
from pathlib import Path
from src.metadata.metadata_store import MetadataStore


DATA_URL = (
    "https://assets.publishing.service.gov.uk/media/667ad86497ea0c79abfe4bfd/"
    "2005-2022-local-authority-ghg-emissions-csv-dataset.csv"
)


def fetch_csv(output_path="data/raw/ons_co2_emissions.csv"):
    """Download the ONS CO2 emissions dataset and store it locally."""
    store = MetadataStore()

    # Ensure parent directory exists
    Path(os.path.dirname(output_path)).mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get(DATA_URL)
        response.raise_for_status()
        with open(output_path, "wb") as f:
            f.write(response.content)

        store.add_event(
            stage="ingestion",
            action="fetch_csv",
            details={
                "url": DATA_URL,
                "output_path": output_path,
                "status": "success",
            },
        )
        return output_path

    except Exception as e:
        store.add_event(
            stage="ingestion",
            action="fetch_csv",
            details={
                "url": DATA_URL,
                "output_path": output_path,
                "status": "failed",
                "error": str(e),
            },
        )
        raise


def run():
    """Entry point for this module."""
    return fetch_csv()
