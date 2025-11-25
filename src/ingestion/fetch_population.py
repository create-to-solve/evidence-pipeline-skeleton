# src/ingestion/fetch_population.py

from pathlib import Path
import requests
from src.metadata.metadata_store import MetadataStore

# Mid-2022: 2023 local authority boundaries edition (LA level)
POPULATION_2022_URL = (
    "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/"
    "populationandmigration/populationestimates/datasets/"
    "estimatesofthepopulationforenglandandwales/"
    "mid20222023localauthorityboundaires/mye22tablesew2023geogs.xlsx"
)


def fetch_population_2022(
    output_path: str | Path = "data/raw/population_2022.xlsx",
) -> str:
    """
    Download the ONS mid-2022 local authority population estimates (England & Wales)
    and save as a raw Excel file.

    Returns
    -------
    str
        Path to the downloaded file (as string).
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    store = MetadataStore()

    try:
        resp = requests.get(POPULATION_2022_URL, timeout=60)
        resp.raise_for_status()
        output_path.write_bytes(resp.content)

        store.add_event(
            stage="ingestion_population",
            action="fetch_population_2022",
            details={
                "url": POPULATION_2022_URL,
                "output_path": str(output_path),
                "status": "success",
            },
        )
        return str(output_path)
    except Exception as exc:
        store.add_event(
            stage="ingestion_population",
            action="fetch_population_2022",
            details={
                "url": POPULATION_2022_URL,
                "output_path": str(output_path),
                "status": "error",
                "error": str(exc),
            },
        )
        raise
