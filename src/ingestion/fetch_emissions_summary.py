# src/ingestion/fetch_emissions_summary.py

from __future__ import annotations

from pathlib import Path
from typing import Optional

import requests

from src.metadata.metadata_store import MetadataStore

# Official DESNZ workbook:
# 2005–2021 UK local authority GHG emissions – data tables (Excel)
SUMMARY_URL = (
    "https://assets.publishing.service.gov.uk/government/uploads/"
    "system/uploads/attachment_data/file/1166194/2005-21-uk-local-authority-ghg-emissions.xlsx"
)


def fetch_emissions_summary(
    url: str = SUMMARY_URL,
    output_path: str | Path = "data/raw/uk_local_authority_ghg_2005_2021.xlsx",
    timeout: int = 60,
) -> str:
    """
    Download the official UK local authority GHG workbook that includes
    the LA CO2 territorial Table 2, and save it under data/raw.

    Returns the path to the downloaded file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    store = MetadataStore()

    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        output_path.write_bytes(resp.content)

        store.add_event(
            stage="ingestion",
            action="fetch_emissions_summary",
            details={
                "url": url,
                "output_path": str(output_path),
                "status": "success",
                "content_type": resp.headers.get("Content-Type", ""),
                "size_bytes": len(resp.content),
            },
        )
    except Exception as exc:
        store.add_event(
            stage="ingestion",
            action="fetch_emissions_summary",
            details={
                "url": url,
                "output_path": str(output_path),
                "status": "error",
                "error": repr(exc),
            },
        )
        raise

    return str(output_path)
