"""Generate a human-readable evidence report for the latest pipeline run."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from src.reporting.indicator_summary import generate_indicator_section


import pandas as pd


def _load_metadata(path: Path) -> List[Dict[str, Any]]:
    """Load the metadata event list from JSON."""
    if not path.exists():
        raise FileNotFoundError(f"Metadata file not found at {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _find_latest_event(
    events: List[Dict[str, Any]],
    stage: str,
    action: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return the most recent event matching stage (and optional action)."""
    for event in reversed(events):
        if event.get("stage") != stage:
            continue
        if action is not None and event.get("action") != action:
            continue
        return event
    return None


def _format_missing_values(missing: Dict[str, Any]) -> str:
    """Format missing-values dict as a markdown bullet list, sorted by count."""
    if not missing:
        return "_No missing values recorded._"
    lines: List[str] = ["### Missing values by column", ""]
    for col, count in sorted(missing.items(), key=lambda kv: kv[1], reverse=True):
        lines.append(f"- **{col}**: {count}")
    return "\n".join(lines)


def _format_numeric_nulls(numeric_nulls: Dict[str, Any]) -> str:
    """Format numeric null counts as markdown."""
    if not numeric_nulls:
        return "_No numeric nulls recorded._"
    lines: List[str] = ["### Numeric nulls by column", ""]
    for col, count in numeric_nulls.items():
        lines.append(f"- **{col}**: {count}")
    return "\n".join(lines)


def _summarise_classification(classified_path: Path) -> str:
    """Produce a simple markdown summary of classification counts, if possible."""
    if not classified_path.exists():
        return "_Classification file not found; no classification summary available._"

    try:
        df = pd.read_csv(classified_path)
    except Exception as exc:  # pragma: no cover
        return f"_Could not read classification file: {exc}_"

    candidate_cols = ["record_type", "row_type", "classification", "class", "category"]
    col = next((c for c in candidate_cols if c in df.columns), None)

    if col is None:
        # Fallback: just show total rows
        return (
            "_No obvious classification column found in "
            "`classified_emissions.csv` (showing total rows only)._"
            f"\n\n- **Total rows**: {len(df)}"
        )

    counts = df[col].value_counts().to_dict()
    lines: List[str] = [
        f"Classification counts (by `{col}`):",
        "",
    ]
    for label, count in counts.items():
        lines.append(f"- **{label}**: {count}")
    return "\n".join(lines)


def generate_report(
    metadata_path: str | Path = "data/processed/metadata.json",
    clean_data_path: str | Path = "data/processed/clean_emissions.csv",
    classified_path: str | Path = "data/processed/classified_emissions.csv",
    output_path: str | Path = "outputs/report.md",
) -> Path:
    """
    Generate a markdown evidence report summarising the latest pipeline run.

    Returns
    -------
    Path
        Path to the generated report file.
    """
    metadata_path = Path(metadata_path)
    clean_data_path = Path(clean_data_path)
    classified_path = Path(classified_path)
    output_path = Path(output_path)

    events = _load_metadata(metadata_path)

    ingestion_event = _find_latest_event(events, stage="ingestion", action="fetch_csv")
    harmonisation_event = _find_latest_event(
        events, stage="harmonisation", action="clean_schema"
    )
    validation_event = _find_latest_event(
        events, stage="validation", action="validate_data"
    )

    # Optional: look for any diagnostic agent event
    # Find the latest diagnostic agent event
    diagnostic_event = None
    for ev in reversed(events):
        if ev.get("stage") == "agent" and ev.get("action") in {
            "diagnostic",
            "diagnostic_agent",
            "diagnostic_report",
        }:
            diagnostic_event = ev
            break


    # Basic row / column counts
    total_rows: Optional[int] = None
    total_columns: Optional[int] = None

    if harmonisation_event is not None:
        d = harmonisation_event.get("details", {})
        total_rows = d.get("rows")
        # columns_cleaned is what your metadata currently stores :contentReference[oaicite:4]{index=4}
        total_columns = d.get("columns") or d.get("columns_cleaned")

    if total_rows is None or total_columns is None:
        if clean_data_path.exists():
            df_clean = pd.read_csv(clean_data_path)
            total_rows = len(df_clean)
            total_columns = len(df_clean.columns)

    now = datetime.now(timezone.utc).isoformat()

    lines: List[str] = []

    # 1. Header
    lines.append("# Evidence Pipeline Report")
    lines.append("")
    lines.append(f"Generated: `{now}`")
    lines.append("")

    # 2. Ingestion Summary
    lines.append("## 1. Ingestion Summary")
    lines.append("")
    if ingestion_event is None:
        lines.append("_No ingestion event found in metadata._")
    else:
        d = ingestion_event.get("details", {})
        lines.append(f"- **Dataset URL**: {d.get('url', 'N/A')}")
        lines.append(f"- **Output path**: `{d.get('output_path', 'N/A')}`")
        lines.append(f"- **Status**: {d.get('status', 'N/A')}")
        error = d.get("error")
        if error:
            lines.append(f"- **Last error**: `{error}`")
    lines.append("")

    # 3. Harmonisation Summary
    lines.append("## 2. Harmonisation Summary")
    lines.append("")
    if harmonisation_event is None:
        lines.append("_No harmonisation event found in metadata._")
    else:
        d = harmonisation_event.get("details", {})
        lines.append(f"- **Input file**: `{d.get('input', 'N/A')}`")
        lines.append(f"- **Output file**: `{d.get('output', 'N/A')}`")
        if total_rows is not None:
            lines.append(f"- **Rows**: {total_rows}")
        if total_columns is not None:
            lines.append(f"- **Columns**: {total_columns}")
    lines.append("")

    # 4. Validation Summary
    lines.append("## 3. Validation Summary")
    lines.append("")
    if validation_event is None:
        lines.append("_No validation event found in metadata._")
    else:
        details = validation_event.get("details", {})
        issues = details.get("issues", {})
        missing = issues.get("missing_values", {})
        invalid_la = issues.get("invalid_la_code_count")
        out_of_range_years = issues.get("out_of_range_years")
        numeric_nulls = issues.get("numeric_null_counts", {})

        lines.append(f"- **Rows checked**: {details.get('rows', 'N/A')}")
        lines.append(f"- **Columns**: {details.get('columns', 'N/A')}")
        if invalid_la is not None:
            lines.append(f"- **Invalid LA codes**: {invalid_la}")
        if out_of_range_years is not None:
            lines.append(f"- **Out-of-range years**: {out_of_range_years}")
        lines.append("")
        if missing:
            lines.append(_format_missing_values(missing))
            lines.append("")
        if numeric_nulls:
            lines.append(_format_numeric_nulls(numeric_nulls))
            lines.append("")

    # 5. Diagnostic Agent Summary
        
    lines.append("## 4. Diagnostic Agent Summary")
    lines.append("")
    if diagnostic_event is None:
        lines.append("_No diagnostic agent event found in metadata._")
    else:
        d = diagnostic_event.get("details", {})

        # Your agent uses "issues_detected", not "summary"
        summary = d.get("issues_detected") or d.get("summary")

        recommended = d.get("recommended_actions") or d.get("recommendations")

        if summary:
            lines.append("### Summary")
            lines.append("")
            for k, v in summary.items():
                lines.append(f"- **{k}**: {v}")
            lines.append("")

        if recommended:
            lines.append("### Recommended actions")
            lines.append("")
            for item in recommended:
                lines.append(f"- {item}")
            lines.append("")

    # 6. Classification Summary
    lines.append("## 5. Classification Summary")
    lines.append("")
    lines.append(_summarise_classification(classified_path))
    lines.append("")

    # 7. Per-Capita Indicator Summary
    indicator_section = generate_indicator_section()
    lines.append(indicator_section)
    lines.append("")

    # 8. Output Artefacts
    lines.append("## 6. Output Artefacts")
    lines.append("")
    raw_path = Path("data/raw/ons_co2_emissions.csv")
    lines.append(f"- Raw CSV: `{raw_path}`")
    lines.append(f"- Clean CSV: `{clean_data_path}`")
    lines.append(f"- Classified CSV (if present): `{classified_path}`")
    lines.append(f"- Metadata: `{metadata_path}`")
    lines.append(f"- Report: `{output_path}`")
    lines.append("")

    # 8. Footer
    lines.append("---")
    lines.append("")
    lines.append("_Generated by `evidence-pipeline-skeleton`._")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return output_path
