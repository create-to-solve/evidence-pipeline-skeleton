from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Any, List


def _md_table(df: pd.DataFrame, max_rows: int = 10) -> str:
    """
    Convert a small DataFrame to a simple Markdown table.
    """
    if df.empty:
        return "_No data available._"

    df = df.head(max_rows)
    lines: List[str] = []

    # Header
    lines.append("| " + " | ".join(df.columns) + " |")
    lines.append("| " + " | ".join(["---"] * len(df.columns)) + " |")

    # Rows
    for _, row in df.iterrows():
        values = [str(v) for v in row.values]
        lines.append("| " + " | ".join(values) + " |")

    return "\n".join(lines)


def generate_indicator_section(
    per_capita_path: str | Path = "data/processed/emissions_per_capita_2021.csv",
) -> str:
    """
    Generate a Markdown section summarising 2021 per-capita territorial CO2 emissions.
    """
    per_capita_path = Path(per_capita_path)

    if not per_capita_path.exists():
        return "_Per-capita indicator file not found._"

    df = pd.read_csv(per_capita_path)

    # Basic stats
    values = df["per_capita_tonnes"].dropna()

    mean_val = round(values.mean(), 2)
    median_val = round(values.median(), 2)
    max_val = round(values.max(), 2)
    min_val = round(values.min(), 2)

    # Top/bottom 10
    top10 = df.sort_values("per_capita_tonnes", ascending=False)[
        ["local_authority", "local_authority_code", "per_capita_tonnes"]
    ].head(10)

    bottom10 = df.sort_values("per_capita_tonnes", ascending=True)[
        ["local_authority", "local_authority_code", "per_capita_tonnes"]
    ].head(10)

    lines: List[str] = []
    lines.append("## 7. Per-Capita Emissions Summary (2021)")
    lines.append("")
    lines.append(f"- **Local Authorities included**: {len(df)}")
    lines.append(f"- **Mean**: {mean_val} tonnes CO₂ per person")
    lines.append(f"- **Median**: {median_val} tonnes CO₂ per person")
    lines.append(f"- **Minimum**: {min_val}")
    lines.append(f"- **Maximum**: {max_val}")
    lines.append("")

    lines.append("### Top 10 Highest Per-Capita Emitters")
    lines.append(_md_table(top10))
    lines.append("")

    lines.append("### Top 10 Lowest Per-Capita Emitters")
    lines.append(_md_table(bottom10))
    lines.append("")

    # Reference visuals if present
    vis_dir = Path("outputs/visuals")
    if vis_dir.exists():
        lines.append("### Visualisations")
        lines.append("")
        lines.append(
            f"- Per-capita distribution: `outputs/visuals/emission_distribution.png`"
        )
        lines.append(
            f"- Classification breakdown: `outputs/visuals/classification_breakdown.png`"
        )
        lines.append("")

    return "\n".join(lines)
