import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from src.metadata.metadata_store import MetadataStore


def _ensure_output_dir(output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)


def plot_missingness(clean_data_path: str, output_dir: str) -> Path:
    df = pd.read_csv(clean_data_path)
    missing = df.isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(missing.index, missing.values)
    ax.set_title("Missing Values by Column")
    ax.set_xlabel("Count of Missing Values")
    ax.set_ylabel("Column")
    plt.tight_layout()

    path = Path(output_dir) / "missing_values.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_emission_distribution(clean_data_path: str, output_dir: str) -> Path:
    df = pd.read_csv(clean_data_path)
    col = "territorial_emissions_kt_co2e"

    fig, ax = plt.subplots(figsize=(10, 6))
    df[col].hist(bins=50, ax=ax)
    ax.set_title("Distribution of Territorial Emissions (kt CO₂e)")
    ax.set_xlabel("Emissions (kt CO₂e)")
    ax.set_ylabel("Frequency")
    plt.tight_layout()

    path = Path(output_dir) / "emission_distribution.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_emission_trend(clean_data_path: str, output_dir: str) -> Path:
    df = pd.read_csv(clean_data_path)

    if "calendar_year" not in df.columns:
        raise ValueError("calendar_year column not found in clean dataset.")

    df_grouped = df.groupby("calendar_year")["territorial_emissions_kt_co2e"].sum()

    fig, ax = plt.subplots(figsize=(10, 6))
    df_grouped.plot(ax=ax)
    ax.set_title("Emissions Trend Over Time (Total kt CO₂e)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Total Emissions (kt CO₂e)")
    plt.tight_layout()

    path = Path(output_dir) / "emission_trend.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_classification_breakdown(classified_path: str, output_dir: str) -> Path:
    df = pd.read_csv(classified_path)

    if "record_type" not in df.columns:
        raise ValueError("record_type column not found in classified dataset.")

    counts = df["record_type"].value_counts()

    fig, ax = plt.subplots(figsize=(10, 6))
    counts.plot(kind="bar", ax=ax)
    ax.set_title("Record Type Breakdown")
    ax.set_xlabel("Record Type")
    ax.set_ylabel("Count")
    plt.tight_layout()

    path = Path(output_dir) / "classification_breakdown.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def generate_all_plots(clean_data_path: str,
                       classified_path: str,
                       output_dir: str = "outputs/visuals/") -> None:

    output_dir = Path(output_dir)
    _ensure_output_dir(output_dir)

    store = MetadataStore()

    generated = {
        "missingness_plot": str(plot_missingness(clean_data_path, output_dir)),
        "emission_distribution_plot": str(plot_emission_distribution(clean_data_path, output_dir)),
        "emission_trend_plot": str(plot_emission_trend(clean_data_path, output_dir)),
        "classification_breakdown_plot": str(plot_classification_breakdown(classified_path, output_dir)),
    }

    store.add_event(
        stage="visualisation",
        action="generate_plots",
        details={"plots": generated}
    )

    return generated
