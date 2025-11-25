# Evidence Pipeline Skeleton

## Overview
This repository implements a lightweight, modular pipeline for processing UK open data into decision-ready evidence. It ingests official datasets, harmonises them into a clean analytical schema, validates structural and data-quality issues, computes basic indicators, and generates a Markdown evidence report with summary statistics and visualisations.

## Structure
The project is organised into modular components for ingestion, harmonisation, validation, indicators, reporting, visualization, and diagnostic agents. The data directory stores raw downloads and processed outputs locally (ignored in Git), while the outputs directory contains the generated report and figures.

## How to Run
How to Run

To run the full pipeline:

from src.pipeline import run
run()

This performs ingestion → cleaning → validation → indicators → report → visuals.

To run individual components:

Emissions summary pipeline:
from src.pipeline_emissions_summary import run_emissions_summary_pipeline
run_emissions_summary_pipeline()

Population pipeline:
from src.pipeline_population import run_population
run_population()

Indicator pipeline:
from src.pipeline_indicators import run_indicators
run_indicators()


