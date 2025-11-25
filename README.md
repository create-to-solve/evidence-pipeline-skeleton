# Evidence Pipeline Skeleton

## Overview
This repository implements a lightweight, modular pipeline for processing UK open data into decision-ready evidence. It ingests official datasets, harmonises them into a clean analytical schema, validates structural and data-quality issues, computes basic indicators, and generates a Markdown evidence report with summary statistics and visualisations.

## Structure
The project is organised into modular components for ingestion, harmonisation, validation, indicators, reporting, visualization, and diagnostic agents. The data directory stores raw downloads and processed outputs locally (ignored in Git), while the outputs directory contains the generated report and figures.

## How to Run
The full pipeline can be executed through the main run() function in src/pipeline, which performs ingestion, cleaning, validation, indicator calculation, and report generation. Individual sub-pipelines (emissions summary, population, indicators) can also be run independently. Dependencies can be installed from requirements.txt.


