"""Passive diagnostic agent for analyzing validation output."""

import json
from pathlib import Path
from src.metadata.metadata_store import MetadataStore


METADATA_PATH = "data/processed/metadata.json"


class DiagnosticAgent:
    """
    Reads the most recent validation event from metadata.json,
    analyzes the issues, and produces a structured diagnostic report.
    """

    def __init__(self, metadata_path=METADATA_PATH):
        self.metadata_path = metadata_path
        self.store = MetadataStore()
        self.metadata = self._load_metadata()

    def _load_metadata(self):
        if not Path(self.metadata_path).exists():
            raise FileNotFoundError("metadata.json not found. Run pipeline first.")
        with open(self.metadata_path, "r") as f:
            return json.load(f)

    def _get_latest_validation(self):
        """
        Find the latest event where stage='validation'.
        """
        validation_events = [
            e for e in self.metadata if e.get("stage") == "validation"
        ]
        if not validation_events:
            raise ValueError("No validation events found in metadata.")
        return validation_events[-1]  # latest

    def analyze(self):
        """
        Perform diagnostic analysis using the latest validation issues.
        """
        validation_event = self._get_latest_validation()
        issues = validation_event["details"]["issues"]

        report = {
            "summary": {
                "total_missing": sum(issues["missing_values"].values()),
                "invalid_la_codes": issues["invalid_la_code_count"],
                "out_of_range_years": issues["out_of_range_years"],
            },
            "missing_values": issues["missing_values"],
            "invalid_la_codes": issues["invalid_la_code_count"],
            "numeric_nulls": issues["numeric_null_counts"],
            "recommended_actions": self._recommend_actions(issues),
        }

        # Log agent action
        self.store.add_event(
            stage="agent",
            action="diagnostic_report",
            details={
                "issues_detected": report["summary"],
                "recommended_actions": report["recommended_actions"],
            },
        )

        return report

    def _recommend_actions(self, issues):
        """
        Basic rule-based recommendations based on validation issues.
        """
        actions = []

        if issues["invalid_la_code_count"] > 0:
            actions.append(
                "Check rows with invalid LA codes. These often correspond to sector-level entries rather than LA summaries."
            )

        if sum(issues["missing_values"].values()) > 0:
            actions.append(
                "Investigate missing values in population and area fields — often structural for non-LA geo entries."
            )

        if issues["out_of_range_years"] > 0:
            actions.append(
                "Remove or correct rows with year values outside 2005–2022."
            )

        if not actions:
            actions.append("No major issues detected. Data appears structurally sound.")

        return actions


def run():
    """Entry point for this module."""
    agent = DiagnosticAgent()
    return agent.analyze()
