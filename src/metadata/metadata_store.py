"""Manages metadata storage and retrieval for pipeline artifacts."""

import json
import os
from datetime import datetime


class MetadataStore:
    """
    A simple metadata store that records pipeline events in a JSON file.
    Each event is appended as a dictionary with keys:
    - stage
    - action
    - timestamp
    - details
    """

    def __init__(self, path="data/processed/metadata.json"):
        self.path = path
        self.events = []

        # Load existing metadata if present
        if os.path.exists(self.path):
            self.load()
        else:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self.save()  # create empty file

    def timestamp(self):
        """Return current UTC timestamp as ISO8601 string."""
        return datetime.utcnow().isoformat() + "Z"

    def add_event(self, stage, action, details=None):
        """Append a metadata event."""
        event = {
            "stage": stage,
            "action": action,
            "timestamp": self.timestamp(),
            "details": details or {},
        }
        self.events.append(event)
        self.save()

    def _convert(self, obj):
        """Recursively convert non-JSON-serializable types (e.g., numpy types)
        into standard Python types."""
        if isinstance(obj, dict):
            return {self._convert(k): self._convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert(i) for i in obj]
        elif hasattr(obj, "item"):  # numpy scalar
            return obj.item()
        else:
            return obj

    def save(self):
        """Write metadata events to disk with type normalization."""
        serializable_events = self._convert(self.events)
        with open(self.path, "w") as f:
            json.dump(serializable_events, f, indent=2)


    def load(self):
        """Load metadata events from disk."""
        with open(self.path, "r") as f:
            try:
                self.events = json.load(f)
            except json.JSONDecodeError:
                self.events = []

    def to_dict(self):
        """Return metadata as a dictionary."""
        return {"events": self.events}

    def reset(self):
        """Erase all events (mostly for testing)."""
        self.events = []
        self.save()


def run():
    """Placeholder run function."""
    pass
