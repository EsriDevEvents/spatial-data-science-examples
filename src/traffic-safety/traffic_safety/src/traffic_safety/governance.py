"""Human-in-the-loop governance and local inference for the Trusted GeoAI session.

This module implements the "Building Trusted GeoAI with ArcGIS, Edge, and
Local Inference" session: it wraps a recommendation produced by
`traffic_safety.recommendations` with a human review/approval step and a
durable, append-only audit trail, and provides a local-inference
`LLMClient` (`OllamaLLMClient`) that can be swapped in for a hosted model
without changing any downstream code - demonstrating sovereign, offline
execution with the same recommendation pipeline used in the previous
session.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum


class ReviewDecision(str, Enum):
    """The outcome of a human review of an AI-generated recommendation."""

    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_REVIEW = "needs_review"


@dataclass
class AuditRecord:
    """A single, immutable entry in the human-in-the-loop audit trail.

    Every field a reviewer needs to validate a recommendation - the
    evidence it was generated from, the reasoning text, and the reviewer's
    decision - is captured together, so the audit trail can answer "why was
    this approved?" without cross-referencing other systems.

    Args:
        recommendation (dict): The recommendation dict produced by
            `traffic_safety.recommendations.generate_recommendation`.
        decision (ReviewDecision): The reviewer's decision.
        reviewer (str): The name or id of the human reviewer.
        comment (str, optional): Optional free-text justification. Defaults
            to "".
        timestamp (str, optional): An ISO-8601 UTC timestamp. Defaults to
            the current time.
    """

    recommendation: dict
    decision: ReviewDecision
    reviewer: str
    comment: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        """Converts this record to a JSON-serializable dict.

        Returns:
            dict: `asdict(self)` with `decision` coerced to its plain
            string value for JSON serialization.
        """
        record = asdict(self)
        record["decision"] = ReviewDecision(self.decision).value
        return record


def append_audit_record(record: AuditRecord, audit_log_path: str) -> None:
    """Appends an audit record to a JSON Lines audit trail file.

    Using an append-only JSONL file (rather than rewriting a JSON array)
    keeps every prior entry untouched, which is important for an audit
    trail that must remain trustworthy even if the process is interrupted
    mid-write.

    Args:
        record (AuditRecord): The record to persist.
        audit_log_path (str): The path to the audit trail file. Parent
            directories are created if they do not already exist.
    """
    directory = os.path.dirname(audit_log_path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    with open(audit_log_path, "a", encoding="utf-8") as audit_file:
        audit_file.write(json.dumps(record.to_dict()) + "\n")


def load_audit_trail(audit_log_path: str) -> list[dict]:
    """Loads every record from a JSON Lines audit trail file.

    Args:
        audit_log_path (str): The path to the audit trail file.

    Returns:
        list[dict]: Every audit record, in the order it was written. Empty
        if the file does not exist yet.
    """
    if not os.path.exists(audit_log_path):
        return []

    with open(audit_log_path, "r", encoding="utf-8") as audit_file:
        return [json.loads(line) for line in audit_file if line.strip()]


def request_human_review(recommendation: dict, decision: ReviewDecision, reviewer: str, comment: str = "", audit_log_path: str = "audit_trail.jsonl") -> AuditRecord:
    """Records a human reviewer's decision on an AI-generated recommendation.

    The decision itself is supplied by the caller rather than collected
    interactively (e.g. via `input()`) here, so this function stays
    deterministic and unit-testable; `notebooks/TrustedGeoAI.ipynb`
    demonstrates collecting the decision interactively and passing it in.

    Args:
        recommendation (dict): The recommendation being reviewed (see
            `traffic_safety.recommendations.generate_recommendation`).
        decision (ReviewDecision): The reviewer's decision.
        reviewer (str): The name or id of the human reviewer.
        comment (str, optional): Optional free-text justification. Defaults
            to "".
        audit_log_path (str, optional): The path to append the resulting
            audit record to. Defaults to "audit_trail.jsonl".

    Returns:
        AuditRecord: The persisted audit record.
    """
    record = AuditRecord(recommendation=recommendation, decision=decision, reviewer=reviewer, comment=comment)
    append_audit_record(record, audit_log_path)
    return record


@dataclass
class OllamaLLMClient:
    """An `LLMClient` (see `traffic_safety.recommendations`) backed by a local Ollama model.

    Implements the same `generate(prompt: str) -> str` interface as
    `traffic_safety.recommendations.TemplateLLMClient`, so a recommendation
    pipeline built against a hosted model can be pointed at a local,
    sovereign model by constructing this client instead - no other code
    changes required. This is the concrete mechanism behind the "Edge and
    Local Inference" part of the session.

    Args:
        model (str, optional): The local Ollama model tag to use. Defaults
            to "llama3.2".
        host (str, optional): The Ollama server host. Defaults to None
            (use the `ollama` package's default, typically
            http://localhost:11434).
    """

    model: str = "llama3.2"
    host: str = None

    def generate(self, prompt: str) -> str:
        """Generates text locally via Ollama, without any external network call.

        Args:
            prompt (str): The prompt to send to the local model.

        Returns:
            str: The model's response text.

        Raises:
            ImportError: If the optional `ollama` package is not installed.
        """
        try:
            import ollama
        except ImportError as error:
            raise ImportError(
                "The 'ollama' package is required for local inference. Install it with `uv add ollama` "
                "and ensure an Ollama server is running locally."
            ) from error

        client = ollama.Client(host=self.host) if self.host else ollama
        response = client.chat(model=self.model, messages=[{"role": "user", "content": prompt}])
        return response["message"]["content"]
