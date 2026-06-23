"""
``JobInfo`` — the immutable identity of one attempt capsule.

Kept in its own module so both the OOP model (:mod:`._queue_state`) and its
private helpers (:mod:`._queue_utils`) can depend on it without coupling to
each other.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class JobInfo:
    """Immutable identity of one attempt capsule."""

    dandiset_id: str
    dandi_path: str
    pipeline: str
    version: str
    params: str
    config: str
    attempt: int
    codebase: str

    def to_dict(self) -> dict[str, object]:
        """Serialise the identity fields to a plain dict."""
        return {
            "dandiset_id": self.dandiset_id,
            "dandi_path": self.dandi_path,
            "pipeline": self.pipeline,
            "version": self.version,
            "params": self.params,
            "config": self.config,
            "attempt": self.attempt,
            "codebase": self.codebase,
        }
