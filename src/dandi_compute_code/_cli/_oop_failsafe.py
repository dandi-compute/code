"""
Soft-rollout failsafe for the new ``QueueState`` OOP model.

Each queue-related CLI command first attempts the new OOP code path. If that
path raises for any reason, the failure is recorded to a failsafe log file (so
it can be investigated and fixed later) and the command falls back to the
current free-function runtime behavior. This keeps the rollout safe: a defect in
the new model never regresses a user-facing command, but every divergence is
captured for follow-up.
"""

import datetime
import json
import logging
import os
import pathlib
import traceback
from collections.abc import Callable
from typing import TypeVar

_log = logging.getLogger(__name__)

_ResultT = TypeVar("_ResultT")

#: Environment variable that overrides the failsafe log location.
_OOP_FAILSAFE_LOG_ENV_VAR = "DANDICOMPUTE_OOP_FAILSAFE_LOG"

#: Default failsafe log file used when the override variable is unset.
_DEFAULT_OOP_FAILSAFE_LOG = pathlib.Path.home() / ".dandicompute" / "oop_failsafe.jsonl"


def _resolve_oop_failsafe_log_path() -> pathlib.Path:
    """Resolve the failsafe log path, honoring the override environment variable."""
    override = os.environ.get(_OOP_FAILSAFE_LOG_ENV_VAR, "").strip()
    log_path = pathlib.Path(override) if override else _DEFAULT_OOP_FAILSAFE_LOG
    return log_path


def _record_oop_failure(*, command: str, error: BaseException) -> None:
    """Append a structured record of an OOP-path failure to the failsafe log file."""
    formatted_traceback = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    record = {
        "logged_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "command": command,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "traceback": formatted_traceback,
    }

    log_path = _resolve_oop_failsafe_log_path()
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open(mode="a") as log_stream:
            log_stream.write(json.dumps(record) + "\n")
    except OSError as log_error:
        _log.warning("Could not write OOP failsafe log to %s: %s", log_path, log_error)


def run_with_oop_failsafe(
    *,
    command: str,
    oop_path: Callable[[], _ResultT],
    fallback_path: Callable[[], _ResultT],
) -> _ResultT:
    """
    Attempt the new OOP ``QueueState`` code path, falling back to current behavior.

    The *oop_path* callable is attempted first. If it raises any exception, the
    failure is recorded to the failsafe log file for later investigation and
    *fallback_path* is invoked to preserve the current runtime behavior. The
    result of whichever path completes is returned.

    Parameters
    ----------
    command : str
        Human-readable command label recorded with any failure (e.g. ``"queue
        process"``).
    oop_path : callable
        Zero-argument callable invoking the new ``QueueState`` model.
    fallback_path : callable
        Zero-argument callable invoking the current free-function behavior.
    """
    try:
        oop_result = oop_path()
        return oop_result
    except Exception as error:
        _record_oop_failure(command=command, error=error)
        _log.warning("OOP path for '%s' failed; falling back to current behavior (%s).", command, error)
        fallback_result = fallback_path()
        return fallback_result
