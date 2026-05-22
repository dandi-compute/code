import json
import pathlib
from collections import Counter, defaultdict
from datetime import datetime, timezone


def _extract_error_lines(*, log_file: pathlib.Path) -> list[str]:
    """Return non-empty log lines containing 'error' (case-insensitive)."""
    if not log_file.is_file():
        return []

    return [
        line.strip()
        for line in log_file.read_text(errors="replace").splitlines()
        if line.strip() and "error" in line.lower()
    ]


def _iter_capsule_log_directories(*, dandiset_directory: pathlib.Path) -> list[pathlib.Path]:
    """Return sorted logs/ directories that belong to attempt capsules."""
    derivatives_root = dandiset_directory / "derivatives"
    if not derivatives_root.is_dir():
        return []

    return sorted(
        path
        for path in derivatives_root.rglob("logs")
        if path.is_dir() and "_attempt-" in path.parent.name
    )


def dump_issues(
    *,
    dandiset_directory: pathlib.Path,
    queue_directory: pathlib.Path,
    output_file_name: str = "issues_dump.json",
) -> list[dict]:
    """Scan nextflow/slurm logs and write per-capsule error lines under *queue_directory*."""
    records: list[dict] = []
    for logs_dir in _iter_capsule_log_directories(dandiset_directory=dandiset_directory):
        nextflow_log = logs_dir / "nextflow.log"
        slurm_logs = sorted(path for path in logs_dir.glob("*slurm.log") if path.is_file())

        nextflow_errors = _extract_error_lines(log_file=nextflow_log)
        slurm_errors = {log_file.name: _extract_error_lines(log_file=log_file) for log_file in slurm_logs}
        slurm_errors = {key: value for key, value in slurm_errors.items() if value}
        if not nextflow_errors and not slurm_errors:
            continue

        records.append(
            {
                "capsule_path": logs_dir.parent.relative_to(dandiset_directory).as_posix(),
                "nextflow_log": nextflow_log.relative_to(dandiset_directory).as_posix() if nextflow_log.is_file() else None,
                "nextflow_errors": nextflow_errors,
                "slurm_errors": {
                    log_name: errors
                    for log_name, errors in sorted(slurm_errors.items(), key=lambda item: item[0])
                },
            }
        )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "capsule_count": len(records),
        "records": records,
    }
    (queue_directory / output_file_name).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return records


def summarize_issues(
    *,
    dandiset_directory: pathlib.Path,
    queue_directory: pathlib.Path,
    dump_output_file_name: str = "issues_dump.json",
    output_file_name: str = "issues_summary.json",
) -> dict[str, list[str]]:
    """Write descending error-frequency summary where keys are counts and values are error strings."""
    records = dump_issues(
        dandiset_directory=dandiset_directory,
        queue_directory=queue_directory,
        output_file_name=dump_output_file_name,
    )

    counts: Counter[str] = Counter()
    for record in records:
        counts.update(record.get("nextflow_errors", []))
        for errors in record.get("slurm_errors", {}).values():
            counts.update(errors)

    grouped: dict[str, list[str]] = defaultdict(list)
    for message, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        grouped[str(count)].append(message)

    summary = {count: messages for count, messages in sorted(grouped.items(), key=lambda item: int(item[0]), reverse=True)}
    output_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
    }
    (queue_directory / output_file_name).write_text(json.dumps(output_payload, indent=2, sort_keys=True) + "\n")
    return summary
