import json
import pathlib
from datetime import datetime, timezone

from ._extract_error_lines import _extract_error_lines
from ._list_capsule_log_directories import _list_capsule_log_directories


def dump_issues(
    *,
    dandiset_directory: pathlib.Path,
    queue_directory: pathlib.Path,
    output_file_name: str = "issues_dump.json",
) -> list[dict]:
    """Scan nextflow/slurm logs and write per-capsule error lines under *queue_directory*."""
    records: list[dict] = []
    for logs_dir in _list_capsule_log_directories(dandiset_directory=dandiset_directory):
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
                "nextflow_log": (
                    nextflow_log.relative_to(dandiset_directory).as_posix() if nextflow_log.is_file() else None
                ),
                "nextflow_errors": nextflow_errors,
                "slurm_errors": {
                    log_name: errors for log_name, errors in sorted(slurm_errors.items(), key=lambda item: item[0])
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
