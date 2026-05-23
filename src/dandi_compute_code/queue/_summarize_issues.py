import pathlib
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json

from ._dump_issues import dump_issues


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

    errors_by_count: dict[str, list[str]] = defaultdict(list)
    for message, count in sorted(counts.items(), key=lambda message_count: (-message_count[1], message_count[0])):
        errors_by_count[str(count)].append(message)

    summary = {
        count: messages
        for count, messages in sorted(
            errors_by_count.items(),
            key=lambda count_messages: int(count_messages[0]),
            reverse=True,
        )
    }
    output_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
    }
    (queue_directory / output_file_name).write_text(json.dumps(output_payload, indent=2, sort_keys=True) + "\n")
    return summary
