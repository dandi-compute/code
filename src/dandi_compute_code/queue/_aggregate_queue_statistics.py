import json
import pathlib
from collections import defaultdict
from datetime import datetime, timezone

from ._duration_string_to_seconds import _duration_string_to_seconds
from ._extract_nextflow_timeline_data import _extract_nextflow_timeline_data
from ._resolve_attempt_dir import _resolve_attempt_dir


def aggregate_queue_statistics(
    *,
    queue_directory: pathlib.Path,
    dandiset_directory: pathlib.Path,
    output_file_name: str = "queue_stats.json",
) -> dict:
    """Write aggregate queue statistics JSON and return the written payload."""
    state_file = queue_directory / "state.jsonl"
    state_entries = (
        [json.loads(line.strip()) for line in state_file.read_text().splitlines() if line.strip()]
        if state_file.exists()
        else []
    )

    successful_asset_bytes_total = sum(
        entry["asset_size_bytes"]
        for entry in state_entries
        if entry.get("has_output")
        and isinstance(entry.get("asset_size_bytes"), int)
        and not isinstance(entry.get("asset_size_bytes"), bool)
    )

    job_step_wall_time_seconds: defaultdict[str, float] = defaultdict(float)
    timeline_files_processed = 0
    for entry in state_entries:
        attempt_dir = _resolve_attempt_dir(base_dir=dandiset_directory, entry=entry)
        timeline_file = attempt_dir / "logs" / "timeline.html"
        if not timeline_file.is_file():
            continue

        timeline_data = _extract_nextflow_timeline_data(timeline_html=timeline_file.read_text())
        if timeline_data is None:
            continue

        processes = timeline_data.get("processes")
        if not isinstance(processes, list):
            continue
        timeline_files_processed += 1

        for process in processes:
            if not isinstance(process, dict):
                continue
            process_label = process.get("label")
            if not isinstance(process_label, str):
                continue
            step_name = process_label.split(" (", 1)[0]
            times = process.get("times")
            if not isinstance(times, list):
                continue
            for step in times:
                if not isinstance(step, dict):
                    continue
                duration_label = step.get("label")
                if not isinstance(duration_label, str):
                    continue
                duration_string = duration_label.split("/", 1)[0].strip()
                duration_seconds = _duration_string_to_seconds(duration_string)
                if duration_seconds > 0:
                    job_step_wall_time_seconds[step_name] += duration_seconds

    statistics = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "state_entry_count": len(state_entries),
        "successful_asset_bytes_total": successful_asset_bytes_total,
        "timeline_files_processed": timeline_files_processed,
        "job_step_wall_time_seconds": {
            key: value for key, value in sorted(job_step_wall_time_seconds.items(), key=lambda item: item[0])
        },
    }

    output_file = queue_directory / output_file_name
    output_file.write_text(json.dumps(statistics, indent=2, sort_keys=True) + "\n")
    return statistics
