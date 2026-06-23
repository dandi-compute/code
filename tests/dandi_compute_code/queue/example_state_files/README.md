# Example queue state files

Each `.jsonl` file in this directory is a literal, committed example of a queue
`state.jsonl` file. They are the shared ground truth for the queue test suite.
Reading them here on GitHub shows exactly what input a test starts from, so the
relationship between input state and expected behavior stays visible without
having to reconstruct dictionaries inside the tests.

Every line is one job capsule, matching the format produced by
`QueueState.to_file` and consumed by `QueueState.from_jsonl`. Tests load these
files through the fixtures in `../conftest.py` (`example_state_files_directory`
and `install_state_file`) rather than building entries inline.

The attempt lifecycle is encoded by the presence flags:

- pending: `has_code` only
- running: `has_logs` and not `has_output`
- successful: `has_output`
- failed: `has_code` and `has_logs` and not `has_output`

| File | Contents |
| --- | --- |
| `empty.jsonl` | No entries (nothing has been prepared yet). |
| `single_pending.jsonl` | One prepared-but-unsubmitted attempt. |
| `pending_and_running.jsonl` | One pending attempt plus one running attempt. |
| `aggregate_two_attempts.jsonl` | One successful and one running attempt, each with a known source-asset size. |
| `aggregate_single_successful.jsonl` | One successful attempt with a known source-asset size. |
| `aggregate_fallback_sourcedata.jsonl` | A successful attempt whose `dandi_path` differs from the on-disk attempt layout. |
| `clean_single_queued.jsonl` | One queued (unsubmitted) `aind+ephys` attempt. |
| `clean_single_with_output.jsonl` | The same attempt, already completed. |
| `clean_single_with_logs.jsonl` | The same attempt, already run (logs present). |
| `clean_single_session.jsonl` | A queued attempt under a session subdirectory. |
| `clean_session_versioned.jsonl` | A queued attempt with a commit-suffixed pipeline version under a session. |
| `clean_two_attempts.jsonl` | Attempt 1 queued, attempt 2 completed, for one asset. |
| `clean_two_dandisets_queued.jsonl` | A queued attempt in each of two Dandisets. |
| `clean_fallback_sourcedata.jsonl` | A queued attempt whose `dandi_path` differs from the on-disk attempt layout. |
| `prepare_failures_reaching_max.jsonl` | Failure history reaching `max_fail_per_dandiset` for one Dandiset. |
