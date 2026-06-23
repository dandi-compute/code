# Example queue state

`state.jsonl` is a single literal, committed example of a queue `state.jsonl`
file. It is the shared ground truth for the queue test suite. Reading it here on
GitHub shows exactly what state the tests start from, so the relationship between
input state and expected behavior stays visible without reconstructing
dictionaries inside the tests.

Each line is one job capsule, matching the format produced by
`QueueState.to_file` and consumed by `QueueState.from_jsonl`. Tests load the file
through the fixtures in `../conftest.py` and select the entry they need by its
`dandi_path`, which is named to describe the scenario it covers. The empty-queue
case is written inline by the few tests that need it rather than kept as a file.

The attempt lifecycle is encoded by the presence flags:

- pending: `has_code` only
- running: `has_logs` and not `has_output`
- successful: `has_output`
- failed: `has_code` and `has_logs` and not `has_output`

| `dandi_path` | Scenario |
| --- | --- |
| `sub-pending` | Prepared but never submitted. |
| `sub-running` | Logs present, no output yet. |
| `sub-successful` | Output present, with a known source-asset size (120 bytes). |
| `sub-failed-repeated` (attempts 1 and 2) | Two failed attempts of one asset, reaching `max_fail_per_dandiset` for Dandiset 000001 (mapped to `asset-aaa`). |
| `sub-fresh` (Dandiset 000002) | A queued asset in another Dandiset with no failures (mapped to `asset-bbb`). |
| `sub-with-session/ses-recording` | A queued attempt nested under a session directory. |
| `sub-sole-attempt/ses-recording` (Dandiset 001371) | The sole attempt in its pipeline/version tree, so empty parents are pruned on removal. Also covers the legacy nested layout. |
| `sub-two-attempts` (attempts 1 and 2) | Attempt 1 queued, attempt 2 completed, so the shared parent is kept after the queued attempt is removed. |
| `sub-already-submitted` | Queued in state, but a submitted marker exists on disk, so it is left alone. |
| `sourcedata` (Dandiset 001849) | A queued attempt whose recorded `dandi_path` differs from the on-disk attempt layout, exercising fallback attempt-directory resolution. |
