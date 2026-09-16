# DANDI Compute: Orchestration code

Contains essential code for orchestrating computation submission and queue management for processing pipelines acting on DANDI assets.



## Job capsules

Each run of a pipeline over one asset lives in its own job capsule directory:

```
derivatives/dandisets-{first 3 digits}/dandiset-{dandiset_id}/{dandi path}/pipeline-{pipeline}/job-{YYMMDD}+{hash}
```

The job ID is the whole name. `YYMMDD` is the date the capsule was prepared, which keeps the name readable and separates re-attempts of the same job across days. The hash is the first six characters of the MD5 checksum of the fields that identify the job, so capsules for different parameters, configs, versions or assets stay apart.

The codebase version is deliberately left out of the hash. A job is the same logical job no matter which release of this package formed it, which is how the queue decides that a capsule already exists and must not be formed a second time.

Everything the name used to spell out is recorded in two places instead:

- the `DandiCompute` provenance block in the capsule's `dataset_description.json`
- the `derivatives/state.tsv` summary table, which reads that provenance back

Capsules prepared before the job ID existed still carry their old `version-..._codebase-..._params-..._config-...` names. Those are read from the name directly and remain fully supported.


## Manual dispatch commands on MIT Engaging

To run manually with confirmation to trigger (for debugging):

```bash
dandicompute aind prepare --id [full content ID]
```

To run automatically:

```bash
dandicompute aind prepare --id [full content ID] --submit
```

To test automatically on the example asset:

```bash
dandicompute prepare aind --test
```

To clean unsubmitted job capsules:

```bash
dandicompute queue clean --dandiset ./dandi/001697/
```

To archive a failed job capsule by moving it from `001697` to the permanent archive `001873`:

```bash
dandicompute archive --job derivatives/dandisets-000/dandiset-000409/sub-mouse01/pipeline-aind+ephys/job-260916+a1b2c3
```


To check whether there is any queued work before dispatching, use `queue pending`. It exits with code 0 when at least one job is awaiting submission, and code 1 when there is nothing to process. This lets a crontab skip the dispatch entirely when the queue is empty:

```bash
dandicompute queue pending --silent && dandicompute queue process --processing ./processing/
```



## Contributing Non-Code Files

Non-code files for the AIND ephys pipeline are organized under the following subdirectories of `src/dandi_compute_code/aind_ephys_pipeline/`:

- **`templates/`** — Jinja2 submission script templates (e.g., `submission_template.txt`).
  Add a new `.txt` template here and reference it via `_globals.py` or a new globals module.

- **`params/`** — JSON parameter files passed to the pipeline (e.g., `default.json`, `no_motion.json`).
  To add a new parameters file:
  1. Add the `[id].json` file to this directory.
  2. Register it in `registries/registered_params.json` by adding an entry with the short name as the key, and its relative `path` and full MD5 `md5` as values, e.g.:
     ```json
     "my+params": {
       "path": "my_params.json",
       "md5": "<md5 hash of the file>"
     }
     ```
  The short name can then be passed via the `parameters_key` argument in `_prepare_job.py` or via `--params` on the CLI.

- **`registries/`** — JSON registry files mapping short names to resource paths and checksums (e.g., `registered_params.json`).

- **`configs/`** — Nextflow configuration files for a specific compute environment (e.g., `mit_engaging.config`).
  To add a new config file:
  1. Add the `[environment].config` file to this directory.
  2. Register it in `registries/registered_configs.json` by adding an entry with the short name as the key, and its relative `path` and full MD5 `md5` as values.
  Use `--config` / `config_key` to select a registered config (default: `default`).
