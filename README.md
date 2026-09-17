# DANDI Compute: Orchestration code

Contains essential code for orchestrating computation submission and queue management for processing pipelines acting on DANDI assets.



## Job capsules

Each run of a pipeline over one asset lives in its own job capsule directory:

```
derivatives/dandisets-{first 3 digits}/dandiset-{dandiset_id}/{dandi path}/pipeline-{pipeline}/job-{YYMMDD}{hash}
```

The job ID is the whole name. `YYMMDD` is the date the capsule was prepared, which keeps the name readable and separates re-attempts of the same job across days. The hash is the first six characters of the MD5 checksum of the fields that identify the job, so capsules for different parameters, configs, versions or assets stay apart.

The codebase version is deliberately left out of the hash. A job is the same logical job no matter which release of this package formed it, which is how the queue decides that a capsule already exists and must not be formed a second time.

Everything the name used to spell out is recorded in two places instead:

- the `DandiCompute` provenance block in the capsule's `dataset_description.json`
- the `derivatives/state.tsv` summary table, which reads that provenance back

The job ID is the only capsule layout this package understands. Capsules prepared before it existed carry older names and are invisible to the queue until they are migrated.

### Migrating legacy capsules

`scripts/migrate_job_capsule_names.py` migrates the capsules already on the archive. The archive has no notion of renaming a path, so a migration is a copy under the new name, an upload of the copies, and a delete of the originals once you have confirmed the result. It works against local clones of the Dandisets sitting next to each other.

Download the clones, then run the four phases in order from the directory holding them:

```bash
dandi download DANDI:001697
dandi download DANDI:001873

python migrate_job_capsule_names.py plan     # what would be copied; changes nothing
python migrate_job_capsule_names.py copy     # copy on disk, write a manifest
python migrate_job_capsule_names.py upload   # batch-upload the new paths
python migrate_job_capsule_names.py clean    # delete the legacy paths
```

`copy` is purely local and leaves the legacy directory exactly as it was, so the clone stays a complete mirror of the archive and a bad plan costs only a re-download. It records every copy in `job-capsule-migration.json` next to the clones, which `upload` and `clean` read back, so you can inspect or edit it before anything reaches the archive. `upload` pushes only the new paths and deletes nothing, so the archive carries both names. Check the new capsules, then `clean` deletes the legacy paths from the archive and removes them locally.

Because the capsules exist twice on disk between `copy` and `clean`, the clone needs room for a second copy of every capsule being migrated.

Use `--root` when the clones are somewhere other than the working directory, and `--dandiset` to migrate one Dandiset at a time.

The script is standalone. It imports nothing from this package, so it can be copied anywhere and run against whatever version of `dandi-compute-code` is installed, or none at all. It needs only the standard library, plus the `dandi` client on PATH for `upload` and `clean`.

A migrated capsule keeps the date it was originally prepared, taken from the modification time of its `code/submit.sh`, and its content ID is read from that same script, so planning is entirely offline. Its hash is what preparation computes for the same job, so a migrated job is never formed a second time.

Two legacy capsules that describe the same logical job and differ only in codebase version map to the same job ID, since the hash ignores the codebase version. Copying both onto one directory would merge them, so they are reported and skipped. Archive or delete all but one, then re-run.

`copy` can be run repeatedly. It adds to the manifest rather than replacing it, and a capsule whose copy an earlier run already made is recorded again rather than skipped, so a lost or truncated manifest is rebuilt by re-running the phase.

The migration stands alone. It shells out to `dandi` for the two archive-facing phases and otherwise reads only the clones, never invoking `dandicompute` and never reading or writing a `state.tsv`.

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
dandicompute archive --job derivatives/dandisets-000/dandiset-000409/sub-mouse01/pipeline-aind+ephys/job-260916a1b2c3
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

Non-code files for the LFP pipeline are organized under the following subdirectories of `src/dandi_compute_code/lfp_pipeline/`:

- **`params/`** — JSON parameter files (e.g., `name-default.json`) plus `parameter_schema.json`, the JSON Schema that defines and constrains the exposed LFP parameters.
  To add a new parameters file:
  1. Add the `name-[id].json` file to this directory.
  2. Register it in `registries/registered_params.json` by adding an entry with the short name as the key, and its relative `path` and full MD5 `md5` as values.
  The short name can then be passed via the `parameters_key` argument of `load_lfp_parameters`.

- **`registries/`** — JSON registry files mapping short names to resource paths and checksums (e.g., `registered_params.json`).

The LFP pipeline depends on heavy scientific packages (SpikeInterface, neuroconv, pynwb). These are deliberately kept out of the base install and are declared only in `src/dandi_compute_code/lfp_pipeline/envs/pyproject.toml`. To run the LFP pipeline, use the runtime container built from `src/dandi_compute_code/lfp_pipeline/containers/lfp.Dockerfile`, or reproduce it locally with `pip install . ./src/dandi_compute_code/lfp_pipeline/envs`. The container image is built and pushed to the GitHub Container Registry by the manually dispatched `Build and upload LFP container image` workflow.
