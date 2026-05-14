# DANDI Compute: Orchestration code

Contains essential code for orchestrating computation submission and queue management for processing pipelines acting on DANDI assets.



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
dandicompute aind prepare --id 048d1ee9-83b7-491f-8f02-1ca615b1d455 --submit
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
  Add a new `[environment].config` file here. Users can pass the path explicitly via the `--config` CLI option or the `config_file_path` argument in `_prepare_job.py`.
