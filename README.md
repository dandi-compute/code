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

- **`params/`** — JSON parameter files passed to the pipeline (e.g., `default_parameters.json`, `no_motion_parameters.json`).
  Add a new `[id]_parameters.json` file here and expose it through the `parameters_key` argument in `_prepare_job.py`.

- **`configs/`** — Nextflow configuration files for a specific compute environment (e.g., `mit_engaging.config`).
  Add a new `[environment].config` file here. Users can pass the path explicitly via the `--config` CLI option or the `config_file_path` argument in `_prepare_job.py`.
