import pathlib

import rich_click

from .aind_ephys_pipeline import (
    prepare_aind_ephys_job,
    submit_aind_ephys_job,
)


# dandicompute
@rich_click.group(name="dandicompute")
def _dandicompute_group():
    pass


# dandicompute aind
@_dandicompute_group.group(name="aind")
def _aind_group() -> None:
    pass


# dandicompute aind prepare [OPTIONS]
@_aind_group.command(name="prepare")
@rich_click.option(
    "--content-id",
    help="The content ID for the data to be processed.",
    required=True,
    type=str,
)
@rich_click.option(
    "--config-file-path",
    help="Path to the configuration file.",
    required=False,
    type=rich_click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
)
@rich_click.option(
    "--pipeline-file-path",
    help="Path to the pipeline file.",
    required=False,
    type=rich_click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
)
def _aind_prepare_command(
    content_id: str,
    config_file_path: pathlib.Path | None = None,
    pipeline_file_path: pathlib.Path | None = None,
) -> None:
    script_file_path = prepare_aind_ephys_job(
        content_id=content_id, config_file_path=config_file_path, pipeline_file_path=pipeline_file_path
    )

    message = f"Preparation complete. Submission script created at: {script_file_path}"
    rich_click.echo(message=message)


# dandicompute aind submit [OPTIONS]
@_aind_group.command(name="submit")
@rich_click.option(
    "--script-file-path",
    help="Path to the submission script file.",
    required=True,
    type=rich_click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
)
def _aind_submit_command(script_file_path: pathlib.Path) -> None:
    submit_aind_ephys_job(script_file_path=script_file_path)
