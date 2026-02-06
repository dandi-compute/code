import pathlib

import click

from ._utils import _styled_echo
from .aind_ephys_pipeline import prepare_aind_ephys_job, submit_aind_ephys_job


# dandicompute
@click.group(name="dandicompute")
def _dandicompute_group():
    pass


# dandicompute aind
@_dandicompute_group.group(name="aind")
def _aind_group() -> None:
    pass


# dandicompute aind prepare [OPTIONS]
@_aind_group.command(name="prepare")
@click.option(
    "--id",
    "content_id",
    help="The content ID for the data to be processed.",
    required=True,
    type=str,
)
@click.option(
    "--config",
    "config_file_path",
    help="Path to the configuration file.",
    required=False,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--pipeline",
    "pipeline_file_path",
    help="Path to the pipeline file.",
    required=False,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--preprocessing",
    "preprocessing_args",
    help="Command-line arguments for preprocessing.",
    required=False,
    type=str,
    default="",
)
@click.option(
    "--submit",
    help="Automatically submit the job.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress output messages.",
    required=False,
    is_flag=True,
    default=False,
)
def _aind_prepare_command(
    content_id: str,
    config_file_path: pathlib.Path | None = None,
    pipeline_file_path: pathlib.Path | None = None,
    preprocessing_args: str = "",
    submit: bool = False,
    silent: bool = False,
) -> None:
    script_file_path = prepare_aind_ephys_job(
        content_id=content_id,
        config_file_path=config_file_path,
        pipeline_file_path=pipeline_file_path,
        preprocessing_args=preprocessing_args,
        silent=silent,
    )

    if submit:
        submit_aind_ephys_job(script_file_path=script_file_path)

    if silent:
        return

    _styled_echo(text="\nPreparation complete!", color="green")

    if submit and not silent:
        _styled_echo(text=f"\n\nProcessing script at: {script_file_path}\n\n", color="yellow")
        return

    _styled_echo(
        text=f"\n\nTo submit the job, run:\n\n\tdandicompute aind submit --script {script_file_path}\n\n",
        color="yellow",
    )


# dandicompute aind submit [OPTIONS]
@_aind_group.command(name="submit")
@click.option(
    "--script",
    "script_file_path",
    help="Path to the submission script file.",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
)
def _aind_submit_command(script_file_path: pathlib.Path) -> None:
    submit_aind_ephys_job(script_file_path=script_file_path)
