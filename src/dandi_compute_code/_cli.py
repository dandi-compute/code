import pathlib

import click

from .aind_ephys_pipeline import (
    prepare_aind_ephys_job,
    submit_aind_ephys_job,
)


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
    "--content-id",
    help="The content ID for the data to be processed.",
    required=True,
    type=str,
)
@click.option(
    "--config-file-path",
    help="Path to the configuration file.",
    required=False,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--pipeline-file-path",
    help="Path to the pipeline file.",
    required=False,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--silent",
    help="Suppress output messages.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--submit",
    help="Automatically submit the job.",
    required=False,
    is_flag=True,
    default=False,
)
def _aind_prepare_command(
    content_id: str,
    config_file_path: pathlib.Path | None = None,
    pipeline_file_path: pathlib.Path | None = None,
    silent: bool = False,
    submit: bool = False,
) -> None:
    script_file_path = prepare_aind_ephys_job(
        content_id=content_id, config_file_path=config_file_path, pipeline_file_path=pipeline_file_path
    )

    if submit:
        submit_aind_ephys_job(script_file_path=script_file_path)
        return

    if silent:
        return

    text = "\nPreparation complete!"
    message = click.style(text=text, fg="green")
    click.echo(message=message)
    text = f"\n\nTo submit the job, run:\n\n\tdandicompute aind submit --script-file-path {script_file_path}\n\n"
    message = click.style(text=text, fg="yellow")
    click.echo(message=message)


# dandicompute aind submit [OPTIONS]
@_aind_group.command(name="submit")
@click.option(
    "--script-file-path",
    help="Path to the submission script file.",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
)
def _aind_submit_command(script_file_path: pathlib.Path) -> None:
    submit_aind_ephys_job(script_file_path=script_file_path)
