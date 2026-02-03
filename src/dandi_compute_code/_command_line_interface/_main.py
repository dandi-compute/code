import rich_click

from .._aind_ephys_pipeline._handle_template import generate_aind_ephys_submission_script

# dandicompute
@rich_click.group(name="dandicompute")
def _dandicompute_cli():
    pass

# dandicompute submit [OPTIONS]
@_dandicompute_cli.command(name="convert")
@rich_click.option(
    "--bids-directory",
    "-o",
    help="Path to the folder where the BIDS dataset will be created (default: current working directory).",
    required=False,
    type=rich_click.Path(writable=True),
    default=None,
)
def _dandicompute_submit_cli(
    bids_directory: str | None = None,
) -> None:
    generate_aind_ephys_submission_script(bids_directory=bids_directory)
