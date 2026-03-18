import pathlib
import shutil

import click
import pydantic


@pydantic.validate_call
def clean_work_directory(directory: pathlib.Path) -> None:
    """
    Clean all contents of a directory except the 'apptainer_cache' subdirectory.

    Parameters
    ----------
    directory : pathlib.Path
        Path to the directory to clean.
    """
    if not directory.is_dir():
        message = f"The specified directory does not exist or is not a directory: {directory}"
        raise NotADirectoryError(message)
    for item in directory.iterdir():
        if item.name == "apptainer_cache":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()


@pydantic.validate_call
def _styled_echo(text: str, color: str) -> None:
    """
    Style a message for Click output.

    Parameters
    ----------
    text : str
        The message text to be styled.
    color : str
        The color to apply to the message (e.g., 'red', 'green', 'yellow', etc.).

    Returns
    -------
    str
        The styled message.
    """
    message = click.style(text=text, fg=color)
    click.echo(message=message)
