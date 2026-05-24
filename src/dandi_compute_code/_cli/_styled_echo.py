import click
import pydantic


@pydantic.validate_call
def _styled_echo(text: str, color: str) -> None:
    """
    Style a message for Click output.

    :param text: The message text to be styled.
    :type text: str
    :param color: The color to apply to the message (e.g., 'red', 'green', 'yellow', etc.).
    :type color: str
    """
    message = click.style(text=text, fg=color)
    click.echo(message=message)
