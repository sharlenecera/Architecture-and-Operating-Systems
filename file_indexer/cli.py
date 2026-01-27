from typing import Optional
from pathlib import Path
import typer

from file_indexer import __app_name__, __version__
from .f_indexer import get_files_greater_than, hash_file

app = typer.Typer()

def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} v{__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help="Show the application's version and exit.",
        callback=_version_callback,
        is_eager=True,
    )
) -> None:
    return


@app.command()
def get_greater_than(
    root_path: str = typer.Option(
        None,
        "--root-path",
        "rp",
        help="Choose the root path to search.",
    ),
    output_path: str = typer.Option(
        None,
        "--output",
        "-o",
        help="Choose where file outputs to.",
        # prompt="Output path of file",
    ),
    min_file_size: int = typer.Option(
        None,
        "--greater-than",
        "-gt",
        help="Get a list of files bigger than x bytes.",
        prompt="Size files must be greater than, in bytes",
    ),
) -> None:
    """Create a list of files bigger than the input number in bytes."""
    try:
        get_files_greater_than(Path(root_path), Path(output_path), min_file_size)
        print(f"Created list of files greater than {min_file_size} in {output_path or "root directory."}")
    except FileNotFoundError as e:
        print(f"File or directory not found: {e}")
    except:
        typer.secho(
            f"Error occurred when writing to file.",
            fg=typer.colors.RED
        )


@app.command()
def checksum(
    file_path: str = typer.Option(
        None,
        "--file",
        "-f",
        help="File to return the hash of.",
        prompt="File path",
    ),
    hash_type: str = typer.Option(
        "sha256",
        "--hash",
        "-h",
        help="Type of hash to use"
    )
) -> None:
    """Return the hash of ."""
    try:
        print(f"Hash: {hash_file(file_path, hash_type)}")
    except ValueError as e:
        print(f"Value Error: {e}")
    except:
        typer.secho(
            f"Error occurred when hashing file.",
            fg=typer.colors.RED
        )
