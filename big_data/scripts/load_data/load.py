"""Load data source code."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from big_data.scripts.load_data.data_types import YouTubeData


def _parse_file(file_path: Path) -> list[YouTubeData]:
    """Given a data file path, open and parse it into a list of YT data."""
    if file_path.is_file() is False:
        msg = f"file_path does not point to a file: {file_path}"
        raise FileNotFoundError(msg)

    with file_path.open(encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")

        return [YouTubeData.from_csv(line) for line in reader]


def _parse_directory(dir_path: Path) -> dict[str, list[YouTubeData]]:
    """Parse each file in directory returning {'directory name', data}."""
    if dir_path.is_dir() is False:
        msg = f"dir_path does not point to a directory: {dir_path}"
        raise FileNotFoundError(msg)

    data = {}
    for path in dir_path.iterdir():
        if path.is_dir():
            data |= _parse_directory(path)  # merge keys, values from below
        elif path.is_file() and not path.stem.startswith("log"):
            data[dir_path.stem] = _parse_file(path)  # add this file key, value
        else:
            msg = f"not a file or dir??? {dir_path}"
            raise FileExistsError()

    return data


def main() -> None:
    """Load data given cli commands."""
    parser = argparse.ArgumentParser(
        prog="YouTube data loader",
        description="loads YT data from text w/ predetermined schema",
    )

    parser.add_argument(
        "--dir-path",
        type=Path,
        help="load all txt files from relative directory path",
        required=True,
        default="",
    )

    args = parser.parse_args()
    if args.dir_path.is_file():
        msg = f"dir-path is not a file: {args.dir_path}"
        raise argparse.ArgumentError(None, msg)

    print(f"path: {args.dir_path}\n")
