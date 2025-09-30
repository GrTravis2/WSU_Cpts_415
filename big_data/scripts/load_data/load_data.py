"""Load data source code."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from data_types import YouTubeData


def parse_file(file_path: Path) -> list[YouTubeData]:
    """Given a data file path, open and parse it into a list of YT data."""
    if file_path.is_file() is False:
        msg = f"file_path does not point to a file: {file_path}"
        raise FileNotFoundError(msg)

    with file_path.open(encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")

        return [YouTubeData.from_csv(line) for line in reader]


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
    parser.add_argument(
        "--super",
        action="store_true",
        help="load the contents of directories inside given path",
        default="",
    )

    args = parser.parse_args()
    if args.dir_path.is_file():
        msg = f"dir-path is not a file: {args.dir_path}"
        raise argparse.ArgumentError(None, msg)
