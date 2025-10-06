"""Load data source code."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Generator

from scripts.load_data.data_types import YouTubeData


def _parse_file(path: Path) -> list[YouTubeData]:
    with path.open(encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")

        data: list[YouTubeData] = []
        for num, line in enumerate(reader):
            try:
                data.append(YouTubeData.from_csv(line))
            except TypeError:
                print(f"failed to parse {num}: {line}")

        return data


def _iter_data(dir_path: Path) -> Generator[tuple[str, list[YouTubeData]]]:
    """Parse each file in directory returning {'directory name', data}."""
    if dir_path.is_dir() is False:
        msg = f"dir_path does not point to a directory: {dir_path}"
        raise FileNotFoundError(msg)

    for path in dir_path.iterdir():
        if path.is_dir():
            yield from _iter_data(path)  # new generator inside path
        elif path.is_file() and not path.stem.startswith("log"):
            yield path.parent.stem, _parse_file(path)


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
    # TODO (Gavin): add another arg for optionally logging parsed data

    args = parser.parse_args()
    if args.dir_path.is_file():
        msg = f"dir-path is not a file: {args.dir_path}"
        raise argparse.ArgumentError(None, msg)

    for dir_name, data in _iter_data(args.dir_path):
        print(f"{len(data)} lines read from file in folder {dir_name}\n")
