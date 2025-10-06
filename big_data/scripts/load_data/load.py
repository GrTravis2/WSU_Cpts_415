"""Load data source code."""

from __future__ import annotations

import argparse
import contextlib
import csv
import functools
import json
from pathlib import Path
from typing import Generator

from scripts.load_data.data_types import YouTubeData


def _parse_file(path: Path) -> tuple[list[YouTubeData], list[str]]:
    with path.open(encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")

        data: list[YouTubeData] = []
        fails: list[str] = []
        for line in reader:
            try:
                data.append(YouTubeData.from_csv(line))
            except TypeError:
                fails.append("\t".join(line) + "\n")

        return data, fails


def _iter_data(
    dir_path: Path,
) -> Generator[tuple[str, list[YouTubeData], list[str]]]:
    """Parse each file in directory returning {'directory name', data}."""
    if dir_path.is_dir() is False:
        msg = f"dir_path does not point to a directory: {dir_path}"
        raise FileNotFoundError(msg)

    for path in dir_path.iterdir():
        if path.is_dir():
            yield from _iter_data(path)  # new generator inside path
        elif (
            path.is_file()  # must be a file
            and not path.stem.startswith("log")  # no log files
            and not path.stem.startswith(".")  # no hidden files
        ):
            data, fails = _parse_file(path)
            yield path.parent.stem, data, fails


def main() -> None:
    """Load data given cli commands."""
    parser = argparse.ArgumentParser(
        prog="YouTube data loader",
        description="loads YT data from text w/ predetermined schema",
    )

    parser.add_argument(
        "--i",
        type=Path,
        help="load all txt files from relative directory path",
        required=True,
        default="",
    )
    parser.add_argument(
        "--o",
        type=Path,
        help="output json to single file in folder passed as out",
        required=True,
        default="",
    )
    parser.add_argument(
        "--log",
        action="store_true",
        help="if provided then failed lines will be logged to out folder",
        default=False,
    )

    args = parser.parse_args()  # check that given folder paths are good
    i_path: Path = args.i
    if not i_path.exists() or i_path.is_file():
        msg = f"given input data path is not a valid folder: {args.i}"
        raise argparse.ArgumentError(None, msg)

    o_path: Path = args.o
    if not o_path.exists() or o_path.is_file():
        msg = f"given output data path is not a valid folder: {args.o}"
        raise argparse.ArgumentError(None, msg)

    for name, data, fails in _iter_data(i_path):
        # data from 2007 has mm/dd format
        # data from 2008+ has format yy/mm/dd -> prepend yy if name is short
        yy_mm_dd = "07" + name if len(name) < 6 else name

        with (
            (o_path / f"{yy_mm_dd}.json").open("a", encoding="utf-8") as out,
            (
                (o_path / f"{yy_mm_dd}.log").open("a", encoding="utf-8")
                if args.log
                else contextlib.nullcontext()
            ) as log,
            (o_path / f"{yy_mm_dd}_stats.txt").open("a", encoding="utf-8") as s,
        ):
            jsons = [d.to_json(yy_mm_dd) + "\n" for d in data]  # data -> json
            out.writelines(jsons)

            if log:
                log.writelines(fails)  # raw lines to log

            # compute basic stats to be used for validation
            validate = {
                "parsed_lines": len(data),  # count of good lines
                "sum_views": functools.reduce(  # sum of views from good lines
                    lambda a, b: a + b.views, data, 0
                ),
            }
            s.write(json.dumps(validate) + "\n")
