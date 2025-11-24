"""Helper functions for submitting jobs to pyspark cluster."""

import pathlib
import subprocess


def spark_submit(path: pathlib.Path, packages: str) -> None:
    """Pass the given path to spark cluster to queue the job."""
    subprocess.run(
        [
            "docker",
            "exec",
            "-it",
            "master",
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://master:7077",
            "--packages",
            packages,
            # str(path)
            str("/".join(path.parts[1:])),
        ],
    )
