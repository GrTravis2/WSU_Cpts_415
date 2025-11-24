"""Helper functions for submitting jobs to pyspark cluster."""

import pathlib
import subprocess


def spark_submit(script_name: str, packages: str) -> None:
    """Pass the given script **inside analysis folder** to spark cluster to queue the job."""
    path = pathlib.Path("big_data/scripts/analysis/") / script_name
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
