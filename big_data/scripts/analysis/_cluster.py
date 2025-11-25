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
            "master",  # run this command on master container
            "/opt/spark/bin/spark-submit",  # submit as spark job
            "--master",
            "spark://master:7077",  # use the spark network
            "--packages",  # include passed packages
            packages,
            str("/".join(path.parts[1:])),  # adjust for flattened scripts
        ],
    )
