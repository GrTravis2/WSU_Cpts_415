"""PySpark Algorithm for comparing YouTube Data related ids vs popularity."""

import argparse

import matplotlib.pyplot as plt
import pandas
import pymongo
from pyspark.sql import SparkSession, functions
from scripts.analysis import _cluster


def new_spark_session(app_name: str, *, host: str = "localhost", db_host: str = "localhost") -> SparkSession:
    """Create connection to mongodb using sparkSession object."""
    mongo_read_uri = f"mongodb://{db_host}/youtube_analysis.videos"
    mongo_write_uri = f"mongodb://{db_host}/youtube_analysis.analyze_links"
    mongo_conn = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    spark: SparkSession = (  # init connection stuff
        SparkSession.builder.config("spark.driver.host", host)  # type: ignore
        .config("spark.mongodb.read.connection.uri", mongo_read_uri)
        .config("spark.mongodb.write.connection.uri", mongo_write_uri)
        .config("spark.jars.packages", mongo_conn)
        .master("local")
        .appName(app_name)
        .getOrCreate()
    )

    return spark


def plot_image(df: pandas.DataFrame) -> None:
    """Query new mongodb collection, plot, and then save image in the output dir."""
    # convert to pandas to use matplotlib api for viewing data
    _, axes = plt.subplots(nrows=2, ncols=2, figsize=(15, 15))
    axes = axes.flatten()
    labels = [
        ("Times Linked", "Views"),
        ("Times Linked", "# of Ratings"),
        ("Times Linked", "# of Comments"),
        ("Times Linked", "Age in Days"),
    ]
    for i, a in enumerate(axes):
        x_label, y_label = labels[i]
        a.set_xlabel(x_label)
        a.set_ylabel(y_label)
    axes[3].tick_params(axis="x", labelrotation=90)
    df.plot.scatter(
        x="sum(link_count)",
        y="views",
        ax=axes[0],
        title="num links vs views",
    )
    df.plot.scatter(
        x="sum(link_count)",
        y="num_ratings",
        ax=axes[1],
        title="num links vs num ratings",
    )
    df.plot.scatter(
        x="sum(link_count)",
        y="num_comments",
        ax=axes[2],
        title="num links vs num comments",
    )
    df.plot.scatter(
        x="sum(link_count)",
        y="age_days",
        ax=axes[3],
        title="num links vs age of video in days",
    )
    plt.savefig("analyze_links.png")


def main() -> None:
    """Script entry point."""
    parser = argparse.ArgumentParser(
        prog="YouTube data loader",
        description="loads YT data from text w/ predetermined schema",
    )
    parser.add_argument(
        "--use-cluster",
        action="store_true",
        help="submit job to pyspark cluster for processing",
        default=False,
    )
    parser.add_argument(
        "--view-results",
        action="store_true",
        help="query mongodb container for query results",
        default=False,
    )
    args = parser.parse_args()

    if args.use_cluster:
        # pass script to spark cluster and let it do the work before exiting
        # script = pathlib.Path("big_data/scripts/analysis/analyze_links.py")
        mongo_conn = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
        _cluster.spark_submit("analyze_links.py", mongo_conn)
        return

    if args.view_results:
        mongo = pymongo.MongoClient("localhost", 27017)
        links = mongo["youtube_analysis"].get_collection("analyze_links")
        df = pandas.DataFrame(list(links.find()))
        plot_image(df)
        return

    spark = new_spark_session("analyze_links", db_host="db:27017")

    # linters disagree here and I dont know how to fix T_T
    # fmt: off
    df = ( # read the data  for further processing
        spark.read.format("mongodb")
        .option("database", "youtube_analysis")
        .option("collection", "videos")
        .load()
    )
    # fmt: on
    df.show()

    link_counts = df.select(  # break out related_ids where each link is its own row
        "related_ids",
        functions.explode("related_ids").alias("id"),
    )
    # add a link count column with constant value of 1 (similar to map function)
    link_counts = link_counts.withColumn("link_count", functions.lit(1))
    link_counts = (  # reduce link counts by summing each reference to a video
        link_counts.groupBy("id")
        .sum("link_count")
        .select("id", "sum(link_count)")
        .orderBy("sum(link_count)", ascending=False)
    )
    # select important categories before joining with link count table
    dependent_vars = df.select(
        "id",
        df.video_engagement.getItem("views").alias("views"),
        df.video_engagement.getItem("num_ratings").alias("num_ratings"),
        df.video_engagement.getItem("num_comments").alias("num_comments"),
        df.video_desc.getItem("age_days").alias("age_days"),
    )
    # join both tables, only keeping rows that are in **BOTH** tables!
    analyze_links = link_counts.join(dependent_vars, "id", "inner")
    analyze_links = analyze_links.orderBy(
        "sum(link_count)",
        ascending=False,
    )

    # mongo_write = "mongodb://db/youtube_analysis.analyze_links"
    # mongo_conn = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    (analyze_links.write.format("mongodb").mode("overwrite").save())


if __name__ == "__main__":
    main()
