"""PySpark Algorithm for graph clustering."""

import argparse

import graphframes
import matplotlib.pyplot as plt
import pandas
import pymongo
from pyspark.sql import SparkSession, functions
from scripts.analysis import _cluster


def new_spark_session(app_name: str, *, host: str = "localhost", db_host: str = "localhost") -> SparkSession:
    """Create connection to mongodb using sparkSession object."""
    mongo_read_uri = f"mongodb://{db_host}/youtube_analysis.videos"
    mongo_write_uri = f"mongodb://{db_host}/youtube_analysis.graph_filter"
    mongo_conn = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    gf_io = "io.graphframes:graphframes-spark3_2.12:0.10.0"
    packages = ",".join((mongo_conn, gf_io))
    spark: SparkSession = (  # init connection stuff
        SparkSession.builder.config("spark.driver.host", host)  # type: ignore
        .config("spark.mongodb.read.connection.uri", mongo_read_uri)
        .config("spark.mongodb.write.connection.uri", mongo_write_uri)
        .config("spark.jars.packages", packages)
        .master("local")
        .appName(app_name)
        .getOrCreate()
    )

    return spark


def plot_image(df: pandas.DataFrame) -> None:
    """Plot the image and save to file."""
    # convert to pandas to use matplotlib api for viewing data
    _, axes = plt.subplots(nrows=1, ncols=3, figsize=(15, 15))
    axes = axes.flatten()
    labels = [
        ("Group Size", "Views"),
        ("Group Size", "# of Distinct Uploaders"),
        ("Times Linked", "# of Distinct Categories"),
    ]
    for i, a in enumerate(axes):
        x_label, y_label = labels[i]
        a.set_xlabel(x_label)
        a.set_ylabel(y_label)
    df.plot.scatter(
        x="cluster_size",
        y="avg(views)",
        ax=axes[0],
        title="Group Size vs Avg Views",
    )
    # reduced_df = df.filter(functions.size(functions.col("collect_list(id)")) > 1).toPandas()
    reduced_df = df[df["collect_list(id)"].apply(len) > 1]
    reduced_df.plot.scatter(
        x="cluster_size",
        y="distinct_uploaders",
        ax=axes[1],
        title="Group Size vs Distinct Uploaders",
    )
    reduced_df.plot.scatter(
        x="cluster_size",
        y="distinct_categories",
        ax=axes[2],
        title="Group Size vs Distinct Categories",
    )
    #also save to local file

    plt.savefig("pictures/graph_filter.png")


def main() -> None:
    """Script entry point."""
    parser = argparse.ArgumentParser(
        prog="SCC algorithm script",
        description="do the SCC with spark",
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
        gf_io = "io.graphframes:graphframes-spark3_2.12:0.10.0"
        packages = ",".join((mongo_conn, gf_io))
        _cluster.spark_submit("graph_filter.py", packages)
        return

    if args.view_results:
        mongo = pymongo.MongoClient("localhost", 27017)
        links = mongo["youtube_analysis"].get_collection("graph_filter")
        df = pandas.DataFrame(list(links.find()))
        plot_image(df)
        return
    spark = new_spark_session("graph_filter")

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

    edges = df.select(  # break out related_ids where each link is its own row
        df.id.alias("src"),
        functions.explode("related_ids").alias("dst"),
    )

    g = graphframes.GraphFrame(df.limit(100_000), edges.limit(100_000))
    g.vertices.show()
    g.edges.show()

    scc = g.stronglyConnectedComponents(maxIter=10)
    scc = scc.select(
        "id",
        "component",
        scc.video_desc.getItem("uploader").alias("uploader"),
        scc.video_desc.getItem("age_days").alias("age_days"),
        scc.video_desc.getItem("category").alias("category"),
        scc.video_engagement.getItem("views").alias("views"),
        scc.video_engagement.getItem("num_ratings").alias("num_ratings"),
        scc.video_engagement.getItem("num_comments").alias("num_comments"),
    )
    scc.orderBy("component", ascending=False).show()
    print(f"scc rows={scc.count()}\n")

    clusters = scc.groupBy("component").agg(
        functions.collect_list("id"),
        functions.collect_set("uploader"),
        functions.collect_set("category"),
        functions.avg("views"),
    )
    clusters = clusters.withColumns(
        {
            "distinct_uploaders": functions.size("collect_set(uploader)"),
            "cluster_size": functions.size("collect_list(id)"),
            "distinct_categories": functions.size("collect_set(category)"),
        }
    )
    clusters.orderBy(functions.size(functions.col("collect_list(id)")), ascending=False).show()
    print(f"cluster rows={clusters.count()}\n")

    (clusters.write.format("mongodb").mode("overwrite").save())


if __name__ == "__main__":
    main()
