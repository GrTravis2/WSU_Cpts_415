"""PySpark Algorithm for graph clustering."""

import graphframes
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions


def new_spark_session(app_name: str, host: str = "localhost") -> SparkSession:
    """Create connection to mongodb using sparkSession object."""
    mongo_uri = "mongodb://127.0.0.1/youtube_analysis.videos"
    mongo_conn = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    gf_io = "io.graphframes:graphframes-spark3_2.12:0.10.0"
    packages = ",".join((mongo_conn, gf_io))
    spark: SparkSession = (  # init connection stuff
        SparkSession.builder.config("spark.driver.host", host)  # type: ignore
        .config("spark.mongodb.read.connection.uri", mongo_uri)
        .config("spark.jars.packages", packages)
        .master("local")
        .appName(app_name)
        .getOrCreate()
    )

    return spark


def main() -> None:
    """Script entry point."""
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
    clusters.orderBy(
        functions.size(functions.col("collect_list(id)")), ascending=False
    ).show()
    print(f"cluster rows={clusters.count()}\n")

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
    full_pandas_df = clusters.toPandas()
    full_pandas_df.plot.scatter(
        x="cluster_size",
        y="avg(views)",
        ax=axes[0],
        title="Group Size vs Avg Views",
    )
    reduced_df = clusters.filter(
        functions.size(functions.col("collect_list(id)")) > 1
    ).toPandas()
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
    plt.show()


if __name__ == "__main__":
    main()
