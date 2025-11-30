import argparse
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, year
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from ._cluster import spark_submit


def is_cluster():
    return os.path.exists("/opt/spark")


def new_spark_session(app_name):
    if is_cluster():
        mongo_uri = "mongodb://db:27017/youtube_analysis.videos"
        master = "spark://master:7077"
    else:
        mongo_uri = "mongodb://127.0.0.1/youtube_analysis.videos"
        master = "local[*]"
    spark = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.mongodb.read.connection.uri", mongo_uri)
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0")
        .getOrCreate()
    )
    return spark


def load_mongo(spark):
    df = (
        spark.read.format("mongodb")
        .option("database", "youtube_analysis")
        .option("collection", "videos")
        .load()
    )
    return df.select(
        col("video_attri.length_seconds").alias("length_seconds"),
        col("video_attri.rating").alias("video_rating"),
        col("video_engagement.views").alias("views"),
        col("video_engagement.num_ratings").alias("num_ratings"),
        col("video_engagement.num_comments").alias("num_comments"),
        col("upload_date"),
    )


def save_heatmap(df, path):
    data = df.values
    labels = list(df.columns)
    plt.figure(figsize=(10, 8))
    plt.imshow(data, cmap="coolwarm", vmin=-1, vmax=1)
    plt.colorbar(label="Correlation")
    plt.xticks(np.arange(len(labels)), labels, rotation=90)
    plt.yticks(np.arange(len(labels)), labels)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def run_correlation():
    spark = new_spark_session("CorrelationAnalysis")
    df = load_mongo(spark)
    df = df.withColumn("upload_day", dayofmonth("upload_date"))
    df = df.withColumn("upload_month", month("upload_date"))
    df = df.withColumn("upload_year", year("upload_date"))
    df_num = df.select(
        col("length_seconds").cast("double"),
        col("num_comments").cast("double"),
        col("num_ratings").cast("double"),
        col("upload_day").cast("double"),
        col("upload_month").cast("double"),
        col("upload_year").cast("double"),
        col("video_rating").cast("double"),
        col("views").cast("double"),
    ).dropna()
    cols = df_num.columns
    assembler = VectorAssembler(inputCols=cols, outputCol="features")
    vdf = assembler.transform(df_num).select("features")
    corr_matrix = Correlation.corr(
        vdf, "features", "pearson").head()[0].toArray()
    pdf = pd.DataFrame(corr_matrix, columns=cols, index=cols)
    if is_cluster():
        out = "/opt/spark/scripts/analysis/correlation_heatmap.png"
    else:
        out = "correlation_heatmap.png"
    save_heatmap(pdf, out)
    spark.stop()


def main(use_cluster):
    if use_cluster:
        spark_submit(
            "correlation_analysis.py",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
        )
        return
    run_correlation()


def entrypoint():
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-cluster", action="store_true")
    args = parser.parse_args()
    main(args.use_cluster)
