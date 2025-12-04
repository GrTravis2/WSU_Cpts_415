import argparse  # allows script to take arguments
import os  # checks if where in local or using a cluster
import pandas as pd  # converts numpy array into a labeled table
import numpy as np  # recieves a convert spark matrix as a numpy array
import matplotlib.pyplot as plt  # creates the heatmap from panda table
from pyspark.sql import SparkSession  # to start a spark session
# converts date types to numeric types
from pyspark.sql.functions import col, dayofmonth, month, year
# converts mongo numeric columns into a vector
from pyspark.ml.feature import VectorAssembler
# need this to run correlation analysis
from pyspark.ml.stat import Correlation
# for running the script with a cluster
from scripts.analysis._cluster import spark_submit


def is_cluster():
    # checks if spark is running in the docker container
    return os.path.exists("/opt/spark")


def new_spark_session(app_name):
    # start a spark session
    if is_cluster():  # if the script is running in docker use cluster settings
        mongo_uri = "mongodb://db:27017/youtube_analysis.videos"
        master = "spark://master:7077"
    # otherwise run it with local settings
    else:
        mongo_uri = "mongodb://127.0.0.1/youtube_analysis.videos"
        master = "local[*]"
    # create connection to spark
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
    # create a spark dataframe from the videos collection in the youtube database
    df = (
        spark.read.format("mongodb")
        .option("database", "youtube_analysis")
        .option("collection", "videos")
        .load()
    )
    # only grab the numeric fields for the dataframe
    return df.select(
        df.video_attri.getItem("length").alias("length_seconds"),
        df.video_attri.getItem("rating").alias("video_rating"),

        df.video_engagement.getItem("views").alias("views"),
        df.video_engagement.getItem("num_ratings").alias("num_ratings"),
        df.video_engagement.getItem("num_comments").alias("num_comments"),

        df.video_desc.getItem("age_days").alias("age_days"),

        # keep for day/month/year extraction
        df.upload_date.alias("upload_date"),
    )


def save_heatmap(df, path):
    # draws a heat map of the data and saves it as a png
    data = df.values  # values for the map
    labels = list(df.columns)  # list of column labels
    plt.figure(figsize=(10, 8))  # size of the map
    plt.imshow(data, cmap="coolwarm", vmin=-1, vmax=1)  # color palette
    plt.colorbar(label="Correlation")  # color bar label
    plt.xticks(np.arange(len(labels)), labels,
               rotation=90)  # labels for the x-axis
    plt.yticks(np.arange(len(labels)), labels)  # labels for the y-axis
    plt.tight_layout()  # adjust the layout so everything fits nicely
    plt.savefig(path)  # where to save the png
    plt.close()


def run_correlation():
    spark = new_spark_session("CorrelationAnalysis")  # connect to spark
    df = load_mongo(spark)  # load the mongo data into a spark dataframe
    # modify the date fields to be numeric types
    df = df.withColumn("upload_day", dayofmonth("upload_date"))
    df = df.withColumn("upload_month", month("upload_date"))
    df = df.withColumn("upload_year", year("upload_date"))
    # cast numeric types as floating point type
    df_num = df.select(
        col("length_seconds").cast("double"),
        col("num_comments").cast("double"),
        col("num_ratings").cast("double"),
        col("upload_day").cast("double"),
        col("upload_month").cast("double"),
        col("upload_year").cast("double"),
        col("video_rating").cast("double"),
        col("views").cast("double"),
    ).dropna()  # drop any null rows
    cols = df_num.columns  # saves the column names for labeling the heat map

    # ML algorithms expect a vector, create a new column and every row entries is a vector of the numeric field data in that row
    assembler = VectorAssembler(inputCols=cols, outputCol="features")

    # select only the features column of vectors to create a new dataframe with
    vector_df = assembler.transform(df_num).select("features")
    corr_matrix = Correlation.corr(vector_df, "features", "pearson").head(
        # compute the correlations using the vector dataframe which returns a matrix and then convert it to an array
    )[0].toArray()
    pdf = pd.DataFrame(corr_matrix, columns=cols, index=cols)
    # where to save the heat map if running locally or on a cluster
    if is_cluster():
        out = "/opt/spark/scripts/analysis/correlation_heatmap.png"
    else:
        out = "correlation_heatmap.png"
    save_heatmap(pdf, out)
    spark.stop()


def main(use_cluster):
    # if arg --use-cluster run the script in docker
    if use_cluster:
        spark_submit(
            "correlation_analysis.py",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
        )
        return
    run_correlation()


def entrypoint():
    parser = argparse.ArgumentParser()  # a parser
    # what the parse will look for
    parser.add_argument("--use-cluster", action="store_true")
    args = parser.parse_args()  # parse the tagged flag
    main(args.use_cluster)  # pass the args to main


if __name__ == "__main__":
    # Spark cluster execution path
    if is_cluster():
        run_correlation()
