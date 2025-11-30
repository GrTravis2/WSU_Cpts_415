from pyspark.sql.functions import col, dayofmonth, month, year
from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pymongo import MongoClient
import argparse
from scripts.analysis._cluster import spark_submit


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="local")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--mongo-uri", type=str,
                        default="mongodb://localhost:27017")
    parser.add_argument("--trial-id", type=int, default=0)
    parser.add_argument("--use-cluster", action="store_true")
    return parser.parse_args()


def load_mongo_data(spark):
    df = (
        spark.read
        .format("mongodb")
        .option("database", "youtube_analysis")
        .option("collection", "videos")
        .load()
    )

    return df.select(
        col("video_desc.age_days").alias("age_days"),
        col("video_attri.length").alias("length_seconds"),
        col("video_attri.rating").alias("video_rating"),
        col("video_engagement.views").alias("views"),
        col("video_engagement.num_ratings").alias("num_ratings"),
        col("video_engagement.num_comments").alias("num_comments"),
        col("upload_date")
    )


def numeric_df(df):
    return df.select(
        col("age_days").cast("double").alias("age_days"),
        col("length_seconds").cast("double").alias("length_seconds"),
        col("num_comments").cast("double").alias("num_comments"),
        col("num_ratings").cast("double").alias("num_ratings"),
        col("upload_day").cast("double").alias("upload_day"),
        col("upload_month").cast("double").alias("upload_month"),
        col("upload_year").cast("double").alias("upload_year"),
        col("video_rating").cast("double").alias("video_rating"),
        col("views").cast("double").alias("views"),
    ).dropna()


def compute_correlation(df):
    numeric_cols = df.columns
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    vector_df = assembler.transform(df).select("features")
    corr_matrix = Correlation.corr(
        vector_df, "features", "pearson").head()[0].toArray()

    correlation = []
    for i in range(len(numeric_cols)):
        for j in range(i + 1, len(numeric_cols)):
            correlation.append(
                (numeric_cols[i], numeric_cols[j], round(corr_matrix[i][j], 4)))
    return correlation


def save_to_mongo(correlation, args):
    client = MongoClient(args.mongo_uri)
    db = client["youtube_analysis"]
    collection = db["correlations"]

    try:
        db.command({"drop": "correlations"})
    except:
        pass

    docs = []
    for col1, col2, value in correlation:
        if value > 0.3:
            category = "Positive"
        elif value < -0.3:
            category = "Negative"
        else:
            category = "Near_Zero"

        docs.append({
            "col1": col1,
            "col2": col2,
            "value": value,
            "category": category,
            "trial_id": args.trial_id
        })

    if docs:
        collection.insert_many(docs)


def run_local(args):
    import time
    start = time.time()

    master = "spark://master:7077" if args.mode == "cluster" else "local[*]"

    spark = (
        SparkSession.builder
        .appName("CorrelationAnalysis")
        .master(master)
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0")
        .config("spark.mongodb.connection.uri", "mongodb://db:27017")
        .getOrCreate()
    )

    df = load_mongo_data(spark)
    df = df.withColumn("upload_day", dayofmonth("upload_date")) \
           .withColumn("upload_month", month("upload_date")) \
           .withColumn("upload_year", year("upload_date"))

    df_num = numeric_df(df)
    correlation = compute_correlation(df_num)

    save_to_mongo(correlation, args)

    positives = [(a, b, v) for a, b, v in correlation if v > 0.3]
    negatives = [(a, b, v) for a, b, v in correlation if v < -0.3]
    near_zero = [(a, b, v) for a, b, v in correlation if -0.3 <= v <= 0.3]

    print("\nPositive Correlations:")
    for a, b, v in positives:
        print(a, b, v)

    print("\nNegative Correlations:")
    for a, b, v in negatives:
        print(a, b, v)

    print("\nNear Zero Correlations:")
    for a, b, v in near_zero:
        print(a, b, v)

    print(f"\nRuntime: {time.time() - start:.2f}s")
    spark.stop()


def entrypoint():
    args = parse_args()

    if args.use_cluster:
        spark_submit(
            "correlation_analysis.py",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
        )
        return

    run_local(args)


if __name__ == "__main__":
    entrypoint()
