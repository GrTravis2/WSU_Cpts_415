from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pymongo import MongoClient
import argparse


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--mode", type=str, default="local",
                        help="Execution mode: local or cluster")

    parser.add_argument("--workers", type=int, default=2,
                        help="Number of worker cores for Spark")

    parser.add_argument("--mongo-uri", type=str,
                        default="mongodb://localhost:27017",
                        help="MongoDB connection URI")

    parser.add_argument("--trial-id", type=int, default=0,
                        help="Trial number for repeated test runs")

    return parser.parse_args()


# loads youtube dataset from mongodb to spark


def load_mongo_data(spark):
    df = (
        spark.read
        .format("mongodb")
        .option("database", "youtube_analysis")
        .option("collection", "videos")
        .load()
    )

    return df


# takes a spark data frame and returns a data frame with numeric columns


def numeric_df(df):
    # Map the expected flat column names to their actual nested paths in Mongo
    column_map = {
        "age_days": "video_desc.age_days",
        "length_seconds": "video_attri.length",
        "num_comments": "video_engagement.num_comments",
        "num_ratings": "video_engagement.num_ratings",
        "upload_day": "upload_day",
        "upload_month": "upload_month",
        "upload_year": "upload_year",
        "video_rating": "video_attri.rating",
        "views": "video_engagement.views"
    }

    # Cast each nested field into a flat double column
    double_columns = [
        col(path).cast("double").alias(name)
        for name, path in column_map.items()
    ]

    # Select and clean numeric data
    df_num = df.select(*double_columns).dropna()

    return df_num


# computes the correlation of the numeric data frame and returns a list of correlations


def compute_correlation(df):
    numeric_cols = df.columns  # list of the numeric columns

    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")

    vector_df = assembler.transform(df).select("features")

    result = Correlation.corr(vector_df, "features", "pearson")
    row = result.head()
    corr_matrix = row[0]

    matrix = corr_matrix.toArray()

    correlation = []

    for i in range(len(numeric_cols)):
        for j in range(i + 1, len(numeric_cols)):
            # Get the correlation number from the matrix
            value = round(matrix[i][j], 4)

            # Build the same tuple structure your old code used
            correlation.append((
                numeric_cols[i],   # column name for row i
                numeric_cols[j],   # column name for column j
                value              # the Pearson correlation between them
            ))

    return correlation


def save_to_mongo(correlation, args):
    client = MongoClient(args.mongo_uri)

    db = client["youtube_analysis"]
    collection = db["correlations"]

    # remove old correlation data - MUST be literal string
    try:
        db.command({"drop": "correlations"})
    except Exception as e:
        print("Drop error (safe to ignore if collection doesn't exist):", e)

    # turn correlation data into a mongo format
    documents = []
    for col1, col2, value in correlation:
        if (value > 0.3):
            category = "Positive"
        elif (value < -0.3):
            category = "Negative"
        else:
            category = "Near_Zero"
        documents.append({
            "col1": col1,
            "col2": col2,
            "value": value,
            "category": category,
            "trial_id": args.trial_id,
        })

    if documents:
        collection.insert_many(documents)

    print("Correlation data saved to MongoDB!")


def main(args):
    import time
    start_time = time.time()

    print(
        f"Running trial {args.trial_id} with {args.workers} workers in {args.mode} mode")

    # Select cluster or local execution
    if args.mode == "cluster":
        master = "spark://master:7077"
    else:
        master = "local[*]"

    executor_cores = str(args.workers)

    # start the spark session
    spark = (
        SparkSession.builder
        .appName("CorrelationAnalysis")
        .master(master)
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0")
        .config("spark.mongodb.connection.uri", "mongodb://db:27017")
        .getOrCreate()
    )

    # load data
    df = load_mongo_data(spark)

    from pyspark.sql.functions import dayofmonth, month, year

    df = df.withColumn("upload_day", dayofmonth("upload_date")) \
        .withColumn("upload_month", month("upload_date")) \
        .withColumn("upload_year", year("upload_date"))

    # numeric df
    df_num = numeric_df(df)

    # compute correlation
    correlation = compute_correlation(df_num)

    # save results
    save_to_mongo(correlation, args)

    # organize the correlations
    PostiveCorr = []
    NegativeCorr = []
    NearZeroCorr = []

    for col1, col2, value in correlation:
        if (value > 0.3):
            PostiveCorr.append((col1, col2, value))
        elif (value < -0.3):
            NegativeCorr.append((col1, col2, value))
        else:
            NearZeroCorr.append((col1, col2, value))

    # print results
    print("Positve Correlations:")
    for col1, col2, value in PostiveCorr:
        print(col1, " and ", col2, " correlation score: ", value)

    print("\nNegative Correlations:")
    for col1, col2, value in NegativeCorr:
        print(col1, " and ", col2, " correlation score: ", value)

    print("\nNear Zero Correlations:")
    for col1, col2, value in NearZeroCorr:
        print(col1, " and ", col2, " correlation score: ", value)

    end_time = time.time()
    runtime = end_time - start_time
    print(f"\nTotal Runtime (seconds): {runtime:.2f}")

    spark.stop()


if __name__ == "__main__":
    args = parse_args()
    main(args)
