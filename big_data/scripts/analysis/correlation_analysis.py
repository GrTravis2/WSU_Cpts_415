from pymongo import MongoClient
import os
os.environ["JAVA_TOOL_OPTIONS"] = "--add-opens=java.base/java.nio=ALL-UNNAMED"


def import_spark():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    return SparkSession, col


# Now import Spark
SparkSession, col = import_spark()


# loads youtube dataset from mongodb to spark


def load_mongo_data(spark):
    df = (
        spark.read
        .format("mongodb")
        .option("database", "youtube_analysis")
        .option("collection", "videos")
        .load()
    )

    print("Loaded rows:", df.count())
    print("Full Youtube dataset Schema:")
    df.printSchema()
    return df


# takes a spark data frame and returns a data frame with numeric columns


def numeric_df(df):
    numeric_cols = [
        "age_days",
        "length_seconds",
        "num_comments",
        "num_ratings",
        "upload_day",
        "upload_month",
        "upload_year",
        "video_rating",
        "views"
    ]

    # list of instructions to cast numeric columns in spark to double types
    double_columns = [col(c).cast("double").alias(c) for c in numeric_cols]

    # create a new data frame with just numeric columns
    df_num = df.select(*double_columns)

    # remove any empty rows
    df_num = df_num.dropna()

    print("Rows without blank values: ", df_num.count())
    print("Numeric data frame schema:")
    df_num.printSchema()

    return df_num

# computes the correlation of the numeric data frame and returns a list of correlations


def compute_correlation(df):
    numeric_cols = df.columns  # list of the numeric columns
    correlation = []

    # loop through every pair of columns and add the results to a correlation list
    for i in range(len(numeric_cols)):
        for j in range(i + 1, len(numeric_cols)):
            col1 = numeric_cols[i]
            col2 = numeric_cols[j]
            raw = df.stat.corr(col1, col2)
            value = round(raw, 4) if raw is not None else None
            correlation.append((col1, col2, value))

    return correlation


def save_to_mongo(correlation):

    client = MongoClient("mongodb://localhost:27017/")
    db = client["youtube_analysis"]
    collection = db["correlations"]

    # remove old correlation data
    collection.drop()

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
            "category": category
        })

    if documents:
        collection.insert_many(documents)

    print("Correlation data saved to MongoDB!")


def main():
    # start the spark session
    spark = (
        SparkSession.builder
        .appName("CorrelationAnalysis")
        .master("local[*]")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0")
        .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/youtube_analysis")
        .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/youtube_analysis")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    # load the mongo data into spark
    df = load_mongo_data(spark)
    print("Loaded rows:", df.count())

    # create a data frame with just numeric values
    df_num = numeric_df(df)

    # run the correlation values and return the a list of correlations
    correlation = compute_correlation(df_num)

    # save to correlation data to mongoDB
    save_to_mongo(correlation)

    # organize the correlations
    PostiveCorr = []
    NegativeCorr = []
    NearZeroCorr = []

    for col1, col2, value in correlation:
        if (col1 != col2):  # only entries that aren't the same column
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

    spark.stop()


if __name__ == "__main__":
    main()
