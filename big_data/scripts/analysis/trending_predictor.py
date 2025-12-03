"""pyspark.ml trending video predictor."""

import argparse

import pandas
import pymongo
import scripts.analysis._cluster as _cluster
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as func


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


class TrendingVideoPredictor:
    """trend predictor."""

    def __init__(self):
        """Set up  the weights."""
        # arbitrary ml model weights for trend score
        self.weights = {
            "recency_boost": 20,
            "views_velocity": 15,
            "engagement_density": 3,
            "rating_quality": 25,
            "length_score": 1,
        }

    def prepare_data(self, df):
        """Clean and prep data."""
        # get columns out of nests
        prepared_df = df.select(
            "id",
            df.video_desc.getItem("uploader").alias("uploader_name"),
            df.video_desc.getItem("age_days").alias("age_days"),
            df.video_desc.getItem("category").alias("category"),
            df.video_attri.getItem("length").alias("length_seconds"),
            df.video_engagement.getItem("views").alias("views"),
            df.video_attri.getItem("rating").alias("video_rating"),
            df.video_engagement.getItem("num_ratings").alias("num_ratings"),
            df.video_engagement.getItem("num_comments").alias("num_comments"),
            "related_ids",
        )

        # make sure data is clean, otherwise get some divide by zero errors
        cleaned_df = prepared_df.filter(
            (func.col("id").isNotNull())
            & (func.col("age_days") >= 0)
            & (func.col("views") >= 0)
            & (func.col("video_rating").between(0, 5))
            & (func.col("num_ratings") >= 0)
            & (func.col("num_comments") >= 0)
        )

        return cleaned_df

    def calculate_features(self, df):
        """Metrics for trending prediction."""
        # views over time, faster growth is better
        df = df.withColumn("recency_boost", func.col("age_days") / 100.0)

        df = df.withColumn("log_views", func.log1p(func.col("views")))
        df = df.withColumn(
            "views_velocity",
            func.col("log_views") * func.col("age_days") / 1000.0,  # scaled down
        )

        # interactions per view higher is better
        df = df.withColumn("total_engagements", func.col("num_ratings") + func.col("num_comments"))
        df = df.withColumn(
            "engagement_density",
            (func.col("total_engagements") / (func.col("log_views") + 1)) / 1000.0,  # scaled down
        )

        # rating quality (weighted by number of ratings) higher is better
        df = df.withColumn("rating_confidence", func.log1p(func.col("num_ratings")) / 10)
        df = df.withColumn("rating_quality", func.col("video_rating") * func.col("rating_confidence"))

        # newer videos with high engagement are extra interesting
        df = df.withColumn(
            "new_video_engagement_bonus",
            func.when(
                (func.col("age_days") > 710)  # relatively new video
                & (func.col("engagement_density") > 0.01),  # Good engagement rate
                2,  # bonus
            ).otherwise(1.0),
        )

        # youtube length preference scoring, short and very long dont tend to do as well
        # around 10 minutes is best
        df = df.withColumn(
            "length_score",
            func.when(func.col("length_seconds").between(120, 900), 1.0)
            .when(func.col("length_seconds") < 30, 0.3)
            .when(func.col("length_seconds") > 3600, 0.4)
            .otherwise(0.7),
        )

        return df

    def build_trending_pipeline(self):
        """Create ML pipeline."""
        feature_columns = [
            "recency_boost",
            "views_velocity",
            "engagement_density",
            "rating_quality",
            "new_video_engagement_bonus",
            "length_score",
        ]

        # make a vector for processing
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="raw_features")

        # standardize to put all features on same scale
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True,
        )

        return Pipeline(stages=[assembler, scaler])

    def calculate_trending_score(self, df):
        """Calculate final trending score using ml results."""
        # Calculate base score directly from the original features
        # Since StandardScaler standardizes the features, we can work with the raw features
        # and apply the standardization formula manually if needed, but for simplicity:

        # Use the original raw features before scaling (they're still in the dataframe)
        df_with_base = df.withColumn(
            "base_trending_score",
            func.col("recency_boost") * self.weights["recency_boost"]
            + func.col("views_velocity") * self.weights["views_velocity"]
            + func.col("engagement_density") * self.weights["engagement_density"]
            + func.col("rating_quality") * self.weights["rating_quality"]
            + func.col("length_score") * self.weights["length_score"],
        )

        scored_df = df_with_base.withColumn(
            "trending_score", func.col("base_trending_score") * func.col("new_video_engagement_bonus")
        )

        return scored_df

    def get_trending_rankings(self, df, top_n=50):
        """Get final trending rankings with insights."""
        category_window = Window.partitionBy("category").orderBy(func.desc("trending_score"))
        global_window = Window.orderBy(func.desc("trending_score"))

        ranked_df = df.select(
            "id",
            "uploader_name",
            "category",
            "age_days",
            "views",
            "video_rating",
            "num_ratings",
            "num_comments",
            "recency_boost",
            "views_velocity",
            "engagement_density",
            "rating_quality",
            "length_score",
            "rating_confidence",
            "base_trending_score",
            "trending_score",
            "new_video_engagement_bonus",
            func.row_number().over(category_window).alias("category_rank"),
            func.percent_rank().over(global_window).alias("global_percentile"),
        ).filter(func.col("category_rank") <= top_n)

        return ranked_df

    def print_feature_breakdown(self, df, limit=20):
        """Return stats for top videos as string."""
        output = []
        output.append("\n" + "=" * 80)
        output.append("STATS FOR TOP VIDEOS")
        output.append("=" * 80)

        top_videos = df.orderBy(func.desc("trending_score")).limit(limit).collect()

        output.append(f"\nFeature Weights: {self.weights}")
        output.append("\nTop Videos Feature Analysis:")
        output.append("-" * 80)

        for i, video in enumerate(top_videos, 1):
            output.append(f"\n#{i}: {video['uploader_name']} - {video['category']}")
            output.append(f"Video ID: {video['id']}")
            output.append(
                f"Age Days: {video['age_days']} | Views: {video['views']:,} | Rating: {video['video_rating']}"
            )
            output.append(f"Ratings: {video['num_ratings']:,} | Comments: {video['num_comments']:,}")

            # stats
            output.append("\nRaw Video Stats:")
            output.append(
                f"  - Recency Boost: {video['recency_boost']:.1f} \
                 (weight: {self.weights['recency_boost']})"
            )
            output.append(
                f"  - Views Velocity: {video['views_velocity']:.3f} \
                  (weight: {self.weights['views_velocity']})"
            )
            output.append(
                f"  - Engagement Density: {video['engagement_density']:.6f} \
                (weight: {self.weights['engagement_density']})"
            )
            output.append(
                f"  - Rating Quality: {video['rating_quality']:.3f} (weight: {self.weights['rating_quality']})"
            )
            output.append(f"  - Length Score: {video['length_score']:.1f} (weight: {self.weights['length_score']})")
            output.append(f"  - Rating Confidence: {video['rating_confidence']:.3f}")

            # hot bonus
            bonus_text = "APPLIED" if video["new_video_engagement_bonus"] > 1.0 else "not applied"
            output.append(f"  - New Video Engagement Bonus: {video['new_video_engagement_bonus']} ({bonus_text})")
            # output final scores
            output.append("\nFinal Scores:")
            output.append(f"  - Scaled Base Trending Score: {video['base_trending_score']:.3f}")
            output.append(f"  - Final Trending Score: {video['trending_score']:.3f}")
            output.append(f"  - Category Rank: #{video['category_rank']}")
            output.append("-" * 80)

        return "\n".join(output)

    def run_analysis(self, df):
        """Run trending analysis pipeline."""
        print("Preparing data...")
        prepared_data = self.prepare_data(df)

        print(f"Records after cleaning: {prepared_data.count()}")

        print("Calculating metrics...")
        featured_data = self.calculate_features(prepared_data)

        print("Building ML pipeline...")
        pipeline = self.build_trending_pipeline()
        pipeline_model = pipeline.fit(featured_data)
        processed_data = pipeline_model.transform(featured_data)

        print("Calculating trending scores...")
        scored_data = self.calculate_trending_score(processed_data)

        print("Generating final rankings...")
        final_rankings = self.get_trending_rankings(scored_data)

        return final_rankings


def write_to_mongodb(df, collection_name="trendCollection") -> None:
    """Write DataFrame to MongoDB collection."""
    print(f"Writing results to MongoDB collection: {collection_name}")

    df.write.format("mongodb").mode("overwrite").option("database", "youtube_analysis").option(
        "collection", collection_name
    ).save()

    print(f"Successfully wrote {df.count()} records to {collection_name}")


def read_from_mongodb() -> None:
    """Read pregenerated data from mongodb."""
    # create a spark session to read the mongo collection as a Spark DataFrame
    spark = new_spark_session("trending_predictor_read")

    df = (
        spark.read.format("mongodb")
        .option("database", "youtube_analysis")
        .option("collection", "trendCollection")
        .load()
    )

    predictor = TrendingVideoPredictor()
    output_lines = []
    output_lines.append(f"Records loaded: {df.count()}")

    # print feature breakdown (expects a Spark DataFrame)
    output_lines.append(predictor.print_feature_breakdown(df, limit=20))

    output_lines.append("\n=== TOP 20 TRENDING VIDEOS ===")
    top20 = (
        df.orderBy(func.desc("trending_score"))
        .select(
            "id",
            "uploader_name",
            "category",
            "trending_score",
            "views",
            "age_days",
            "category_rank",
        )
        .limit(20)
        .collect()
    )

    header = f"{'ID':<15} {'Uploader':<20} {'Category':<15} {'Trend Score':<12} \
        {'Views':<12} {'Age Days':<10} {'Cat Rank':<8}"
    output_lines.append(header)
    output_lines.append("-" * len(header))

    for row in top20:
        line = f"{row['id']:<15} {row['uploader_name']:<20} {row['category']:<15} {row['trending_score']:<12.3f} \
            {row['views']:<12} {row['age_days']:<10} {row['category_rank']:<8}"
        output_lines.append(line)

    output_str = "\n".join(output_lines)
    write_to_txt_file(output_str)

    spark.stop()


def write_to_txt_file(output_str, file_path="text_outputs/trend_output.txt") -> None:
    """Write output string to a text file."""
    with open(file_path, "w") as file:
        file.write(output_str)
    print(f"Results written to {file_path}")
    return


def main():
    """Script entry point."""
    parser = argparse.ArgumentParser(
        prog="Trend algorithm script",
        description="calculate trending video scores",
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
        # script = pathlib.Path("big_data/scripts/analysis/trending_predictor.py")
        mongo_conn = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
        _cluster.spark_submit("trending_predictor.py", mongo_conn)
        return
    if args.view_results:
        mongo = pymongo.MongoClient("localhost", 27017)
        trends = mongo["youtube_analysis"].get_collection("trendCollection")
        df = pandas.DataFrame(list(trends.find()))
        output_lines = []
        output_lines.append("Top Trending Videos from MongoDB:\n")
        output_lines.append(df.head(20).to_string())
        output_str = "\n".join(output_lines)
        write_to_txt_file(output_str)
        return

    spark = new_spark_session("trending_predictor")

    # fmt: off
    df = (
        spark.read.format("mongodb")
        .option("database", "youtube_analysis")
        .option("collection", "videos")
        .load()
    )
    # fmt: on

    output_lines = []
    output_lines.append(f"Records loaded: {df.count()}")

    # run predictor
    predictor = TrendingVideoPredictor()
    results = predictor.run_analysis(df)

    output_lines.append(predictor.print_feature_breakdown(results, limit=20))

    output_lines.append("\n=== TOP 20 TRENDING VIDEOS ===")
    top20 = (
        results.orderBy(func.desc("trending_score"))
        .select(
            "id",
            "uploader_name",
            "category",
            "trending_score",
            "views",
            "age_days",
            "category_rank",
        )
        .limit(20)
        .collect()
    )
    header = f"{'ID':<15} {'Uploader':<20} {'Category':<15} {'Trend Score':<12}\
          {'Views':<12} {'Age Days':<10} {'Cat Rank':<8}"
    output_lines.append(header)
    output_lines.append("-" * len(header))

    for row in top20:
        line = f"{row['id']:<15} {row['uploader_name']:<20} {row['category']:<15} \
            {row['trending_score']:<12.3f} {row['views']:<12} {row['age_days']:<10} {row['category_rank']:<8}"
        output_lines.append(line)
    write_to_mongodb(results, "trendCollection")

    spark.stop()
    output_str = "\n".join(output_lines)
    write_to_txt_file(output_str)
    return


if __name__ == "__main__":
    main()
