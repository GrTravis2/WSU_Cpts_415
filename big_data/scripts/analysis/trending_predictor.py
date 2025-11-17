"""pyspark.ml trending video predictor."""

from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as func
from pyspark.sql.types import DoubleType


def new_spark_session(app_name: str, host: str = "localhost") -> SparkSession:
    """Create connection to mongodb using sparkSession object."""
    mongo_uri = "mongodb://127.0.0.1/youtube_analysis.videos"
    mongo_conn = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    spark: SparkSession = (  # init connection stuff
        SparkSession.builder.config("spark.driver.host", host)  # type: ignore
        .config("spark.mongodb.read.connection.uri", mongo_uri)
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
                & (func.col("engagement_density") > 0.01),  # Ggod engagement rate
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

        def compute_weighted_score(scaled_features):
            # Manual weighted sum
            return float(
                scaled_features[0] * self.weights["recency_boost"]
                + scaled_features[1] * self.weights["views_velocity"]
                + scaled_features[2] * self.weights["engagement_density"]
                + scaled_features[3] * self.weights["rating_quality"]
                + scaled_features[5] * self.weights["length_score"]
            )

        weighted_score = func.udf(compute_weighted_score, DoubleType())
        print("Calculating weighted trending scores...")
        df_with_base = df.withColumn("base_trending_score", weighted_score(func.col("scaled_features")))
        scored_df = df_with_base.withColumn(
            "trending_score", func.col("base_trending_score") * func.col("new_video_engagement_bonus")
        )

        return scored_df

    def get_trending_rankings(self, df, top_n=50):
        """Get final trending rankings with insights."""
        # Window for category-wise ranking
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

    def print_feature_breakdown(self, df, limit=15):
        """Stats for top20."""
        print("\n" + "=" * 80)
        print("STATS FOR TOP VIDEOS")
        print("=" * 80)

        top_videos = df.orderBy(func.desc("trending_score")).limit(limit).collect()

        print(f"\nFeature Weights: {self.weights}")
        print("\nTop Videos Feature Analysis:")
        print("-" * 80)

        for i, video in enumerate(top_videos, 1):
            print(f"\n#{i}: {video['uploader_name']} - {video['category']}")
            print(f"Video ID: {video['id']}")
            print(f"Age Days: {video['age_days']} | Views: {video['views']:,} | Rating: {video['video_rating']}")
            print(f"Ratings: {video['num_ratings']:,} | Comments: {video['num_comments']:,}")

            # stats
            print("\nRaw Video Stats:")
            print(
                f"  • Recency Boost: {video['recency_boost']:.1f} \
                 (weight: {self.weights['recency_boost']})"
            )
            print(
                f"  • Views Velocity: {video['views_velocity']:.3f} \
                  (weight: {self.weights['views_velocity']})"
            )
            print(
                f"  • Engagement Density: {video['engagement_density']:.6f} \
                (weight: {self.weights['engagement_density']})"
            )
            print(f"  • Rating Quality: {video['rating_quality']:.3f} (weight: {self.weights['rating_quality']})")
            print(f"  • Length Score: {video['length_score']:.1f} (weight: {self.weights['length_score']})")
            print(f"  • Rating Confidence: {video['rating_confidence']:.3f}")

            # hot bonus
            bonus_text = "APPLIED" if video["new_video_engagement_bonus"] > 1.0 else "not applied"
            print(f"  • New Video Engagement Bonus: {video['new_video_engagement_bonus']} ({bonus_text})")

            # output final scores
            print("\nFinal Scores:")
            print(f"  • Scaled Base Trending Score: {video['base_trending_score']:.3f}")
            print(f"  • Final Trending Score: {video['trending_score']:.3f}")
            print(f"  • Category Rank: #{video['category_rank']}")
            print("-" * 80)

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


def main() -> None:
    """Script entry point."""
    spark = new_spark_session("trending_predictor")

    # fmt: off
    df = (
        spark.read.format("mongodb")
        .option("database", "youtube_analysis")
        .option("collection", "videos")
        .load()
    )
    # fmt: on

    print(f"Records loaded: {df.count()}")

    # run predictor
    predictor = TrendingVideoPredictor()
    results = predictor.run_analysis(df)

    predictor.print_feature_breakdown(results, limit=20)

    print("\n=== TOP 20 TRENDING VIDEOS ===")
    results.orderBy(func.desc("trending_score")).select(
        "id",
        "uploader_name",
        "category",
        "trending_score",
        "views",
        "age_days",
        "category_rank",
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
