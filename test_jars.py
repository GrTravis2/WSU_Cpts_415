from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("JarTest")
        .config("spark.jars",
                "file:///C:/hadoop/lib/mongo-spark-connector_2.12-3.0.2.jar,"
                "file:///C:/hadoop/lib/mongodb-driver-sync-4.3.4.jar,"
                "file:///C:/hadoop/lib/mongodb-driver-core-4.3.4.jar,"
                "file:///C:/hadoop/lib/bson-4.3.4.jar")
        .getOrCreate()
)

print("Spark loaded JARs:", spark.sparkContext._conf.get("spark.jars"))
spark.stop()
