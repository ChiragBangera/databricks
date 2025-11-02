from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
from delta import configure_spark_with_delta_pip, DeltaTable


builder = (
    SparkSession.builder.appName("delta-table-creation")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.executor.memory", "512m")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.warehouse.dir", "s3a://delta-lake/warehouse")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# spark.sql(
#    """
#    CREATE DATABASE IF NOT EXISTS datasets
#    COMMENT "Book datasets"
# """
# )

# spark.sql("DROP TABLE IF EXISTS datasets.netflix_titles")
#
# spark.sql(
#    """
#    CREATE OR REPLACE TABLE datasets.netflix_titles(
#    show_id STRING,
#    type STRING,
#    director STRING,
#    cast STRING,
#    country STRING,
#    date_added STRING,
#    release_year STRING,
#    rating STRING,
#    duration STRING,
#    listed_in STRING,
#    description STRING
#    ) USING DELTA"""
# )

spark.sql("CREATE SCHEMA IF NOT EXISTS datasets")


netflix_titles = (
    spark.read.option("header", "true")
    .option("nullValues", "null")
    .option("escape", '"')
    .option("multiLine", "true")
    .csv("s3a://databricks-bucket/netflix_titles.csv")
)

netflix_titles.show(n=5)


netflix_titles.write.format("delta").mode("overwrite").option(
    "mergeSchema", "true"
).saveAsTable("datasets.netflix_titles")
print("table-saved>>>>>>")


readback = spark.read.format("delta").load(
    "s3a://delta-lake/warehouse/datasets.db/netflix_titles"
)
readback.show(n=5)
