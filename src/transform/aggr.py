from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import Window as w

spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.executor.memory", "512m")
    .appName("aggregate-data")
    .getOrCreate()
)


spark.sparkContext.setLogLevel("ERROR")


csv_configs = {
    "header": "true",
    "multiLine": "true",
    "escape": '"',
    "nullValue": "null",
}

df = spark.read.options(**csv_configs).csv("s3a://databricks-bucket/netflix_titles.csv")
df.show(n=5)


df = df.withColumn(
    "date_added", f.to_date(f.trim(f.col("date_added")), format="MMMM d, yyyy")
)
df.show(n=5)


grouped_data = df.groupBy(f.col("Country"))

count_df = grouped_data.count().orderBy(f.desc("count"))
count_df.show()

df.show(n=5)
max_release_df = grouped_data.agg(f.max(f.col("date_added")))
max_release_df.show()

release_date_grouped = (
    df.groupBy("Country")
    .agg(
        f.count(f.col("show_id")).alias("NumberOfReleases"),
        f.max(f.col("date_added")).alias("LastReleaseDate"),
        f.min(f.col("date_added")).alias("FirstReleaseDate"),
    )
    .orderBy(f.desc(f.col("NumberOfReleases")))
)
release_date_grouped.show(5)


pivot_table = (
    df.groupBy("Country")
    .pivot("type")
    .agg(f.count(f.col("show_id")))
    .orderBy(f.col("Movie"))
)
pivot_table.show(5)

schema = f.StructType(
    [
        t.StructField("Id", t.IntegerType(), True),
        t.StructField("ProductId", t.StringType(), True),
        t.StructField("UserId", t.StringType(), True),
        t.StructField("ProfileName", t.StringType(), True),
        t.StructField("HelpfulnessNumerator", t.StringType(), True),
        t.StructField("HelpfulnessDenominator", t.StringType(), True),
        t.StructField("Score", t.IntegerType(), True),
        t.StructField("Time", t.StringType(), True),
        t.StructField("Summary", t.StringType(), True),
        t.StructField("Text", t.StringType(), True),
    ]
)

review_df = (
    spark.read.option("header", "true")
    .schema(schema)
    .csv("s3a://databricks-bucket/Reviews.csv")
)
review_df.show(5)


quantiles = review_df.approxQuantile("Score", [0.25, 0.50, 0.75], 0.1)
print("Approximate Quantiles:", quantiles)


# windowing
window_spec = w.partitionBy("Country").orderBy("date_added")
result = df.withColumn("row_number", f.row_number().over(window_spec))
result.select(
    f.col("title"), f.col("country"), f.col("date_added"), f.col("row_number")
).show(5)
