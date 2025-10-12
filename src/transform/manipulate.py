from doctest import master
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


spark = (
    SparkSession.builder.appName("manipulate1")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "512m")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("Error")

df = spark.read.option("multiLine", "true").json(
    "s3a://databricks-bucket/nobel_prizes.json"
)

df.show(n=5)
df_transformed = df.select(
    f.col("overallMotivation"),
    f.col("year"),
    f.col("category"),
    f.col("laureates"),
    f.transform(
        f.col("laureates"), lambda x: f.concat(x.firstname, f.lit(" "), x.surname)
    ).alias("laureates_full_name"),
)
df_transformed.show(n=5)

df_deduped = df_transformed.dropDuplicates(["category", "overallMotivation", "year"])
df_deduped.show(n=5)


df_renamed = df_deduped.selectExpr(
    "category as Topic", "year as Year", "overallMotivation as Motivation"
)
df_renamed.show(n=5)
spark.stop()
