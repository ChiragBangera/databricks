from pyspark.sql import SparkSession
from pyspark.sql import types as t


spark = (
    SparkSession.builder.appName("Spark-MinIO")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

schema = t.StructType(
    [
        t.StructField(name="show_id", dataType=t.StringType(), nullable=True),
        t.StructField(name="type", dataType=t.StringType(), nullable=True),
        t.StructField(name="title", dataType=t.StringType(), nullable=True),
        t.StructField(name="cast", dataType=t.StringType(), nullable=True),
        t.StructField(name="country", dataType=t.StringType(), nullable=True),
        t.StructField(name="date_added", dataType=t.DateType(), nullable=True),
        t.StructField(name="release_year",
                      dataType=t.IntegerType(), nullable=True),
        t.StructField(name="rating", dataType=t.StringType(), nullable=True),
        t.StructField(name="duration", dataType=t.StringType(), nullable=True),
        t.StructField(name="listed_in",
                      dataType=t.StringType(), nullable=True),
        t.StructField(name="description",
                      dataType=t.StringType(), nullable=True),
    ]
)

df = (
    spark.read.option("header", "true")
    .schema(schema=schema)
    .csv("s3a://databricks-bucket/netflix_titles.csv")
)
df.show(n=10)
spark.stop()
