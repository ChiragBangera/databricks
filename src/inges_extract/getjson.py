from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    get_json_object,
    json_tuple,
    collect_list,
    flatten,
)
from pyspark.sql.types import StringType


spark: SparkSession = (
    SparkSession.builder.appName(name="getJson")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.haddop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.executor.memory", "512m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel(logLevel="ERROR")


df: DataFrame = spark.createDataFrame(
    [(1, '{"name": "Alice", "age": 25}'), (2, '{"name": "Bob", "age": 30}')],
    ["id", "json_data"],
)

# Extract the name field from the JSON string column
name_df = df.select(get_json_object("json_data", "$.name").alias("name"))

name_str_df = name_df.withColumn(
    "name_str", name_df["name"].cast(dataType=StringType())
)

name_age_df: DataFrame = df.select(
    json_tuple("json_data", "name", "age").alias("name", "age")
)


df_arrays: DataFrame = spark.createDataFrame(
    [(1, [[1, 2], [3, 4], [5, 6]]), (2, [[7, 8], [9, 10], [11, 12]])], ["id", "data"]
)

collect_list_df: DataFrame = df_arrays.select(collect_list("data").alias("data"))

name_str_df.show()
df.show()
name_age_df.show()
df_arrays.show()
collect_list_df.show()
