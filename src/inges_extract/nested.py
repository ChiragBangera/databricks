from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark: SparkSession = (
    SparkSession.builder.appName("nested-structures")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.executor.memory", "512m")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

stanford_question = spark.read.option("multiLine", "true").json(
    "s3a://databricks-bucket/Stanford Question Answering Dataset.json"
)

question_exploded = stanford_question.select(
    f.col("title"), f.explode(f.col("paragraphs")).alias("paragraphs")
).select(
    f.col("title"),
    f.col("paragraphs").getField("context").alias("context"),
    f.explode(f.col("paragraphs").getField("qas")).alias("questions"),
)


df_array_distinct = question_exploded.select(
    f.col("title"),
    f.col("context"),
    f.col("questions.id").alias("question_id"),
    f.col("questions.question").alias("question_text"),
    f.array_distinct(f.col("questions.answers")).alias("answers"),
)

df_array_distinct.show()

df_array_distinct.filter(
    f.col("answers").getItem(0).getField("text").isNotNull()
).show()


# array_contains function
df = spark.createDataFrame(
    [
        (["apple", "orange", "banana"],),
        (["grape", "kiwi", "melon"],),
        (["pear", "apple", "pineapple"],),
    ],
    ["fruits"],
)


df.select("fruits").show()


df.select(
    f.col("fruits"), f.array_contains(f.col("fruits"), "apple").alias("contains_apple")
).show(truncate=False)


# map_keys and map_values functions
data = [
    {"user_info": {"name": "Alice", "age": 28, "email": "alice@example.com"}},
    {"user_info": {"name": "Bob", "age": 35, "email": "bob@example.com"}},
    {"user_info": {"name": "Charlie", "age": 42, "email": "charlie@example.com"}},
]

df = spark.createDataFrame(data)
df.show(truncate=False)


df.select(
    f.col("user_info"),
    f.map_keys(f.col("user_info")).alias("extracted_keys"),
    f.map_values(f.col("user_info")).alias("extracted_values"),
).show()


# outer_explode function
data = [
    {"words": ["hello", "world"]},
    {"words": ["foo", "bar", "baz"]},
    {"words": None},
]
df = spark.createDataFrame(data)
(df.select(f.explode_outer("words").alias("word")).show(truncate=False))
