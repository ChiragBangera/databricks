from pyspark.ml.feature import CountVectorizer, StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark: SparkSession = (
    SparkSession.builder.appName("processing-text-data")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.executor.memory", "512m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


df = (
    spark.read.option("header", "true")
    .option("multiLine", "true")
    .csv("s3a://databricks-bucket/Reviews.csv")
)

df_clean = df.withColumn(
    "Text", f.regexp_replace(f.col("Text"), "<[^>]*>|[^a-zA-z ]", "")
).withColumn("Text", f.regexp_replace(f.col("Text"), " +", " "))


tokenizer = Tokenizer(inputCol="Text", outputCol="words")
df_words = tokenizer.transform(df_clean)
df_words.show()


remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
df_stop_words = remover.transform(df_words)

df_exploded = df_stop_words.select(f.explode(f.col("filtered_words")).alias("word"))
word_counts = df_exploded.groupBy("word").count().orderBy("count", ascending=False)
word_counts.show(n=50)


vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="features")
vectorized_data = vectorizer.fit(df_stop_words).transform(df_stop_words)
vectorized_data.show(n=50, truncate=False)


vectorized_data.repartition(1).write.mode("overwrite").json(
    "s3a://databricks-bucket/reviews_vectorized.json"
)
