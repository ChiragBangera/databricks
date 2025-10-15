from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t

spark = (
    SparkSession.builder.appName("filtering")
    .master("spark://spark-master:7077")
    .config("spark.executor.memeory", "512m")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


netflixschema = t.StructType(
    [
        t.StructField("show_id", t.StringType(), True),
        t.StructField("type", t.StringType(), True),
        t.StructField("title", t.StringType(), True),
        t.StructField("director", t.StringType(), True),
        t.StructField("cast", t.StringType(), True),
        t.StructField("country", t.StringType(), True),
        t.StructField("date_added", t.StringType(), True),
        t.StructField("release_year", t.IntegerType(), True),
        t.StructField("rating", t.StringType(), True),
        t.StructField("duration", t.StringType(), True),
        t.StructField("listed_in", t.StringType(), True),
        t.StructField("description", t.StringType(), True),
    ]
)

df = (
    spark.read.option("header", "true")
    .option("nullValues", "null")
    .option("multiLine", "true")
    .option("escape", '"')
    .schema(netflixschema)
    .csv("s3a://databricks-bucket/netflix_titles.csv")
)

df = df.withColumn("date_added", f.to_date(f.col("date_added"), "MMMM d, yyyy"))
df.show(n=5)

filtered_data = df.filter(f.col("release_year") > 2020)
filtered_data.show(n=5)

filtered_data = df.filter(
    (f.col("country") == "United States") & (f.col("release_year") > 2020)
)
filtered_data.show(n=5)


filtered_df = df.filter(
    f.col("country").isin(["United States", "United Kingdom", "India"])
)
filtered_df.show(n=5)


# filter data using regular expression
filtered_df = df.filter(f.col("listed_in").rlike("(Crime|Thriller)"))
filtered_df.show(n=5)


# filter date range
filtered_df = df.filter(f.col("date_added").between("2021-02-01", "2021-03-01"))
filtered_df.show()


# filter on arrays
df_recipes = spark.read.parquet("s3a://databricks-bucket/recipes.parquet")
df_recipes.select(f.col("RecipeIngredientParts")).show()

filtered_df = df_recipes.filter(
    f.array_contains(f.col("RecipeIngredientParts"), "apple")
)
