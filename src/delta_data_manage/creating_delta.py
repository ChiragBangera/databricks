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
#  CREATE DATABASE IF NOT EXISTS datasets
#  COMMENT "Book datasets"
# """
# )
#
# spark.sql("DROP TABLE IF EXISTS datasets.netflix_titles")
#
# spark.sql(
#    """
#  CREATE OR REPLACE TABLE datasets.netflix_titles(
#  show_id STRING,
#  type STRING,
#  title STRING,
#  director STRING,
#  cast STRING,
#  country STRING,
#  date_added STRING,
#  release_year STRING,
#  rating STRING,
#  duration STRING,
#  listed_in STRING,
#  description STRING
#  ) USING DELTA"""
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


# netflix_titles.write.format("delta").mode("overwrite").saveAsTable(
#     "datasets.netflix_titles"
# )
# print("table-saved>>>>>>")


readback = spark.read.format("delta").load(
    "s3a://delta-lake/warehouse/datasets.db/netflix_titles"
)
readback.show(n=5)

# updating data in Delta Lake table

deltaTable = DeltaTable.forPath(
    spark, "s3a://delta-lake/warehouse/datasets.db/netflix_titles"
)
deltaTable.toDF().show()
deltaTable.update(condition=f.expr("director IS NULL"), set={"director": f.lit("")})
deltaTable.toDF().show()

# merging data into delta tables
# creating empty delta table using the same above table

# spark.sql("DROP TABLE IF EXISTS datasets.movie_and_show_titles")

# spark.sql(
#    """
# CREATE OR REPLACE TABLE datasets.movie_and_show_titles(
# show_id STRING,
# type STRING,
# title STRING,
# director STRING,
# cast STRING,
# country STRING,
# date_added STRING,
# release_year STRING,
# rating STRING,
# duration STRING,
# listed_in STRING,
# description STRING
# ) USING DELTA"""
# )


deltaTable_target = DeltaTable.forPath(
    spark, "s3a://delta-lake/warehouse/datasets.db/movie_and_show_titles"
)
deltaTable_target.toDF().show(5)


# Now merging source dataframe with target DeltaTable
# df_netflix_deduped = readback.dropDuplicates(
#     ["type", "title", "director", "date_added"]
# )

# Why are we dropping duplicates
# - Merge operation only allow single update to each row. The reason for this is that Delta Lake employs a techique called optimistic concurrency control(OCC)
# to protect data quality and avoid clashes between simultaneous transactions. When more than one row is updated with same critirion, there is a chance that
# another write conflict and a transaction might change one of those rows before the MERGE action is finalized causing a write conflict and a tracsaction failure.

# deltaTable_target.alias("movie_and_show_titles").merge(
#     df_netflix_deduped.alias("updates"),
#     """
#     lower(movie_and_show_titles.type) = lower(updates.type)
#     AND
#     lower(movie_and_show_titles.title) = lower(updates.title)
#     AND
#     lower(movie_and_show_titles.director) = lower(updates.director)
#     AND
#     movie_and_show_titles.date_added = updates.date_added
#     """,
# ).whenMatchedUpdate(
#     set={
#         "show_id": "updates.show_id",
#         "type": "updates.type",
#         "title": "updates.title",
#         "director": "updates.director",
#         "cast": "updates.cast",
#         "country": "updates.country",
#         "date_added": "updates.date_added",
#         "release_year": "updates.release_year",
#         "rating": "updates.rating",
#         "duration": "updates.duration",
#         "listed_in": "updates.listed_in",
#         "description": "updates.description",
#     }
# ).whenNotMatchedInsert(
#     values={
#         "show_id": "updates.show_id",
#         "type": "updates.type",
#         "title": "updates.title",
#         "director": "updates.director",
#         "cast": "updates.cast",
#         "country": "updates.country",
#         "date_added": "updates.date_added",
#         "release_year": "updates.release_year",
#         "rating": "updates.rating",
#         "duration": "updates.duration",
#         "listed_in": "updates.listed_in",
#         "description": "updates.description",
#     }
# ).execute()

# deltaTable_merged = (
#     DeltaTable.forPath(
#         sparkSession=spark,
#         path="s3a://delta-lake/warehouse/datasets.db/movie_and_show_titles",
#     )
#     .history()
#     .show()
# )

# df_titles = (
#     spark.read.option("header", "true")
#     .option("multiLine", "true")
#     .csv("s3a://databricks-bucket/titles.csv")
# )

# df_titles_deduped = df_titles.drop_duplicates(["type", "title"])
# df_titles_deduped.show(5)

# deltaTable_target.alias("movie_and_show_titles").merge(
#     df_titles_deduped.alias("updates"),
#     """
#     lower(movie_and_show_titles.type) = lower(updates.type)
#     AND
#     lower(movie_and_show_titles.title) = lower(updates.title)
#     AND
#     movie_and_show_titles.release_year = updates.release_year
#     """,
# ).whenMatchedUpdate(
#     set={
#         "show_id": "updates.id",
#         "type": "updates.type",
#         "title": "updates.title",
#         "country": "updates.production_countries",
#         "release_year": "updates.release_year",
#         "rating": "updates.age_certification",
#         "duration": "updates.runtime",
#         "listed_in": "updates.genres",
#         "description": "updates.description",
#     }
# ).whenNotMatchedInsert(
#     values={
#         "show_id": "updates.id",
#         "type": "updates.type",
#         "title": "updates.title",
#         "country": "updates.production_countries",
#         "release_year": "updates.release_year",
#         "rating": "updates.age_certification",
#         "duration": "updates.runtime",
#         "listed_in": "updates.genres",
#         "description": "updates.description",
#     }
# ).execute()

deltaTable_merged = (
    DeltaTable.forPath(
        sparkSession=spark,
        path="s3a://delta-lake/warehouse/datasets.db/movie_and_show_titles",
    )
    .history()
    .show()
)


updated_titles = spark.read.format("delta").load(
    "s3a://delta-lake/warehouse/datasets.db/movie_and_show_titles"
)
print("reading complete=====>")

updated_titles.write.format("delta").mode("overwrite").saveAsTable(
    "datasets.movie_and_show_titles"
)

spark.sql("SELECT * from datasets.movie_and_show_titles").show(2)
