from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    ArrayType,
    DoubleType,
)

spark: SparkSession = (
    SparkSession.builder.appName("readjson")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.executor.memory", "512m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

schema: StructType = StructType(
    fields=[
        StructField(name="category", dataType=StringType(), nullable=True),
        StructField(
            name="laureates",
            dataType=ArrayType(
                elementType=StructType(
                    fields=[
                        StructField(
                            name="firstname", dataType=StringType(), nullable=True
                        ),
                        StructField(name="id", dataType=StringType(), nullable=True),
                        StructField(
                            name="motivation", dataType=StringType(), nullable=True
                        ),
                        StructField(name="share", dataType=StringType(), nullable=True),
                        StructField(
                            name="surname", dataType=StringType(), nullable=True
                        ),
                    ]
                )
            ),
            nullable=True,
        ),
        StructField(name="overallMotivation", dataType=StringType(), nullable=True),
        StructField(name="year", dataType=DoubleType(), nullable=True),
    ]
)
jsondata: DataFrame = (
    spark.read.option(key="multiLine", value="true")
    .option(key="mode", value="PERMISSIVE")
    .option(key="columnNameOfCorruptRecord", value="corrupt_record")
    .json(path="s3a://databricks-bucket/nobel_prizes.json")
)

jsondata.printSchema()
jsondata.show(n=10)

jsondata_flat = jsondata.withColumn("laureates", explode(col("laureates"))).select(
    col("category"),
    col("overallMotivation"),
    col("laureates.id"),
    col("laureates.firstname"),
    col("laureates.surname"),
    col("laureates.share"),
    col("laureates.motivation"),
)

jsondata_flat.show(n=10)
