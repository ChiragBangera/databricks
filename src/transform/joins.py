from math import trunc
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .config("spark.executor.memory", "512m")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .appName("joins")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


csv_reader_config = {
    "header": "true",
    "multiLine": "true",
    "nullValues": "null",
    "escape": '"',
}

cards_df = spark.read.options(**csv_reader_config).csv(
    "s3a://databricks-bucket/Credit Card/CardBase.csv"
)

customers_df = spark.read.options(**csv_reader_config).csv(
    "s3a://databricks-bucket/Credit Card/CustomerBase.csv"
)

transactions_df = spark.read.options(**csv_reader_config).csv(
    "s3a://databricks-bucket/Credit Card/TransactionBase.csv"
)

fruad_df = spark.read.options(**csv_reader_config).csv(
    "s3a://databricks-bucket/Credit Card/FraudBase.csv"
)

# inner join
customer_cards_df = cards_df.join(customers_df, on="Cust_ID", how="inner")
customer_cards_df.show(n=5, truncate=False)


# left outer join
joined_transactions_df = transactions_df.join(
    fruad_df, on="Transaction_ID", how="left_outer"
)
joined_transactions_df.show(n=5, truncate=False)


joined_transactions_df.filter(f.col("Fraud_Flag").isNotNull()).show()


# joins with complex conditions


joinExpr = (
    customer_cards_df["Card_Number"] == joined_transactions_df["Credit_Card_ID"]
) & (joined_transactions_df["Fraud_Flag"].isNotNull())

customer_with_fraud_df = customer_cards_df.join(
    joined_transactions_df, on=joinExpr, how="inner"
)

customer_with_fraud_df.show(truncate=False)
