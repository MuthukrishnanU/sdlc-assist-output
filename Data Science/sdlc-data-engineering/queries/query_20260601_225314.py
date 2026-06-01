from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, count, sum as spark_sum, concat_ws

spark = SparkSession.builder.appName("TransactionAggregation").getOrCreate()

# Load transactionsInfo table
transactions_df = spark.table("transactionsInfo")

# Extract month-year from timestamp and aggregate monthwise, channelwise
result_df = (transactions_df
    .withColumn("transaction_month", concat_ws("-", year(col("timestamp")), month(col("timestamp"))))
    .groupBy("transaction_month", "channel")
    .agg(
        spark_sum("amount").alias("total_transaction_amount"),
        count("transaction_id").alias("transaction_count")
    )
    .select(
        "transaction_month",
        "channel",
        "total_transaction_amount",
        "transaction_count"
    )
    .orderBy("transaction_month", "channel")
)

result_df.show(truncate=False)
