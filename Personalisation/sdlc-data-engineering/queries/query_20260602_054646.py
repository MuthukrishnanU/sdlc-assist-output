
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, sum, count

# Initialize Spark Session
spark = SparkSession.builder.appName("SDLC_Automation").getOrCreate()

# Load data into DataFrame
df_transactions = spark.read.format("delta").load("/path/to/transactionsInfo")

# Aggregating data by year, month, and channel
yearly_monthly_totals = df_transactions \
    .groupBy(year("timestamp").alias("year"), month("timestamp").alias("month"), "channel") \
    .agg(
        sum("amount").alias("total_amount"),
        count("transaction_id").alias("transaction_count")
    )

# Print the result
yearly_monthly_totals.show()
