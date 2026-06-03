from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, sum as spark_sum, count as spark_count, when

# Initialize Spark session
spark = SparkSession.builder.appName('TransactionAnalysis').getOrCreate()

# Load the transactionsInfo table
transactions_df = spark.read.format('parquet').load('path_to_transactionsInfo')

# Extract year and month from timestamp
transactions_df = transactions_df.withColumn('year', year(col('timestamp')))
transactions_df = transactions_df.withColumn('month', month(col('timestamp')))

# Aggregate data by year, month, and channel
aggregated_df = transactions_df.groupBy('year', 'month', 'channel') \
    .agg(
        spark_count('*').alias('total_transactions'),
        spark_sum('amount').alias('total_amount')
    )

# Add category based on transaction count
aggregated_df = aggregated_df.withColumn(
    'category',
    when(col('total_transactions') > 100, 'Progressive Channel').otherwise('Regular Channel')
)

# Show the result
aggregated_df.show()