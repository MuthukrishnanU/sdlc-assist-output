from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load the transactionsInfo table
transactions_df = spark.table('transactionsInfo')

# Extract year and month from the timestamp
transactions_df = transactions_df.withColumn('year_month', F.date_format('timestamp', 'yyyy-MM'))

# Group by year_month and channel to calculate required metrics
aggregated_df = transactions_df.groupBy('year_month', 'channel').agg(
    F.count('transaction_id').alias('total_transaction_count'),
    F.sum('amount').alias('total_amount'),
    F.countDistinct('customer_id').alias('unique_customer_count')
)

# Add a category column based on the total transaction count
result_df = aggregated_df.withColumn(
    'category',
    F.when(F.col('total_transaction_count') > 100, 'Progressive Channel').otherwise('')
)

# Select the final columns
result_df = result_df.select(
    'year_month',
    'channel',
    'total_transaction_count',
    'total_amount',
    'unique_customer_count',
    'category'
)

# Show the result
result_df.show()