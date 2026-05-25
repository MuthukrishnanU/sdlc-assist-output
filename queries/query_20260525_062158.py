
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName('SDLC_Automation').getOrCreate()

# Load DataFrames
accountBalances = spark.table('accountBalances')
transactionsInfo = spark.table('transactionsInfo')
customerDetails = spark.table('customerDetails')

# Join tables to get the necessary columns
result_df = accountBalances.alias('a') \
    .join(transactionsInfo.alias('t'), col('a.customer_id') == col('t.customer_id'), 'inner') \
    .join(customerDetails.alias('c'), col('t.customer_id') == col('c.customer_id'), 'inner') \
    .select('a.customer_id', 'a.available_balance', 'c.credit_score')

# Filter based on the conditions
filtered_df = result_df.filter((col('available_balance') > 100000) & (col('credit_score') > 600))

# Show the result
filtered_df.show()

# For DQ insights
row_count = filtered_df.count()
null_values = filtered_df.select([col(c).isNull().cast('int').alias(c) for c in filtered_df.columns]).agg(sum(c) for c in filtered_df.columns).first()[0]
duplicate_rows = filtered_df.groupBy(filtered_df.columns).count().filter(col('count') > 1).count()
minimum = filtered_df.agg({'available_balance': 'min'}).first()[0]
maximum = filtered_df.agg({'available_balance': 'max'}).first()[0]
average = filtered_df.agg({'available_balance': 'avg'}).first()[0]

print({'row_count': row_count, 'null_values': null_values, 'duplicate_rows': duplicate_rows, 'minimum': minimum, 'maximum': maximum, 'average': average})
