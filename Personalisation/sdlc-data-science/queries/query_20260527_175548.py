from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, min, max

# Initialize Spark Session
spark = SparkSession.builder.appName('LoanCustomerTransactions').getOrCreate()

# Load tables
customer_details_df = spark.read.table('customerDetails')
transactions_info_df = spark.read.table('transactionsInfo')
loan_info_df = spark.read.table('loanInfo')

# Filter active home loan customers
home_loan_customers_df = loan_info_df \
    .filter((col('loan_type') == 'Home') & (col('loan_status') == 'Active')) \
    .join(customer_details_df, 'customer_id', 'inner') \
    .select('customer_id', 'first_name', 'last_name', 'credit_score', 'principal_amount')

# Create credit score flag
home_loan_customers_df = home_loan_customers_df \
    .withColumn('credit_score_flag', 
                when(col('credit_score') < 650, 'Risky')
                .when((col('credit_score') >= 651) & (col('credit_score') <= 750), 'Average')
                .when((col('credit_score') >= 751) & (col('credit_score') <= 850), 'Good')
                .otherwsie('Excellent'))

# Create principal amount bucket
home_loan_customers_df = home_loan_customers_df \
    .withColumn('principal_bucket', 
                when(col('principal_amount') < 1000000, 'low')
                .when((col('principal_amount') >= 1000000) & (col('principal_amount') <= 5000000), 'medium')
                .otherwise('high'))

# Count UPI transactions by customer
upi_transactions_df = transactions_info_df \
    .filter(col('channel') == 'UPI') \
    .groupBy('customer_id') \
    .agg(count('transaction_id').alias('upi_transaction_count'))

# Identify UPI inclined customers
upi_inclined_customers_df = upi_transactions_df \
    .filter(col('upi_transaction_count') > 10) \
    .select('customer_id')

# Add UPI inclined flag
loan_customer_transactions_df = home_loan_customers_df \
    .join(upi_inclined_customers_df, 'customer_id', 'left') \
    .withColumn('upi_inclined', when(col('customer_id').isNotNull(), 'Yes').otherwise('No'))

# Show the final DataFrame
loan_customer_transactions_df.show()

# Data Quality Insights
# Assuming primary numeric column for DQ is 'principal_amount'
row_count = loan_customer_transactions_df.count()
null_values = loan_customer_transactions_df.select([count(col).alias(col) for col in loan_customer_transactions_df.columns]).
                        select([sum(col) for col in loan_customer_transactions_df.columns if 'null' in col]).collect()[0][0]
duplicate_rows = loan_customer_transactions_df.groupBy(home_loan_customers_df.columns).count().filter('count > 1').count()
minimum = loan_customer_transactions_df.agg(min('principal_amount')).collect()[0][0]
maximum = loan_customer_transactions_df.agg(max('principal_amount')).collect()[0][0]
average = loan_customer_transactions_df.agg(avg('principal_amount')).collect()[0][0]
distinct_values = loan_customer_transactions_df.distinct().count()
empty_strings = loan_customer_transactions_df.select([count(when(col(c) == '', c)).alias(c) for c in loan_customer_transactions_df.columns]).
                            select([sum(c) for c in loan_customer_transactions_df.columns if 'null' in c]).collect()[0][0]

# Generate insights
 insights = {
    'row_count': row_count,
    'null_values': null_values,
    'duplicate_rows': duplicate_rows,
    'minimum': minimum,
    'maximum': maximum,
    'average': average,
    'distinct_values': distinct_values,
    'empty_strings': empty_strings
}

print(insights)

