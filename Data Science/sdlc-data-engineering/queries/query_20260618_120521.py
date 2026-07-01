
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count
from pyspark.sql.window import Window

# Assuming spark session is already created
# spark = SparkSession.builder.appName('LoanCustomerTransactions').getOrCreate()

# Load the tables
customerDetails_df = spark.table('customerDetails')
loanInfo_df = spark.table('loanInfo')
transactionsInfo_df = spark.table('transactionsInfo')

# Filter for Home Loan and Active status
home_loan_active_df = loanInfo_df.filter((col('loan_type') == 'Home') & (col('loan_status') == 'Active'))

# Join with customerDetails to get customer names
customer_loans_df = home_loan_active_df.join(customerDetails_df, 'customer_id', 'left')

# Create credit score flag
customer_loans_df = customer_loans_df.withColumn(
    'credit_score_flag',
    when(col('credit_score') < 650, 'Risky')
    .when((col('credit_score') >= 651) & (col('credit_score') <= 750), 'Average')
    .when((col('credit_score') > 750) & (col('credit_score') <= 850), 'Good')
    .otherwise('Excellent')
)

# Create principal amount bucket
customer_loans_df = customer_loans_df.withColumn(
    'principal_amount_bucket',
    when(col('principal_amount') < 1000000, 'low bucket')
    .when((col('principal_amount') >= 1000000) & (col('principal_amount') <= 5000000), 'medium bucket')
    .otherwise('high bucket')
)

# Count UPI transactions per customer
upi_transactions_df = transactionsInfo_df.filter(col('channel') == 'UPI')
upi_count_df = upi_transactions_df.groupBy('customer_id').agg(count('*').alias('upi_transaction_count'))

# Flag UPI inclined customers
upi_inclined_df = upi_count_df.withColumn(
    'upi_inclined',
    when(col('upi_transaction_count') > 10, True).otherwise(False)
)

# Join UPI inclined flag with customer loans
final_df = customer_loans_df.join(upi_inclined_df, 'customer_id', 'left')

# Fill null values in upi_inclined with False
final_df = final_df.fillna({'upi_inclined': False})

# Select required columns
result_df = final_df.select(
    'customer_id', 'first_name', 'last_name', 'merchant_name', 'loan_type', 'loan_status',
    'credit_score', 'principal_amount', 'transaction_type', 'channel',
    'credit_score_flag', 'principal_amount_bucket', 'upi_inclined'
)

# Deduplicate the final DataFrame
result_df = result_df.dropDuplicates(['customer_id'])

# Show the result
result_df.show()
