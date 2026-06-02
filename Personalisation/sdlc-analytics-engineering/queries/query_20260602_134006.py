from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName('LoanCustomerTransactions').getOrCreate()

# Load tables
accountBalances = spark.read.table('accountBalances')
loanInfo = spark.read.table('loanInfo')
transactionsInfo = spark.read.table('transactionsInfo')
customerDetails = spark.read.table('customerDetails')

# Filter for active home loans
active_home_loans = loanInfo.filter((col('loan_type') == 'Home') & (col('loan_status') == 'Active'))

# Join with customer details
customer_loans = active_home_loans.join(customerDetails, 'customer_id')

# Add credit score flag
customer_loans = customer_loans.withColumn('credit_score_flag',
    when(col('credit_score') < 650, 'Risky')
    .when((col('credit_score') >= 651) & (col('credit_score') <= 750), 'Average')
    .when((col('credit_score') > 750) & (col('credit_score') <= 850), 'Good')
    .otherwise('Excellent'))

# Add principal amount bucket
customer_loans = customer_loans.withColumn('principal_bucket',
    when(col('principal_amount') < 1000000, 'low')
    .when((col('principal_amount') >= 1000000) & (col('principal_amount') <= 5000000), 'medium')
    .otherwise('high'))

# Join with transactions info
customer_transactions = customer_loans.join(transactionsInfo, 'customer_id', 'left')

# Calculate UPI transaction count per customer
windowSpec = Window.partitionBy('customer_id')
customer_transactions = customer_transactions.withColumn('upi_transaction_count',
    count(when(col('channel') == 'UPI', True)).over(windowSpec))

# Add UPI inclined flag
customer_transactions = customer_transactions.withColumn('upi_inclined',
    when(col('upi_transaction_count') > 10, lit('Yes')).otherwise(lit('No')))

# Select final columns
final_df = customer_transactions.select(
    'customer_id', 'first_name', 'last_name', 'credit_score', 'credit_score_flag',
    'loan_type', 'loan_status', 'principal_bucket', 'transaction_type', 'merchant_name',
    'channel', 'upi_inclined'
)

# Show the result
final_df.show()