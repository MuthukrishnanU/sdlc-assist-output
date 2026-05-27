
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Initialize Spark session
spark = SparkSession.builder.appName('LoanCustomerAnalysis').getOrCreate()

# Load tables
loan_info_df = spark.read.table('loanInfo')
customer_details_df = spark.read.table('customerDetails')
transactions_info_df = spark.read.table('transactionsInfo')

# Filter customers with Home Loan and Active status
home_loan_customers_df = loan_info_df \
    .filter((col('loan_type') == 'Home Loan') & (col('loan_status') == 'Active'))

# Join with customer details to get customer name
loan_customer_df = home_loan_customers_df \
    .join(customer_details_df, on='customer_id', how='inner') \
    .select('customer_id', 'first_name', 'last_name', 'principal_amount', 'credit_score')

# Create a credit score flag
loan_customer_df = loan_customer_df \
    .withColumn('credit_score_flag', 
                when(col('credit_score') < 650, 'Risky')
                .when((col('credit_score') >= 651) & (col('credit_score') <= 750), 'Average')
                .when((col('credit_score') > 750) & (col('credit_score') <= 850), 'Good')
                .when(col('credit_score') > 850, 'Excellent'))

# Create a principal amount bucket flag
loan_customer_df = loan_customer_df \
    .withColumn('principal_amount_bucket',
                when(col('principal_amount') < 1000000, 'Low Bucket')
                .when((col('principal_amount') >= 1000000) & (col('principal_amount') <= 5000000), 'Medium Bucket')
                .when(col('principal_amount') > 5000000, 'High Bucket'))

# Identify UPI inclined customers
upi_transactions_df = transactions_info_df \
    .filter(col('transaction_type') == 'UPI') \
    .groupBy('customer_id') \
    .agg(count('transaction_type').alias('upi_transaction_count')) \
    .filter(col('upi_transaction_count') > 10)

# Add the UPI inclined flag to the main dataset
loan_customer_transactions_df = loan_customer_df \
    .join(upi_transactions_df.select('customer_id'), on='customer_id', how='left') \
    .withColumn('upi_inclined', when(col('customer_id').isNotNull(), 'Yes').otherwise('No'))

# Display the final dataframe
loan_customer_transactions_df.show()

# Clean up resources
spark.stop()
