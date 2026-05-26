
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit

# Initialize Spark session
spark = SparkSession.builder.appName("LoanCustomerAnalysis").getOrCreate()

# Load tables
accountBalances = spark.table('accountBalances')
transactionsInfo = spark.table('transactionsInfo')
customerDetails = spark.table('customerDetails')
loanInfo = spark.table('loanInfo')

# Loan Type filtering and joining with customers
df_loan_customers = loanInfo.join(customerDetails, loanInfo.customer_id == customerDetails.customer_id)
df_loan_customers = df_loan_customers.filter((col('loan_type') == 'Home Loan') & (col('loan_status') == 'Active'))

# Add credit score flag
df_loan_customers = df_loan_customers.withColumn('credit_score_flag', 
    when(col('credit_score') < 651, 'Risky')
    .when((col('credit_score') >= 651) & (col('credit_score') <= 750), 'Average')
    .when((col('credit_score') > 750) & (col('credit_score') <= 850), 'Good')
    .otherwise('Excellent'))

# Add principal amount bucket
df_loan_customers = df_loan_customers.withColumn('principal_bucket', 
    when(col('principal_amount') < 1000000, 'low')
    .when((col('principal_amount') >= 1000000) & (col('principal_amount') <= 5000000), 'medium')
    .otherwise('high'))

# UPI transactions flag
upi_transactions = transactionsInfo.filter(col('transaction_type') == 'UPI')
upi_transactions_count = upi_transactions.groupBy('customer_id').agg(count('*').alias('upi_transaction_count'))

# Flag UPI inclined customers
df_upi_inclined = upi_transactions_count.withColumn('upi_inclined', 
    when(col('upi_transaction_count') > 10, lit('Yes')).otherwise(lit('No')))

# Join the UPI inclined flag to the main dataset
df_final = df_loan_customers.join(df_upi_inclined, 'customer_id', 'left')

# Fill null values in 'upi_inclined'
df_final = df_final.withColumn('upi_inclined', when(col('upi_inclined').isNull(), lit('No')).otherwise(col('upi_inclined')))

# Select final columns
df_final = df_final.select('customer_id', 'first_name', 'last_name', 'credit_score_flag', 'principal_bucket', 'upi_inclined')

# Register temporary view
df_final.createOrReplaceTempView('loan_customer_transactions')

