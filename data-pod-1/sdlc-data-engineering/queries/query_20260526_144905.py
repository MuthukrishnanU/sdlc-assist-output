from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName('LoanQuery').getOrCreate()

# Load the customerDetails and loanInfo tables
df_customer = spark.read.format('csv').option('header', 'true').load('customerDetails.csv')
df_loan = spark.read.format('csv').option('header', 'true').load('loanInfo.csv')

# Join the dataframes on customer_id to get necessary fields
df_joined = df_customer.join(df_loan, df_customer.customer_id == df_loan.customer_id, 'inner')

# Filter to get customers with active auto loans
df_filtered = df_joined.filter((col('loan_status') == 'Active') & (col('loan_type') == 'auto loan'))

# Select the relevant customer details
df_result = df_filtered.select('customer_id', 'first_name', 'last_name', 'kyc_status')

# Show the result
df_result.show()