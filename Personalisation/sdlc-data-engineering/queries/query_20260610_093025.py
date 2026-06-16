from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName('Loan Data').getOrCreate()

# Load the data into DataFrames
customerDetails = spark.read.csv('customerDetails.csv', header=True, inferSchema=True)
loanInfo = spark.read.csv('loanInfo.csv', header=True, inferSchema=True)

# Join the DataFrames on customer_id
joined_df = customerDetails.join(loanInfo, 'customer_id', 'inner')

# Filter for active home loans
active_home_loans = joined_df.filter((loanInfo.loan_type == 'Home') & (loanInfo.loan_status == 'Active'))

# Select the required columns
result_df = active_home_loans.select('customer_id', 'first_name', 'last_name', 'loan_id', 'loan_type', 'loan_status')

# Show the results
result_df.show()