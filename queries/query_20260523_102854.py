from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("LoanTypeAutoFilter").getOrCreate()

# Load the data tables
accountBalances = spark.read.format("parquet").load("path/to/accountBalances")
customerDetails = spark.read.format("parquet").load("path/to/customerDetails")
loanInfo = spark.read.format("parquet").load("path/to/loanInfo")

# Perform the join and filter operations
auto_loan_customers = loanInfo \
    .filter(col("loan_type") == "Auto") \
    .join(customerDetails, loanInfo.customer_id == customerDetails.customer_id) \
    .join(accountBalances, loanInfo.account_id == accountBalances.account_id) \
    .select(customerDetails.customer_id, accountBalances.remaining_balance)

# Show the result
auto_loan_customers.show()