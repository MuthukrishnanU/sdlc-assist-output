
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HighRiskCustomerAnalysis") \
    .getOrCreate()

# Load tables into DataFrames
account_balances_df = spark.table("accountBalances")
customer_details_df = spark.table("customerDetails")
loan_info_df = spark.table("loanInfo")

# Filter high-risk customers based on kyc_status and credit_score
high_risk_customers_df = customer_details_df \
    .filter((col("kyc_status") == "High") & (col("credit_score") < 600))

# Join the DataFrames to get relevant information
joined_df = high_risk_customers_df \
    .join(account_balances_df, "customer_id") \
    .join(loan_info_df, "customer_id")

# Calculate total transaction volumes for high-risk customers
result_df = joined_df \
    .groupBy("customer_id") \
    .agg(sum(col("available_balance") + col("remaining_balance")).alias("total_transaction_volume"))

# Show results
result_df.show()
