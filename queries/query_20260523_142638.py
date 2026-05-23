from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Initialize Spark session
spark = SparkSession.builder.appName("HighRiskCustomers").getOrCreate()

# Load tables
df_account_balances = spark.read.table("accountBalances")
df_transactions_info = spark.read.table("transactionsInfo")
df_customer_details = spark.read.table("customerDetails")
df_loan_info = spark.read.table("loanInfo")

# Identify high-risk customers, assuming 'status' in loanInfo implies risk where 'status' = 'high-risk'
high_risk_customers = df_loan_info.filter(df_loan_info.status == 'high-risk').select("customer_id")

# Join tables to get transactions of high-risk customers
high_risk_transactions = df_transactions_info \
    .join(high_risk_customers, on="customer_id")

# Calculate total transaction volumes
high_risk_volumes = high_risk_transactions.groupBy("customer_id").agg(sum("amount").alias("total_volume"))

# Show results
high_risk_volumes.show()