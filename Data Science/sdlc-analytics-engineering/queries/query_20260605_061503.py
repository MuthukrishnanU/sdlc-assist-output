from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as spark_sum, lit
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("LoanCustomerTransactions").getOrCreate()

# Read tables
customerDetails = spark.table("customerDetails")
accountBalances = spark.table("accountBalances")
loanInfo = spark.table("loanInfo")
transactionsInfo = spark.table("transactionsInfo")

# Filter for Home Loan and Active status
home_loans = loanInfo.filter(
    (col("loan_type") == "Home") & (col("loan_status") == "Active")
)

# Join with customerDetails to get customer names
customer_loans = home_loans.join(
    customerDetails,
    "customer_id",
    "inner"
).select(
    "customer_id",
    "first_name",
    "last_name",
    "loan_type",
    "loan_status",
    "credit_score",
    "principal_amount"
)

# Create credit score flag
customer_loans = customer_loans.withColumn(
    "credit_score_flag",
    when(col("credit_score") < 650, "Risky")
    .when((col("credit_score") >= 651) & (col("credit_score") <= 750), "Average")
    .when((col("credit_score") > 750) & (col("credit_score") <= 850), "Good")
    .when(col("credit_score") > 850, "Excellent")
    .otherwise("Unknown")
)

# Create principal amount bucket
customer_loans = customer_loans.withColumn(
    "principal_amount_bucket",
    when(col("principal_amount") < 1000000, "low")
    .when((col("principal_amount") >= 1000000) & (col("principal_amount") <= 5000000), "medium")
    .when(col("principal_amount") > 5000000, "high")
    .otherwise("Unknown")
)

# Deduplicate transactionsInfo to avoid row duplication
window_spec = Window.partitionBy("customer_id").orderBy(col("timestamp").desc())
deduped_transactions = transactionsInfo.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")

# Join with deduplicated transactions to get transaction details and UPI count
customer_transactions = customer_loans.join(
    deduped_transactions,
    "customer_id",
    "left"
).select(
    "customer_id",
    "first_name",
    "last_name",
    "merchant_name",
    "loan_type",
    "loan_status",
    "credit_score",
    "principal_amount",
    "transaction_type",
    "channel"
)

# Calculate UPI transaction count per customer using window function
window_spec = Window.partitionBy("customer_id")
customer_transactions = customer_transactions.withColumn(
    "upi_transaction_count",
    spark_sum(when(col("channel") == "UPI", 1).otherwise(0)).over(window_spec)
)

# Create UPI inclined flag
customer_transactions = customer_transactions.withColumn(
    "upi_inclined_flag",
    when(col("upi_transaction_count") > 10, "UPI inclined").otherwise("Not UPI inclined")
)

# Drop duplicates to ensure one row per customer
loan_customer_transactions = customer_transactions.dropDuplicates(["customer_id"])

# Select final columns
loan_customer_transactions = loan_customer_transactions.select(
    "customer_id",
    "first_name",
    "last_name",
    "merchant_name",
    "loan_type",
    "loan_status",
    "credit_score",
    "credit_score_flag",
    "principal_amount",
    "principal_amount_bucket",
    "transaction_type",
    "channel",
    "upi_inclined_flag"
)

# Write output (optional, for verification)
loan_customer_transactions.write.mode("overwrite").saveAsTable("loan_customer_transactions")