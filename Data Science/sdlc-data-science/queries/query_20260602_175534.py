from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit, row_number, max as spark_max
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("LoanCustomerTransactions").getOrCreate()

# Load tables
customerDetails = spark.table("customerDetails")
loanInfo = spark.table("loanInfo")
transactionsInfo = spark.table("transactionsInfo")

# Step 1: Filter active home loans and join with customer details
customer_loans = customerDetails.join(
    loanInfo,
    "customer_id",
    "inner"
).filter(
    (col("loan_type") == "Home") & (col("loan_status") == "Active")
)

# Step 2: Create credit score flag
customer_loans = customer_loans.withColumn(
    "credit_score_flag",
    when(col("credit_score") < 650, "Risky")
    .when((col("credit_score") >= 651) & (col("credit_score") <= 750), "Average")
    .when((col("credit_score") >= 750) & (col("credit_score") <= 850), "Good")
    .when(col("credit_score") > 850, "Excellent")
    .otherwise("Unknown")
)

# Step 3: Create principal amount bucket
customer_loans = customer_loans.withColumn(
    "principal_bucket",
    when(col("principal_amount") < 1000000, "low bucket")
    .when((col("principal_amount") >= 1000000) & (col("principal_amount") <= 5000000), "medium bucket")
    .when(col("principal_amount") > 5000000, "high bucket")
    .otherwise("Unknown")
)

# Step 4: Calculate UPI transaction count per customer using window function
# We need to join transactionsInfo at row level to preserve merchant_name, transaction_type, channel
# But first calculate UPI count per customer using window

# Add UPI indicator
transactions_with_upi = transactionsInfo.withColumn(
    "is_upi",
    when((col("channel") == "UPI") & (col("status") == "Success"), 1).otherwise(0)
)

# Calculate UPI count per customer using window
window_spec = Window.partitionBy("customer_id")
transactions_with_upi = transactions_with_upi.withColumn(
    "upi_transaction_count",
    count("is_upi").over(window_spec)  # This counts all rows per customer, not just UPI
)

# Actually, we need count of UPI transactions per customer, not count of all rows
# Let's use sum of is_upi instead
transactions_with_upi = transactions_with_upi.withColumn(
    "upi_transaction_count",
    count(when(col("is_upi") == 1, 1)).over(window_spec)  # This won't work directly
)

# Better approach: use sum of is_upi
transactions_with_upi = transactionsInfo.withColumn(
    "is_upi",
    when((col("channel") == "UPI") & (col("status") == "Success"), 1).otherwise(0)
).withColumn(
    "upi_transaction_count",
    sum("is_upi").over(window_spec)
)

# Create UPI inclined flag
transactions_with_upi = transactions_with_upi.withColumn(
    "upi_inclined_flag",
    when(col("upi_transaction_count") > 10, "UPI inclined customer").otherwise("Non UPI inclined customer")
)

# Step 5: Join transactions at row level (left join to keep all customers even if no transactions)
# But we need to deduplicate to prevent row explosion
# Use row_number to get latest transaction per customer
window_dedup = Window.partitionBy("customer_id").orderBy(col("timestamp").desc())
transactions_dedup = transactions_with_upi.withColumn(
    "rn",
    row_number().over(window_dedup)
).filter(col("rn") == 1).drop("rn")

final_df = customer_loans.join(
    transactions_dedup,
    "customer_id",
    "left"
)

# Step 6: Select all required columns plus computed columns
# Required columns: customer_id, first_name, last_name, merchant_name, loan_type, loan_status, credit_score, principal_amount, transaction_type, channel
# Computed columns: credit_score_flag, principal_bucket, upi_inclined_flag

loan_customer_transactions = final_df.select(
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("merchant_name"),
    col("loan_type"),
    col("loan_status"),
    col("credit_score"),
    col("principal_amount"),
    col("transaction_type"),
    col("channel"),
    col("credit_score_flag"),
    col("principal_bucket"),
    col("upi_inclined_flag")
).dropDuplicates(["customer_id"])

# Write output
loan_customer_transactions.write.mode("overwrite").saveAsTable("loan_customer_transactions")

# Show sample
loan_customer_transactions.show(10, truncate=False)
