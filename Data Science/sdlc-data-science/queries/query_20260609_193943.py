from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit, row_number, max as spark_max
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("LoanCustomerTransactions").getOrCreate()

# Load sample data (1000 rows context)
customerDetails = spark.table("customerDetails")
accountBalances = spark.table("accountBalances")
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
    "principal_amount_bucket",
    when(col("principal_amount") < 1000000, "low bucket")
    .when((col("principal_amount") >= 1000000) & (col("principal_amount") <= 5000000), "medium bucket")
    .when(col("principal_amount") > 5000000, "high bucket")
    .otherwise("Unknown")
)

# Step 4: Calculate UPI transaction count per customer using window function
# We need to join transactionsInfo at row level to preserve transaction columns
# Use window function to count UPI transactions per customer without collapsing rows

upi_window = Window.partitionBy("customer_id")

# First, calculate UPI count per customer in a separate DataFrame to avoid duplication issues
upi_counts = transactionsInfo.filter(col("channel") == "UPI") \
    .groupBy("customer_id") \
    .agg(count("*").alias("upi_transaction_count"))

# Create UPI inclined flag
upi_counts = upi_counts.withColumn(
    "upi_inclined_flag",
    when(col("upi_transaction_count") > 10, "UPI Inclined").otherwise("Not UPI Inclined")
)

# Step 5: Join customer_loans with upi_counts (left join to keep all loan customers)
customer_loans_upi = customer_loans.join(
    upi_counts,
    "customer_id",
    "left"
).withColumn(
    "upi_inclined_flag",
    when(col("upi_inclined_flag").isNull(), "Not UPI Inclined").otherwise(col("upi_inclined_flag"))
)

# Step 6: Join with transactionsInfo at row level to get transaction details
# Deduplicate transactionsInfo to prevent row duplication using row_number

transactions_window = Window.partitionBy("customer_id").orderBy(col("timestamp").desc())

deduped_transactions = transactionsInfo.withColumn(
    "rn",
    row_number().over(transactions_window)
).filter(col("rn") == 1).drop("rn")

final_df = customer_loans_upi.join(
    deduped_transactions,
    "customer_id",
    "left"
)

# Step 7: Select all required columns and computed columns
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
    col("principal_amount_bucket"),
    col("upi_inclined_flag"),
    col("upi_transaction_count")
).dropDuplicates(["customer_id"])

# Step 8: Write output
loan_customer_transactions.write.mode("overwrite").saveAsTable("loan_customer_transactions")

# Optional: Show sample
loan_customer_transactions.show(10, truncate=False)
