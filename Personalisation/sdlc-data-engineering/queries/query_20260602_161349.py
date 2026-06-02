from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit, row_number, max as spark_max
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("LoanCustomerTransactions").getOrCreate()

# Read tables
customerDetails = spark.table("customerDetails")
loanInfo = spark.table("loanInfo")
transactionsInfo = spark.table("transactionsInfo")

# Step 1: Filter active home loans and join with customer details
active_home_loans = loanInfo.filter(
    (col("loan_type") == "Home") & (col("loan_status") == "Active")
)

customer_loans = active_home_loans.join(
    customerDetails,
    "customer_id",
    "inner"
).select(
    customerDetails.customer_id,
    customerDetails.first_name,
    customerDetails.last_name,
    customerDetails.credit_score,
    loanInfo.loan_type,
    loanInfo.loan_status,
    loanInfo.principal_amount
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
# First, filter successful UPI transactions
upi_transactions = transactionsInfo.filter(
    (col("channel") == "UPI") & (col("status") == "Success")
)

# Add UPI transaction count per customer using window
window_spec = Window.partitionBy("customer_id")
upi_transactions_counted = upi_transactions.withColumn(
    "upi_transaction_count",
    count("transaction_id").over(window_spec)
)

# Create UPI inclined flag
upi_flag_df = upi_transactions_counted.withColumn(
    "upi_inclined_flag",
    when(col("upi_transaction_count") > 10, "Yes").otherwise("No")
).select(
    "customer_id",
    "upi_inclined_flag",
    "upi_transaction_count"
)

# Deduplicate to get one row per customer (keep one representative transaction row)
# Using row_number to pick the latest transaction per customer
window_dedup = Window.partitionBy("customer_id").orderBy(col("timestamp").desc())
upi_deduped = upi_transactions.withColumn(
    "rn",
    row_number().over(window_dedup)
).filter(col("rn") == 1).select("customer_id", "merchant_name", "transaction_type", "channel")

# Step 5: Join UPI flag back to customer_loans
# First join the deduped transaction details (left join to keep all customers)
customer_loans_txn = customer_loans.join(
    upi_deduped,
    "customer_id",
    "left"
)

# Then join the UPI inclined flag
customer_loans_txn = customer_loans_txn.join(
    upi_flag_df.dropDuplicates(["customer_id"]),
    "customer_id",
    "left"
).withColumn(
    "upi_inclined_flag",
    when(col("upi_inclined_flag").isNull(), "No").otherwise(col("upi_inclined_flag"))
)

# Step 6: Select all required columns and deduplicate final output
loan_customer_transactions = customer_loans_txn.select(
    "customer_id",
    "first_name",
    "last_name",
    "merchant_name",
    "loan_type",
    "loan_status",
    "credit_score",
    "principal_amount",
    "transaction_type",
    "channel",
    "credit_score_flag",
    "principal_amount_bucket",
    "upi_inclined_flag"
).dropDuplicates(["customer_id"])

# Write output
loan_customer_transactions.write.mode("overwrite").saveAsTable("loan_customer_transactions")

# Show sample
loan_customer_transactions.show(10, truncate=False)
