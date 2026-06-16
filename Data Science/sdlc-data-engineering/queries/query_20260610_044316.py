from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as _sum, lit
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("LoanCustomerTransactions").getOrCreate()

# Read tables
customerDetails = spark.table("customerDetails")
accountBalances = spark.table("accountBalances")
loanInfo = spark.table("loanInfo")
transactionsInfo = spark.table("transactionsInfo")

# Step 1: Filter customers with Home Loan and Active status
customer_loans = loanInfo.filter(
    (col("loan_type") == "Home") & (col("loan_status") == "Active")
).join(
    customerDetails, "customer_id", "inner"
).select(
    "customer_id",
    "first_name",
    "last_name",
    "credit_score",
    "principal_amount"
)

# Step 2: Create credit score flag
customer_loans = customer_loans.withColumn(
    "credit_score_flag",
    when(col("credit_score") < 650, "Risky")
    .when((col("credit_score") >= 651) & (col("credit_score") <= 750), "Average")
    .when((col("credit_score") > 750) & (col("credit_score") <= 850), "Good")
    .when(col("credit_score") > 850, "Excellent")
    .otherwise("Unknown")
)

# Step 3: Create principal amount bucket
customer_loans = customer_loans.withColumn(
    "principal_amount_bucket",
    when(col("principal_amount") < 1000000, "low")
    .when((col("principal_amount") >= 1000000) & (col("principal_amount") <= 5000000), "medium")
    .when(col("principal_amount") > 5000000, "high")
    .otherwise("Unknown")
)

# Step 4: Count UPI transactions per customer
upi_transactions_count = transactionsInfo.filter(
    (col("channel") == "UPI") & (col("status") == "Success")
).groupBy("customer_id").agg(
    count("*").alias("upi_transaction_count")
)

# Step 5: Join UPI transaction count with customer_loans
customer_loans = customer_loans.join(
    upi_transactions_count, "customer_id", "left"
).withColumn(
    "upi_inclined_flag",
    when(col("upi_transaction_count") > 10, "Yes").otherwise("No")
)

# Step 6: Join with transactionsInfo to get merchant_name, channel, transaction_type
# Deduplicate transactionsInfo to avoid row duplication
window_spec = Window.partitionBy("customer_id").orderBy(col("timestamp").desc())

deduped_transactions = transactionsInfo.withColumn(
    "row_num",
    row_number().over(window_spec)
).filter(col("row_num") == 1).drop("row_num")

# Join with deduplicated transactions
loan_customer_transactions = customer_loans.join(
    deduped_transactions,
    "customer_id",
    "left"
).select(
    "customer_id",
    "first_name",
    "last_name",
    "merchant_name",
    lit("Home").alias("loan_type"),
    lit("Active").alias("loan_status"),
    "credit_score",
    "principal_amount",
    "transaction_type",
    "channel",
    "credit_score_flag",
    "principal_amount_bucket",
    "upi_inclined_flag"
).dropDuplicates(["customer_id"])

# Show the result
loan_customer_transactions.show(1000, truncate=False)

# Write the result to a table
loan_customer_transactions.write.saveAsTable("loan_customer_transactions")