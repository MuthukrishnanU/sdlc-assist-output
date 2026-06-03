from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, row_number, lit, max as spark_max
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("LoanCustomerTransactions").getOrCreate()

# Load tables
customerDetails = spark.table("customerDetails")
loanInfo = spark.table("loanInfo")
transactionsInfo = spark.table("transactionsInfo")

# Step 1: Filter active home loans and join with customer details
active_home_loans = loanInfo.filter(
    (col("loan_type") == "Home") & (col("loan_status") == "Active")
)

customer_loans = customerDetails.join(
    active_home_loans, "customer_id", "inner"
).select(
    "customer_id",
    "first_name",
    "last_name",
    "credit_score",
    "loan_type",
    "loan_status",
    "principal_amount"
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
# Count UPI transactions per customer
upi_window = Window.partitionBy("customer_id")

# Create a temp view of transactions with UPI count per customer
transactions_with_upi_count = transactionsInfo.withColumn(
    "upi_transaction_count",
    count(when((col("channel") == "UPI") & (col("status") == "Success"), 1)).over(upi_window)
)

# Step 5: Deduplicate transactionsInfo to get one row per customer (latest transaction)
transaction_row_num = Window.partitionBy("customer_id").orderBy(col("timestamp").desc())

deduped_transactions = transactions_with_upi_count.withColumn(
    "rn", row_number().over(transaction_row_num)
).filter(col("rn") == 1).drop("rn")

# Step 6: Create UPI inclined flag
deduped_transactions = deduped_transactions.withColumn(
    "upi_inclined_flag",
    when(col("upi_transaction_count") > 10, "UPI inclined").otherwise("Not UPI inclined")
)

# Step 7: Join deduplicated transactions with customer_loans
final_df = customer_loans.join(
    deduped_transactions.select(
        "customer_id",
        "channel",
        "merchant_name",
        "transaction_type",
        "upi_transaction_count",
        "upi_inclined_flag"
    ),
    "customer_id",
    "left"
)

# Step 8: Select all required columns and computed columns
loan_customer_transactions = final_df.select(
    "customer_id",
    "first_name",
    "last_name",
    "credit_score",
    "loan_type",
    "loan_status",
    "channel",
    "merchant_name",
    "transaction_type",
    "principal_amount",
    "credit_score_flag",
    "principal_amount_bucket",
    "upi_inclined_flag"
)

# Step 9: Deduplicate by customer_id to ensure row count matches primary table
loan_customer_transactions = loan_customer_transactions.dropDuplicates(["customer_id"])

# Step 10: Write output
loan_customer_transactions.write.mode("overwrite").saveAsTable("loan_customer_transactions")

# Show sample
loan_customer_transactions.show(10, truncate=False)
