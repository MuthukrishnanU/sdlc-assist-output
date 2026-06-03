
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("LoanCustomerTransactions").getOrCreate()

# Load tables
customerDetails = spark.read.table("customerDetails")
loanInfo = spark.read.table("loanInfo")
transactionsInfo = spark.read.table("transactionsInfo")

# Filter for active home loans
active_home_loans = loanInfo.filter((col("loan_type") == "Home") & (col("loan_status") == "Active"))

# Join with customer details
customer_loans = active_home_loans.join(customerDetails, "customer_id")

# Add credit score flag
customer_loans = customer_loans.withColumn(
    "credit_score_flag",
    when(col("credit_score") < 650, "Risky")
    .when((col("credit_score") >= 651) & (col("credit_score") <= 750), "Average")
    .when((col("credit_score") > 750) & (col("credit_score") <= 850), "Good")
    .otherwise("Excellent")
)

# Add principal amount bucket
customer_loans = customer_loans.withColumn(
    "principal_bucket",
    when(col("principal_amount") < 1000000, "low")
    .when((col("principal_amount") >= 1000000) & (col("principal_amount") <= 5000000), "medium")
    .otherwise("high")
)

# Calculate UPI transaction count per customer
upi_transactions = transactionsInfo.filter(col("channel") == "UPI")
upi_count = upi_transactions.groupBy("customer_id").agg(count("transaction_id").alias("upi_transaction_count"))

# Join UPI transaction count with customer loans
customer_loans = customer_loans.join(upi_count, "customer_id", "left")

# Add UPI inclined flag
customer_loans = customer_loans.withColumn(
    "upi_inclined",
    when(col("upi_transaction_count") > 10, lit(True)).otherwise(lit(False))
)

# Select required columns
final_df = customer_loans.select(
    "customer_id",
    "first_name",
    "last_name",
    "merchant_name",
    "loan_type",
    "loan_status",
    "credit_score",
    "principal_amount",
    "transaction_id",
    "channel",
    "credit_score_flag",
    "principal_bucket",
    "upi_inclined"
).dropDuplicates(["customer_id"])

# Show the result
final_df.show()
