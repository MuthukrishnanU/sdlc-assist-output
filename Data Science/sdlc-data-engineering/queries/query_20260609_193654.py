from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("ActiveHomeLoans").getOrCreate()

# Load sample data (1000 records context)
customerDetails = spark.table("customerDetails")
loanInfo = spark.table("loanInfo")

# Join customerDetails with loanInfo on customer_id
# Filter for Active Home Loans as per logic
active_home_loans = customerDetails.join(
    loanInfo,
    customerDetails.customer_id == loanInfo.customer_id,
    "inner"
).filter(
    (col("loan_status") == "Active") & (col("loan_type") == "Home")
).select(
    customerDetails.customer_id,
    customerDetails.first_name,
    customerDetails.last_name,
    loanInfo.loan_status,
    loanInfo.loan_type,
    loanInfo.loan_id,
    loanInfo.remaining_balance,
    customerDetails.credit_score
)

# Deduplicate to ensure one row per customer (preventing row duplication from potential many-to-one joins)
active_home_loans = active_home_loans.dropDuplicates(["customer_id"])

# Display results
active_home_loans.show(truncate=False)

# Optional: write or further process
# active_home_loans.write.mode("overwrite").parquet("output/active_home_loans")
