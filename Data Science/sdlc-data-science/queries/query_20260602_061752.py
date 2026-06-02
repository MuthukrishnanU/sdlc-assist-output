from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, concat, lit, sum as spark_sum, max as spark_max, min as spark_min, avg as spark_avg

# Initialize Spark Session
spark = SparkSession.builder.appName("LoanCustomerTransactions").getOrCreate()

# Load tables
customerDetails = spark.table("customerDetails")
accountBalances = spark.table("accountBalances")
loanInfo = spark.table("loanInfo")
transactionsInfo = spark.table("transactionsInfo")

# Step 1: Base loan filter - Home Loan with Active status
home_active_loans = loanInfo.filter(
    (col("loan_type") == "Home") & (col("loan_status") == "Active")
)

# Step 2: Join with customer details
customer_loans = home_active_loans.join(
    customerDetails,
    on="customer_id",
    how="inner"
).select(
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("loan_type"),
    col("loan_status"),
    col("principal_amount"),
    col("credit_score")
)

# Step 3: Add credit score flag
customer_loans_flagged = customer_loans.withColumn(
    "credit_score_flag",
    when(col("credit_score") < 650, "Risky")
    .when((col("credit_score") >= 651) & (col("credit_score") <= 750), "Average")
    .when((col("credit_score") > 750) & (col("credit_score") <= 850), "Good")
    .when(col("credit_score") > 850, "Excellent")
    .otherwise("Unknown")
)

# Step 4: Add principal amount bucket
customer_loans_bucketed = customer_loans_flagged.withColumn(
    "principal_bucket",
    when(col("principal_amount") < 1000000, "low bucket")
    .when((col("principal_amount") >= 1000000) & (col("principal_amount") <= 5000000), "medium bucket")
    .when(col("principal_amount") > 5000000, "high bucket")
    .otherwise("Unknown")
)

# Step 5: Calculate UPI transaction counts per customer
upi_transaction_counts = transactionsInfo.filter(
    (col("channel") == "UPI") & (col("status") == "Success")
).groupBy("customer_id").agg(
    count("transaction_id").alias("upi_transaction_count")
)

# Step 6: Create UPI inclined flag
customer_upi_flag = upi_transaction_counts.withColumn(
    "upi_inclined_flag",
    when(col("upi_transaction_count") > 10, "UPI Inclined").otherwise("Non UPI Inclined")
)

# Step 7: Join UPI flag with main dataset
loan_customer_enriched = customer_loans_bucketed.join(
    customer_upi_flag.select("customer_id", "upi_inclined_flag"),
    on="customer_id",
    how="left"
).fillna({"upi_inclined_flag": "Non UPI Inclined"})

# Step 8: Join with transactions info to get merchant_name, channel, transaction_type
# Since we need to include transaction columns, we join with full transactionsInfo
# To avoid explosion, we'll aggregate or join appropriately. Based on requirements,
# we will join to get transaction details. For multiple transactions per customer,
# we keep all rows as the output table should contain transaction details.

final_output = loan_customer_enriched.join(
    transactionsInfo.select("customer_id", "merchant_name", "channel", "transaction_type"),
    on="customer_id",
    how="left"
).select(
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("merchant_name"),
    col("loan_type"),
    col("loan_status"),
    col("principal_amount"),
    col("credit_score"),
    col("channel"),
    col("transaction_type"),
    col("credit_score_flag"),
    col("principal_bucket"),
    col("upi_inclined_flag")
)

# Save as loan_customer_transactions table
final_output.write.mode("overwrite").saveAsTable("loan_customer_transactions")

# Show sample
final_output.show(20, truncate=False)

# Data Quality checks
dq_row_count = final_output.count()
dq_null_values = final_output.select([col(c).isNull().cast("int") for c in final_output.columns]).agg(*[spark_sum(col(c)) for c in final_output.columns]).collect()[0].asDict().values()
dq_null_values = sum(dq_null_values)

dq_duplicate_rows = final_output.count() - final_output.dropDuplicates().count()

dq_min_principal = final_output.select(spark_min("principal_amount")).collect()[0][0]
dq_max_principal = final_output.select(spark_max("principal_amount")).collect()[0][0]
dq_avg_principal = final_output.select(spark_avg("principal_amount")).collect()[0][0]

dq_distinct_rows = final_output.distinct().count()

# Check for empty strings
dq_empty_strings = 0
for c in final_output.columns:
    if final_output.schema[c].dataType.typeName() == "string":
        dq_empty_strings += final_output.filter(col(c) == "").count()

print(f"DQ Row Count: {dq_row_count}")
print(f"DQ Null Values: {dq_null_values}")
print(f"DQ Duplicate Rows: {dq_duplicate_rows}")
print(f"DQ Min Principal: {dq_min_principal}")
print(f"DQ Max Principal: {dq_max_principal}")
print(f"DQ Avg Principal: {dq_avg_principal}")
print(f"DQ Distinct Rows: {dq_distinct_rows}")
print(f"DQ Empty Strings: {dq_empty_strings}")
