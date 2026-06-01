from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("CustomerLoanFilter").getOrCreate()

# Load tables
customerDetails = spark.table("customerDetails")
loanInfo = spark.table("loanInfo")

# Join and filter: customers with loan type as 'Home' and loan status as 'Active'
result_df = (
    customerDetails
    .join(loanInfo, customerDetails.customer_id == loanInfo.customer_id, "inner")
    .filter((col("loan_type") == "Home") & (col("loan_status") == "Active"))
    .select(
        customerDetails.customer_id,
        customerDetails.first_name,
        customerDetails.last_name,
        loanInfo.loan_type,
        loanInfo.loan_status
    )
    .dropDuplicates()
)

# Show sample results
result_df.show(1000, truncate=False)

# Optional: write or register as temp view
result_df.createOrReplaceTempView("active_home_loan_customers")
