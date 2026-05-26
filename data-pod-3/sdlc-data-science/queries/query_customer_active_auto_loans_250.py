
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("LoanDataFiltering") \
    .getOrCreate()

# Load data into DataFrames
customer_details_df = spark.table("customerDetails")
loan_info_df = spark.table("loanInfo")

# Join customerDetails and loanInfo on customer_id
df_joined = customer_details_df.join(loan_info_df, "customer_id")

# Filter DataFrame for active auto loans
filtered_df = df_joined \
    .filter((col("loan_status") == "Active") & (col("loan_type") == "auto loan")) \
    .select("customer_id", "first_name", "last_name", "kyc_status", "loan_status", "loan_type")

# Show result
filtered_df.show()
