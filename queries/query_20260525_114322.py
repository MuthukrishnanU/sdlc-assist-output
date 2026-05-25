
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("LoanAnalysis").getOrCreate()

# Load tables into DataFrames
df_loan_info = spark.table("loanInfo")

# Filter customers with active loans and remaining balance more than 10000
active_loans_df = df_loan_info \
    .filter((col("loan_status") == "Active") & (col("remaining_balance") > 10000))

# Collect the results
results = active_loans_df.collect()

# Print out results for verification
for row in results:
    print(row)
