from pyspark.sql import functions as F

df_customerDetails = spark.read.format("parquet").load("customerDetails")
df_loanInfo = spark.read.format("parquet").load("loanInfo")

# Join customerDetails and loanInfo on customer_id
joined_df = df_customerDetails.join(df_loanInfo, on="customer_id", how="inner")

# Filter for active home loans
result_df = joined_df.filter((F.col("loan_type") == "Home") & (F.col("loan_status") == "Active"))

# Select required columns
result_df = result_df.select(["customer_id", "first_name", "last_name", "loan_type", "loan_status", "remaining_balance"])