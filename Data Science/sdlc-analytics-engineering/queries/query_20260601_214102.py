from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("HomeLoansActiveCustomers").getOrCreate()

customerDetails = spark.table("customerDetails")
loanInfo = spark.table("loanInfo")

result_df = (
    customerDetails
    .join(loanInfo, on="customer_id", how="inner")
    .filter((col("loan_type") == "Home") & (col("loan_status") == "Active"))
    .select("customer_id", "first_name", "last_name", "loan_type", "loan_status")
    .dropDuplicates()
)

result_df.show(100)