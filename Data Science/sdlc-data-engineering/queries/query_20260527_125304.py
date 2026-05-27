
# PySpark code to filter customers with Home loans and Active status
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder 
    .appName("Loan Info Filter") 
    .getOrCreate()

# Load tables
loanInfo = spark.table("loanInfo")
customerDetails = spark.table("customerDetails")

# Join and filter the data
filtered_customers = loanInfo 
    .join(customerDetails, loanInfo.customer_id == customerDetails.customer_id) 
    .filter((loanInfo.loan_type == "Home") & (loanInfo.loan_status == "Active")) 
    .select(customerDetails.first_name, customerDetails.last_name)

# Show the results
filtered_customers.show()
