from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Customer Loans").getOrCreate()

# Load the customerDetails and loanInfo DataFrames
customerDetails = spark.read.parquet("customerDetails.parquet")
customerDetails.createOrReplaceTempView("customerDetails")

loanInfo = spark.read.parquet("loanInfo.parquet")
loanInfo.createOrReplaceTempView("loanInfo")

# Register the DataFrames as temporary views
spark.sql("SELECT cd.customer_id, cd.first_name, cd.last_name, li.loan_type, li.loan_status, li.principal_amount \
FROM customerDetails cd \
JOIN loanInfo li ON cd.customer_id = li.customer_id \
WHERE li.loan_type = 'Home' AND li.loan_status = 'Active'").show(1000)
