from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName('Loan Data').getOrCreate()

# Load the data into DataFrames
customerDetails = spark.read.csv('customerDetails.csv', header=True, inferSchema=True)
loanInfo = spark.read.csv('loanInfo.csv', header=True, inferSchema=True)

# Register the DataFrames as temporary views
customerDetails.createOrReplaceTempView('customerDetails')
loanInfo.createOrReplaceTempView('loanInfo')

# Perform the query
result = spark.sql("'''
    SELECT cd.customer_id, cd.first_name, cd.last_name, li.loan_type, li.loan_status, cd.credit_score
    FROM customerDetails cd
    JOIN loanInfo li ON cd.customer_id = li.customer_id
    WHERE li.loan_type = 'Home' AND li.loan_status = 'Active'
"''')

# Show the results
result.show()
