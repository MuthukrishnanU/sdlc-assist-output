
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull

# Initialize Spark Session
spark = SparkSession.builder 
    .appName("BalanceCheck") 
    .getOrCreate()

# Sample data loading via schema definition and data simulation, replace with actual data load
accountBalances = spark.createDataFrame([
    ("account1", "cust1", 100000.0),
    ("account2", "cust2", 30000.0),
    ("account3", "cust3", 70000.0),
    ("account4", "cust4", 50000.0),
    ("account5", "cust5", 120000.0)
], ["account_id", "customer_id", "remaining_balance"])

# Filter customers with remaining balance more than 50000 and non-null balances
filteredDF = accountBalances \
    .filter((col("remaining_balance") > 50000) & ~isnull("remaining_balance"))

# Show the results
filteredDF.show()

# Stop the Spark session
spark.stop()
