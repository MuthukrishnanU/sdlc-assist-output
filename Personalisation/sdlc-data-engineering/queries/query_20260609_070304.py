from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("ActiveHomeLoans").getOrCreate()

# Load tables
customerDetails = spark.table("customerDetails")
loanInfo = spark.table("loanInfo")

# Join customerDetails with loanInfo on customer_id
# Filter for active home loans
active_home_loans = (
    customerDetails
    .join(loanInfo, "customer_id", "inner")
    .filter((col("loan_type") == "Home") & (col("loan_status") == "Active"))
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "loan_type",
        "loan_status",
        "principal_amount"
    )
    .dropDuplicates(["customer_id"])
)

# Show results
active_home_loans.show(1000, truncate=False)

# Optional: write or register as temp view
active_home_loans.createOrReplaceTempView("active_home_loans")
