
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CustomerFilter").getOrCreate()

# Assuming dataframes for each of the tables
customer_details_df = spark.table("customerDetails")
account_balances_df = spark.table("accountBalances")
transactions_info_df = spark.table("transactionsInfo")

# Filter customers with verified KYC status
verified_customers_df = customer_details_df.filter(col("kyc_status") == "Verified")

# Filter account balances where available balance is greater than 100,000 for savings account
target_balances_df = account_balances_df.filter((col("available_balance") > 100000) & (col("transaction_type") == 'savings'))

# Filter transactions where UPI transaction amount is greater than 5,000
upi_transactions_df = transactions_info_df.filter((col("transaction_type") == "UPI") & (col("available_balance") > 5000))

# Join all conditions together to find the target customers
result_df = verified_customers_df.join(target_balances_df, "customer_id") \
                                    .join(upi_transactions_df, "customer_id")

result_df.show()
