from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Sample data generation for transactionsInfo (1000 records)
data = [
    (f"txn_{i}", f"acc_{i%100}", f"cust_{i%200}", round(100 + (i % 5000) * 0.99, 2), 
     "Credit" if i % 2 == 0 else "Debit", 
     ["UPI", "NetBanking", "ATM", "POS"][i % 4], 
     F.date_add(F.current_date(), -i % 365).cast("date"), 
     f"merchant_{i%50}", 
     "Success" if i % 10 != 0 else ["Failed", "Flagged"][i % 2])
    for i in range(1000)
]

# Create DataFrame
transactions_df = spark.createDataFrame(
    data,
    schema=["transaction_id", "account_id", "customer_id", "amount", 
            "transaction_type", "channel", "timestamp", "merchant_name", "status"]
)

# Monthwise, channelwise transaction amount and count
result_df = (
    transactions_df
    .withColumn("month", F.date_format("timestamp", "yyyy-MM"))
    .groupBy("month", "channel")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("transaction_id").alias("transaction_count")
    )
    .orderBy("month", "channel")
)

# Show the result
result_df.show()