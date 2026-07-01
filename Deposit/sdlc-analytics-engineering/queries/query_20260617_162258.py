from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Assuming the Spark session is already created and available as 'spark'

# Load the customerDetails and loanInfo tables
customer_details_df = spark.table('customerDetails')
loan_info_df = spark.table('loanInfo')

# Filter loanInfo for active home loans
active_home_loans_df = loan_info_df.filter((col('loan_type') == 'Home') & (col('loan_status') == 'Active'))

# Join customerDetails with active home loans on customer_id
joined_df = customer_details_df.join(active_home_loans_df, 'customer_id', 'inner')

# Select the required columns
result_df = joined_df.select(
    'customer_id',
    'first_name',
    'last_name',
    'email',
    'phone',
    'date_of_birth',
    'loan_type',
    'loan_status',
    'principal_amount',
    'remaining_balance'
)

# Deduplicate the result to ensure unique customer records
result_df = result_df.dropDuplicates(['customer_id'])

# Show the result
result_df.show()