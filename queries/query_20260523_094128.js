from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['financialDB']

# Define collections
data_balance = db['accountBalances']
data_customer = db['customerDetails']
data_loan = db['loanInfo']

# Aggregation pipeline to calculate total transaction volumes for high-risk customers
pipeline = [
    # Join customerDetails with accountBalances on customer_id
    {
        '$lookup': {
            'from': 'accountBalances',
            'localField': 'customer_id',
            'foreignField': 'customer_id',
            'as': 'account_info'
        }
    },
    # Unwind the joined account_info array
    {
        '$unwind': '$account_info'
    },
    # Join with loanInfo to add loan details
    {
        '$lookup': {
            'from': 'loanInfo',
            'localField': 'customer_id',
            'foreignField': 'customer_id',
            'as': 'loan_info'
        }
    },
    # Unwind the joined loan_info array
    {
        '$unwind': '$loan_info'
    },
    # Match to filter high-risk customers (e.g., high credit_score, poor kyc_status)
    {
        '$match': {
            'credit_score': {'$lt': 600},  # Assuming a credit score below 600 is high-risk
            'kyc_status': 'not_verified'
        }
    },
    # Group and sum transaction volumes for these customers
    {
        '$group': {
            '_id': '$customer_id',
            'total_transaction_volume': {'$sum': '$account_info.available_balance'},
        }
    }
]

# Execute the pipeline
result = list(data_customer.aggregate(pipeline))

# Print the results
for r in result:
    print(r)

# Close the connection
client.close()