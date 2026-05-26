import pymongo

# Connect to MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['loanDatabase']

# Define the collections
db_loan_info = db['loanInfo']
db_customer_details = db['customerDetails']

# Query to find customers with active loan status
active_loans_pipeline = [
    {
        "$lookup": {
            "from": "customerDetails",
            "localField": "customer_id",
            "foreignField": "customer_id",
            "as": "customer"
        }
    },
    {
        "$unwind": "$customer"
    },
    {
        "$match": {
            "loan_status": "active"
        }
    },
    {
        "$project": {
            "_id": 0,
            "customer.customer_id": 1,
            "customer.first_name": 1,
            "customer.last_name": 1,
            "loan_id": 1,
            "loan_status": 1,
            "remaining_balance": 1
        }
    }
]

active_customers = list(db_loan_info.aggregate(active_loans_pipeline))

# Output the fetched active loan customers
for customer in active_customers:
    print(customer)
