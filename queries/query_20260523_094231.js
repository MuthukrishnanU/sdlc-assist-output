
# Import Firestore client
from google.cloud import firestore

# Initialize Firestore client
db = firestore.Client()

# Define the high-risk customer threshold
CREDIT_SCORE_THRESHOLD = 600

# Reference collections
doc_ref_balances = db.collection('accountBalances')
doc_ref_customers = db.collection('customerDetails')
doc_ref_loans = db.collection('loanInfo')

# Fetch customer details with high-risk credit score
high_risk_customers = doc_ref_customers.where('credit_score', '<=', CREDIT_SCORE_THRESHOLD).stream()

# Calculate total transaction volume for high-risk customers
total_transaction_volume = 0

for customer in high_risk_customers:
    customer_id = customer.id
    # Fetch account balance
    account_balance = doc_ref_balances.document(customer_id).get()
    if account_balance.exists:
        available_balance = account_balance.to_dict().get('available_balance', 0)
        total_transaction_volume += available_balance

    # Fetch loan information
    loan_info = doc_ref_loans.document(customer_id).get()
    if loan_info.exists:
        remaining_balance = loan_info.to_dict().get('remaining_balance', 0)
        total_transaction_volume += remaining_balance

print(f"Total Transaction Volume for High-Risk Customers: {total_transaction_volume}")
