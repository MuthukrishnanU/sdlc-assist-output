WITH loan_customers AS (
    SELECT 
        cd.first_name || ' ' || cd.last_name AS customer_name,
        li.customer_id,
        li.credit_score,
        li.principal_amount,
        CASE 
            WHEN li.credit_score < 650 THEN 'Risky'
            WHEN li.credit_score BETWEEN 651 AND 750 THEN 'Average'
            WHEN li.credit_score BETWEEN 751 AND 850 THEN 'Good'
            ELSE 'Excellent'
        END AS credit_score_flag,
        CASE 
            WHEN li.principal_amount < 1000000 THEN 'Low Bucket'
            WHEN li.principal_amount BETWEEN 1000000 AND 5000000 THEN 'Medium Bucket'
            ELSE 'High Bucket'
        END AS principal_bucket
    FROM 
        customerDetails cd
    JOIN 
        loanInfo li ON cd.customer_id = li.customer_id
    WHERE 
        li.loan_type = 'Home Loan' AND li.loan_status = 'Active'
),
transaction_count AS (
    SELECT 
        customer_id,
        COUNT(*) AS upi_transaction_count
    FROM 
        transactionsInfo
    WHERE 
        transaction_type = 'UPI'
    GROUP BY 
        customer_id
    HAVING 
        COUNT(*) > 10
)

SELECT 
    lc.customer_name,
    lc.credit_score_flag,
    lc.principal_bucket,
    CASE WHEN tc.upi_transaction_count IS NOT NULL THEN 'UPI Inclined' ELSE 'Not UPI Inclined' END AS upi_inclination_flag
FROM 
    loan_customers lc
LEFT JOIN 
    transaction_count tc ON lc.customer_id = tc.customer_id;