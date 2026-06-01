SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    l.loan_type,
    l.loan_status
FROM customerDetails c
INNER JOIN loanInfo l
    ON c.customer_id = l.customer_id
WHERE l.loan_type = 'Home'
    AND l.loan_status = 'Active';