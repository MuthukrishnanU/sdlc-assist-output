SELECT 
    cd.customer_id,
    cd.first_name,
    cd.last_name,
    li.loan_type,
    li.loan_status
FROM customerDetails cd
INNER JOIN loanInfo li ON cd.customer_id = li.customer_id
WHERE li.loan_type = 'Home'
  AND li.loan_status = 'Active';