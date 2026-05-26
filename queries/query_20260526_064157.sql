SELECT cd.customer_id, cd.first_name, cd.last_name, li.loan_type, li.loan_status 
FROM loanInfo li 
JOIN customerDetails cd ON li.customer_id = cd.customer_id 
WHERE li.loan_status = 'Active' AND li.loan_type = 'auto loan';