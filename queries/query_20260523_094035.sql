SELECT cd.customer_id, SUM(ab.available_balance) AS total_transaction_volume
FROM customerDetails cd
JOIN accountBalances ab ON cd.customer_id = ab.customer_id
JOIN loanInfo li ON cd.customer_id = li.customer_id
WHERE cd.kyc_status = 'Verified' AND cd.credit_score < 600 AND li.loan_status = 'Active'
GROUP BY cd.customer_id;