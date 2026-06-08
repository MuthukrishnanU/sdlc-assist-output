SELECT COUNT(*) AS record_count FROM (
    SELECT * FROM customerDetails WHERE kyc_status = 'Verified'
    UNION ALL
    SELECT * FROM accountBalances WHERE account_type IN ('Savings', 'Current')
    UNION ALL
    SELECT * FROM loanInfo WHERE loan_status = 'Active'
    UNION ALL
    SELECT * FROM transactionsInfo WHERE status = 'Success'
) AS approved_records;