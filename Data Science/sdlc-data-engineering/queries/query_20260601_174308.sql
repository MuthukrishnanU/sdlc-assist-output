SELECT 
    DATE_TRUNC('month', timestamp) AS transaction_month,
    channel,
    COUNT(transaction_id) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactionsInfo
GROUP BY 
    DATE_TRUNC('month', timestamp),
    channel
ORDER BY 
    transaction_month DESC,
    channel;