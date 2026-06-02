WITH loan_customers AS (
  SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.credit_score,
    l.loan_type,
    l.loan_status,
    l.principal_amount
  FROM customerDetails c
  INNER JOIN loanInfo l
    ON c.customer_id = l.customer_id
  WHERE l.loan_type = 'Home'
    AND l.loan_status = 'Active'
),
upi_counts AS (
  SELECT
    customer_id,
    COUNT(*) AS upi_transaction_count
  FROM transactionsInfo
  WHERE channel = 'UPI'
  GROUP BY customer_id
  HAVING COUNT(*) > 10
),
final_dataset AS (
  SELECT
    lc.customer_id,
    lc.first_name,
    lc.last_name,
    t.merchant_name,
    lc.loan_type,
    lc.loan_status,
    lc.principal_amount,
    t.channel,
    t.transaction_type,
    lc.credit_score,
    CASE
      WHEN lc.credit_score < 650 THEN 'Risky'
      WHEN lc.credit_score BETWEEN 651 AND 750 THEN 'Average'
      WHEN lc.credit_score BETWEEN 751 AND 850 THEN 'Good'
      WHEN lc.credit_score > 850 THEN 'Excellent'
      ELSE 'Unknown'
    END AS credit_score_flag,
    CASE
      WHEN lc.principal_amount < 1000000 THEN 'low bucket'
      WHEN lc.principal_amount BETWEEN 1000000 AND 5000000 THEN 'medium bucket'
      WHEN lc.principal_amount > 5000000 THEN 'high bucket'
      ELSE 'Unknown'
    END AS principal_amount_bucket,
    CASE
      WHEN uc.upi_transaction_count > 10 THEN 'UPI inclined'
      ELSE 'Not UPI inclined'
    END AS upi_inclined_flag
  FROM loan_customers lc
  LEFT JOIN transactionsInfo t
    ON lc.customer_id = t.customer_id
  LEFT JOIN upi_counts uc
    ON lc.customer_id = uc.customer_id
)
SELECT
  customer_id,
  first_name,
  last_name,
  merchant_name,
  loan_type,
  loan_status,
  principal_amount,
  channel,
  transaction_type,
  credit_score,
  credit_score_flag,
  principal_amount_bucket,
  upi_inclined_flag
FROM final_dataset;