DECLARE
  CURSOR c_home_active_loans IS
    SELECT 
      cd.customer_id,
      cd.first_name,
      cd.last_name,
      li.loan_type,
      li.loan_status
    FROM 
      customerDetails cd
    INNER JOIN 
      loanInfo li
    ON 
      cd.customer_id = li.customer_id
    WHERE 
      li.loan_type = 'Home'
      AND li.loan_status = 'Active';
  
  v_record c_home_active_loans%ROWTYPE;
BEGIN
  OPEN c_home_active_loans;
  
  LOOP
    FETCH c_home_active_loans INTO v_record;
    EXIT WHEN c_home_active_loans%NOTFOUND;
    
    -- Output row (DBMS_OUTPUT for demonstration; replace with processing logic)
    DBMS_OUTPUT.PUT_LINE(
      'CustomerID: ' || v_record.customer_id || 
      ' | Name: ' || v_record.first_name || ' ' || v_record.last_name ||
      ' | Loan Type: ' || v_record.loan_type ||
      ' | Status: ' || v_record.loan_status
    );
  END LOOP;
  
  CLOSE c_home_active_loans;
END;
/
