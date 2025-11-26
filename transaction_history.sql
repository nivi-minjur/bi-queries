SELECT DISTINCT 
    pa.product_account_uuid, 
    txn.consumer_uuid, 
    txn.purchase_merchant_name, 
    txn.transaction_confirmed_at_est, 
    txn.transaction_type, 
    amount*-1, 
    fee_reason, 
    interest_reason, 
    apr.final_apr
FROM EDW_PRD.marts.mrt_public__fct_transactions txn
LEFT JOIN EDW_PRD.marts.mrt_public__dim_credit_profile_final_apr_history apr
    ON txn.credit_line_uuid = apr.credit_line_uuid
    AND txn.transaction_confirmed_at_est >= apr.effective_start_at_est
    AND txn.transaction_confirmed_at_est < COALESCE(apr.effective_end_at_est, '9999-12-31')
LEFT JOIN EDW_PRD.marts.mrt_public__dim_product_accounts pa 
    ON pa.credit_line_uuid = txn.credit_line_uuid
WHERE txn.merchant_combname = 'CBH'
    AND transaction_status = 'CONFIRMED'
    AND transaction_type <> 'AUTH'
    AND MONTH(DATE_TRUNC('month', transaction_confirmed_at_est)) = MONTH(DATE_TRUNC('month', CURRENT_DATE())) - 1
    AND DATE_TRUNC('day', transaction_confirmed_at_est) >= '2025-10-10'
    AND is_test_account = FALSE
    AND is_stmt_pseudo_txn = FALSE
    AND is_pre_conversion_txn = FALSE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY txn.transaction_uuid 
    ORDER BY COALESCE(apr.effective_end_at_est, '9999-12-31') DESC
) = 1
ORDER BY 4 DESC;