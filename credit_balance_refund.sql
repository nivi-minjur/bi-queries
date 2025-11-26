-- Account level reporting of customers with a Credit Balance refund, to include date issued, amount, volume of credit balance refunds requested and account identifiers.

select product_account_uuid,transaction_confirmed_at_est, amount * -1 as amount from 
EDW_PRD.MARTS.MRT_PUBLIC__FCT_TRANSACTIONS
where month(date_trunc('month', created_at_est)) = month(date_trunc('month', current_date())) - 1
and transaction_type = 'CREDIT_RETURN'  
and merchant_combname = 'CBH'
and purchase_merchant_name = 'Credit balance refunded'
;