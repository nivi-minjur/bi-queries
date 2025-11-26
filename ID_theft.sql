-- Report of all accounts reflecting ID Theft Claims, unless included in another report, along with volume of accounts reflecting this in the month being reported.

select a.product_account_uuid, a.started_at_est, a.status_reason_code,
from 
EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNT_STATUS_EVENTS a
join 
EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS b
on a.product_account_uuid = b.product_account_uuid
where a.status_reason_code = 'ID_THEFT' 
and month(date_trunc('month', a.started_at_est)) = month(date_trunc('month', current_date())) - 1
and merchant_combname = 'CBH'
and is_test_account = FALSE
qualify row_number() over (partition by a.product_account_uuid order by a.started_at_est asc) = 1;