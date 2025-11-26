--A monthly report of all accounts newly classified as in SCRA status, to include account identifier and date of status change. In addition, total volume of accounts newly within these statuses.

select a.product_account_uuid, a.started_at_est, a.status_reason, b.merchant_name
from 
EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNT_STATUS_EVENTS a
join 
EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS b
on a.product_account_uuid = b.product_account_uuid
where a.status_reason in ('SCRA', 'CONVERTED_SCRA')
and is_treatment = 'True'
and month(date_trunc('month', a.started_at_est)) = month(date_trunc('month', current_date())) - 1
and b.merchant_name in ('Crate & Barrel', 'CB2') and is_test_account = FALSE
qualify row_number() over (partition by a.product_account_uuid order by a.started_at_est asc) = 1
;


