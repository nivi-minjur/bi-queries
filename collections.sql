-- Account level reporting that includes: Past Due, Deceased, CCCS, Cease and Desist, and Settlement, to include account identifier and date of status change. In addition, total volume of accounts newly within these statuses.


-- DQ accounts (1+ DPD) from loan tape
-- status deceased or CCCS from status history

select product_account_uuid, 
dateadd('day', -1*days_past_due, LAST_DAY(DATEADD('month', -1, current_date())) ) as started_at_est,
'DELINQUENT' as status, 
a.merchant_name 
from 
Edw_prd.reports.rpt_public__fct_loan_tape a
join EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS b
on b.credit_line_uuid = a.credit_line_uuid 
where a.merchant_name in ('Crate & Barrel', 'CB2')
and activity_date = LAST_DAY(DATEADD('month', -1, current_date()))
and days_past_due > 0

UNION 

select a.product_account_uuid, a.started_at_est, 
case when a.status_reason in ('CCCS', 'DECEASED') then a.status_reason
end as status,
b.merchant_name
from 
EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNT_STATUS_EVENTS a
join 
EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS b
on a.product_account_uuid = b.product_account_uuid
where a.status_reason in ('CCCS', 'DECEASED')
and month(date_trunc('month', a.started_at_est)) = month(date_trunc('month', current_date())) - 1
and b.merchant_name in ('Crate & Barrel', 'CB2') and is_test_account = FALSE
qualify row_number() over (partition by a.product_account_uuid order by a.started_at_est asc) = 1
