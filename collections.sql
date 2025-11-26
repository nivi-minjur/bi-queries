-- Account level reporting that includes: Past Due, Deceased, CCCS, Cease and Desist, and Settlement, to include account identifier and date of status change. In addition, total volume of accounts newly within these statuses.


-- DQ accounts (1+ DPD) from loan tape
-- status deceased or CCCS from status history
-- manual C+D and settlement

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

UNION

select product_account_uuid, '2025-10-09 23:59:00.000' as started_at_est, 'CEASE AND DESIST' as status, merchant_name from 
EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS
where consumer_uuid in 
('CSMR-v1-82e5fb28-833e-4ee0-9915-a8945bb3c96c',
'CSMR-v1-0764773a-ccd1-45a0-b7c5-529f57c07a0f',
'CSMR-v1-0f6b28ca-5619-41f0-a915-e9ac50cf9acd',
'CSMR-v1-09d9fe7c-d407-4ea0-99d1-74f41ec36b3d',
'CSMR-v1-b34777b8-7905-357e-84e3-18a778d9c3db',
'CSMR-v1-829d53f7-9690-411e-870d-b0c4242d6c70',
'CSMR-v1-48c0a829-63c4-3f95-b849-a184b5412990',
'CSMR-v1-aa8a2077-ae5f-4175-a01c-9a9b34e6290c',
'CSMR-v1-f7d410ee-eba9-47bb-9d68-87df0957dd38') 
--consumer_uuids from manual C+D list: https://docs.google.com/spreadsheets/d/1wFZzjjz8Mcd8-5xcD423Tjg5qFJulGLnjyUihe6ZfcY/edit?gid=353511271#gid=353511271
OR 
credit_line_uuid in (select credit_line_uuid from demo_db.public.CRATE_CEASE_AND_DESIST)
and merchant_combname = 'CBH'; --table consisting of collections exclusion list https://docs.google.com/spreadsheets/d/1rJlAA3-ecLSsn0jQR87RxAO3sCq6WfTbRaV1yy7b_Fg/edit?gid=0#gid=0