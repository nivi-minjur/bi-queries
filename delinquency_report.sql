WITH card_reissue as (
 SELECT
        DATEDIFF(DAY, A.LAST_INGEST_AT_EST, B.CREATED_AT_EST) AS DATE_DIFF,
        A.CARD_UUID, 
        A.PRODUCT_ACCOUNT_UUID,  
        A.CARD_STATUS, 
        A.CARD_TYPE, 
        A.CANCEL_REASON,
        A.LAST_INGEST_AT_EST,
        B.CREATED_AT_EST AS TXN_CREATED_AT_EST,
        B.TRANSACTION_UUID
        FROM EDW_PRD.MARTS.MRT_PUBLIC__DIM_CARD_STATUS_HISTORY A
        INNER JOIN EDW_PRD.MARTS.MRT_PUBLIC__FCT_TRANSACTIONS B ON A.CARD_UUID = B.CARD_UUID
            AND DATEDIFF(DAY, A.LAST_INGEST_AT_EST, B.CREATED_AT_EST) <= 0
            AND DATEDIFF(DAY, A.LAST_INGEST_AT_EST, B.CREATED_AT_EST) >= -3 
        WHERE --CANCEL_REASON IN ('STOLEN','FRAUD','LOST') AND 
        CARD_STATUS = 'CANCELED'

), 

DISPUTES AS (
    SELECT A.TRANSACTION_UUID AS DISPUTE_TRANSACTION_UUID,
    A.SOURCE_TRANSACTION_ID AS POSTED_TRANSACTION_UUID, 
    A.DISPUTE_REASON, 
    A.DISPUTE_EXPLANATION, 
    C.TRANSACTION_UUID AS AUTH_TRANSACTION_UUID,
    --A.ORIGINAL_ID,  
    --B.SOURCE_TRANSACTION_ID AS TRANSACTION_SOURCE, 
    B.ORIGINAL_ID AS TRANSACTION_ORIGINAL, 
    A.CONSUMER_UUID AS DISPUTE_CONSUMER_UUID, 
    A.TRANSACTION_PENDING_AT_EST AS DISPUTE_START,
    A.UPDATED_AT_EST AS DISPUTE_UPDATED,
    A.TRANSACTION_STATUS AS DISPUTE_STATUS,
    --A.TRANSACTION_CONFIRMED_AT_EST AS DISPUTE_CONFIRMED, A.TRANSACTION_REJECTED_AT_EST AS DISPUTE_REJECTED,
    A.PURCHASE_METHOD, 
    A.PURCHASE_CATEGORY, 
    A.PURCHASE_CATEGORY_CODE, 
    A.MERCHANT_NAME, 
    A.PURCHASE_MERCHANT_NAME, 
    A.AMOUNT, A.AUTH_USER_UUID,
    B.TRANSACTION_CONFIRMED_AT_EST AS TRANSACTION_DATE_EDW,
    D.*
  
    FROM EDW_PRD.MARTS.MRT_PUBLIC__FCT_TRANSACTIONS A
    LEFT JOIN EDW_PRD.MARTS.MRT_PUBLIC__FCT_TRANSACTIONS B
        ON A.SOURCE_TRANSACTION_ID = B.TRANSACTION_UUID
        --AND A.TRANSACTION_TYPE = 'DISPUTE'
        --AND B.TRANSACTION_TYPE = 'TRANSACTION'
    LEFT JOIN EDW_PRD.MARTS.MRT_PUBLIC__FCT_TRANSACTIONS C
        --ON B.ORIGINAL_ID = C.SOURCE_TRANSACTION_ID
        ON B.ORIGINAL_ID = C.SOURCE_TRANSACTION_ID
    LEFT JOIN EDW_PRD.STG_GOOGLE_SHEETS.STG_GOOGLE_SHEETS__DISPUTES_LOG D
        ON ((A.TRANSACTION_UUID = D.TRANSACTION_UUID) OR
        (A.SOURCE_TRANSACTION_ID = D.TRANSACTION_UUID) OR
        (C.TRANSACTION_UUID = D.TRANSACTION_UUID))
        --AND A.PURCHASE_MERCHANT_NAME = 'KEY-DROP'
    WHERE A.TRANSACTION_TYPE ='DISPUTE'), 

SMS_DATA AS (
    SELECT
        A.CREATED_AT_EST,
        C.CREATED_AT_EST AS STEP_CREATED_AT_EST,
        dateadd(day, 2, C.CREATED_AT_EST) as allowlist_window,
        A.EXECUTION_ID,
        B.CONSUMER_UUID,
        B.SOURCE_UUID,
        B.Transactionamount as AMOUNT,
        B.CARDNAME AS CARD_NAME,
        C.EXECUTION_STEP_NAME,
        C.TRANSITIONED_FROM,
        C.TRANSITIONED_TO,
        
        CASE WHEN C.TRANSITIONED_FROM IN ('sardine_AllowListTransaction', 'es_Sardine_AllowListTransaction', 'allowListTransaction', 'es_allowListTransaction') -- ('sardine_AllowListTransaction', 'es_Sardine_AllowListTransaction') 
        THEN TRUE 
        ELSE FALSE END AS IS_NOT_FRAUD_SMS,
        
        CASE WHEN C.TRANSITIONED_FROM IN ('lockCardSardine', 'es_lockCardSardine', 'lockCard', 'es_lockCard','confirmedFraud','es_confirmedFraud') -- ('lockCardSardine', 'es_lockCardSardine') 
        THEN TRUE 
        ELSE FALSE END AS IS_FRAUD_SMS,
        
        CASE WHEN C.TRANSITIONED_FROM NOT IN ('sardine_AllowListTransaction', 'es_Sardine_AllowListTransaction', 'allowListTransaction', 'es_allowListTransaction','lockCardSardine', 'es_lockCardSardine','confirmedFraud','es_confirmedFraud') -- ('sardine_AllowListTransaction', 'es_Sardine_AllowListTransaction', 'lockCardSardine', 'es_lockCardSardine') 
        AND A.EXECUTION_STATUS = 'ended' 
        THEN TRUE 
        ELSE FALSE END AS IS_NO_REPLY_SMS,
        
        CASE WHEN C.TRANSITIONED_FROM IN ('unrecognizedReply', 'es_unrecognizedReply')  THEN TRUE 
        ELSE FALSE END AS HAS_UNRECOGNIZED_SMS,
        
        A.EXECUTION_STATUS AS EXECUTION_STATUS_SMS,
        
        CASE WHEN C.TRANSITIONED_FROM IN ('sardine_AllowListTransaction', 'es_Sardine_AllowListTransaction', 'allowListTransaction', 'es_allowListTransaction','confirmedFraud','es_confirmedFraud') THEN 0          
            ELSE 1 -- Assign lowest priority to other values
            END AS PRIORITY
    
    FROM EDW_PRD.STG_TWILIO.STG_TWILIO__EXECUTIONS A
        LEFT JOIN EDW_PRD.STG_PUBLIC.STG_PUBLIC__SERVICE_EVENT_TWILIO_EXECUTION_LOG B on A.EXECUTION_ID = B.EVENT_ID
        LEFT JOIN EDW_PRD.STG_TWILIO.STG_TWILIO__EXECUTION_STEPS C ON A.EXECUTION_ID = C.EXECUTION_ID // AND C.TRANSITIONED_FROM IN ('sardine_AllowListTransaction', 'es_Sardine_AllowListTransaction', 'lockCard','lockCardSardine', 'es_lockCardSardine','unrecognizedReply', 'es_unrecognizedReply')
        
    WHERE A.FLOW_ID = 'FWb8dc994e196fb45768d303a02db132e1'
        AND TO_DATE(A.CREATED_AT_EST) >= '2024-10-01'
),

SMS_DATA_DEDUPED AS (
    SELECT *
    FROM SMS_DATA
    QUALIFY ROW_NUMBER() OVER (PARTITION BY EXECUTION_ID ORDER BY PRIORITY ASC) = 1
), 

DPD_data as (
select
t1.activity_date,
t1.credit_line_uuid,
  t1.total_balance,
  t1.days_past_due,
  case
    when (
      t1.days_past_due is null
      or t1.days_past_due = 0
    ) then 'A. current'
    when (t1.days_past_due between 1 and 29) then 'B. 1-29'
    when (t1.days_past_due between 30 and 59) then 'C. 30-59'
    when (t1.days_past_due between 60 and 89) then 'D. 60-89'
    when (t1.days_past_due between 90 and 119) then 'E. 90-119'
    when (t1.days_past_due between 120 and 149) then 'F. 120-149'
    when (t1.days_past_due between 150 and 179) then 'G. 150-179'
    when (t1.days_past_due >= 180) then 'H. 180+'
    else 'error'
  end as DPD_Bucket,
from
  Edw_prd.reports.rpt_public__fct_loan_tape t1
  left join EDW_PRD.MARTS_CONVERSION.MRT_CBH__DIM_PRIMARY_PROFILE_RECONCILE_MERGED t2 on t1.credit_line_uuid = t2.credit_line_uuid
WHERE
  to_date (t1.ACTIVITY_DATE) = LAST_DAY(DATEADD('month', -1, current_date()))
  and t1.merchant_name in ('Crate & Barrel', 'CB2') 

)


select 
DPD_Bucket, 
count(distinct sc.credit_line_uuid),
sum(total_balance) as amount
from dpd_data sc
group by 1

UNION


select 'charge off' as dpd_bucket, count(distinct product_account_uuid), sum(total_charged_off)
from EDW_PRD.reports.rpt_public__fct_daily_account_balance 
where EOD_status = 'CHARGE_OFF' 
and merchant_name in ('Crate & Barrel', 'CBH')
and month(date_trunc('month', activity_date)) = month(date_trunc('month', current_date())) - 1
and is_test_account = False

UNION

select 'disputes' as DPD_bucket, count(distinct product_account_uuid) as disputes, sum(amount) as amount from edw_prd.marts.mrt_public__fct_transactions 
where transaction_type = 'DISPUTE'
and merchant_combname = 'CBH'
and month(date_trunc('month', TRANSACTION_PENDING_AT_EST)) = month(date_trunc('month', current_date())) - 1

UNION

select 
'fraud' as DPD_bucket, count(distinct a.product_account_uuid) as fraud, sum(a.amount*-1) as amount 
from edw_prd.marts.mrt_public__fct_transactions a
LEFT JOIN EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNT_Status_events C ON A.PRODUCT_ACCOUNT_UUID = C.PRODUCT_ACCOUNT_UUID AND C.IS_LATEST = 'TRUE'
LEFT JOIN DISPUTES D ON A.TRANSACTION_UUID = D.AUTH_TRANSACTION_UUID
LEFT JOIN CARD_REISSUE F ON A.TRANSACTION_UUID = F.TRANSACTION_UUID
LEFT JOIN SMS_DATA_DEDUPED H on A.SOURCE_TRANSACTION_ID = H.SOURCE_UUID
where a.merchant_combname = 'CBH'
and ((PURCHASE_AUTH_REJECTION_REASON not in ('blocked_by_feedzai_rules', 'blocked_by_sardine_rules')) -- not blocked by fraud check
or ((D.DISPUTE_REASON = 'FRAUDULENT' OR UPPER(D.REASON_FOR_DISPUTE) LIKE ('%UNAUTHORIZED%')) AND WORKFLOW_STAGE = 'Resolution' AND BILLING_ERROR_STATUS_RESOLUTION = 'Borrower favor') -- associated fraud dispute
or (status_reason_code in ('THIRD_PARTY_FRAUD','ID_THEFT', 'FIRST_PARTY_FRAUD')) -- account tagged as fraud
or (F.CANCEL_REASON IN ('STOLEN','FRAUD','LOST')) -- card canceled due to fraud
or (IS_FRAUD_SMS = TRUE)) --SMS confirmation
and transaction_status in ('CONFIRMED', 'PENDING', 'CANCELLED')
and month(date_trunc('month', TRANSACTION_PENDING_AT_EST)) = month(date_trunc('month', current_date())) - 1
;
