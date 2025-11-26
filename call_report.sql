-- A report of all calls/call attempts that occurred in the prior month, including date/time stamp, category of call, inbound/outbound and account identifier. To be provided to for QA review purposes unless direct access is agreed upon. Separate reports may be required based on vendors.

-- Only customer support agent calls required

WITH uuid_lookup as (SELECT 
call_id, user_phone_fix, consumer_uuid, custom_imprint_uuid, ticket_id
FROM (SELECT case when left(REGEXP_REPLACE(call_from_number, '[^0-9]', ''),1) = '1' and length(REGEXP_REPLACE(call_from_number, '[^0-9]', '')) = 11 then right(REGEXP_REPLACE(call_from_number, '[^0-9]', ''), 10) else REGEXP_REPLACE(call_from_number, '[^0-9]', '') end as user_phone_fix,call_id, ticket_id, custom_imprint_uuid
FROM EDW_PRD.MARTS.MRT_ZENDESK__FCT_TICKET_CALLS_SUMMARY calls
left join (SELECT distinct ticket_id as ticket_id_2, call_from_number FROM EDW_PRD.MARTS.MRT_ZENDESK__FCT_TICKET_COMMENTS where call_from_number is not null) ticket_comments
on calls.ticket_id=ticket_comments.ticket_id_2
) a 
left join (SELECT distinct consumer_uuid, phone  FROM edw_prd.marts.mrt_public__dim_profiles) c on a.user_phone_fix=c.phone)


select 
pa.product_account_uuid, 
coalesce(calls.custom_imprint_uuid, uuid.consumer_uuid) as consumer_uuid,
coalesce(call_created_at_est, ticket_created_at_est) as call_timestamp, 
calls.custom_reason_code as call_category,
calls.ticket_id,
case when calls.brand_id in ('40954903794580', '36869591739412', '42879080717716') then 'CBH'
else 'Not CBH' end as program_name, 
call_direction
from
EDW_PRD.MARTS.MRT_ZENDESK__FCT_TICKET_CALLS_SUMMARY calls
left join
uuid_lookup uuid
on calls.call_id = uuid.call_id
left join 
EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS pa
on coalesce(calls.custom_imprint_uuid, uuid.consumer_uuid) = pa.consumer_uuid
and program_name = pa.merchant_combname
left join (select distinct ticket_id from EDW_PRD.STG_ZENDESK.STG_ZENDESK__TICKET_TAG_HISTORY where tag_name = 'closed_by_merge') ta
on ta.ticket_id = calls.ticket_id
where 
((call_direction = 'inbound' 
and (calls.custom_reason_code <> 'general__testing' OR calls.custom_reason_code IS NULL) 
and is_overflowed = 'FALSE' 
and (IVR_ACTION = 'group' or IVR_ACTION IS NULL) 
and IS_CALL_OUTSIDE_BUSINESS_HOURS = 'FALSE' 
and (CALL_COMPLETION_STATUS NOT IN ('abandoned_in_ivr') or CALL_COMPLETION_STATUS is null) ) 
or call_direction = 'outbound')
and month(date_trunc('month', call_timestamp)) = month(date_trunc('month', current_date())) - 1
and brand_id in ('40954903794580', '36869591739412', '42879080717716') --CBH brand ids
and ta.ticket_id is null
and date_trunc('day', call_timestamp) >= '2025-10-10'
order by call_timestamp desc
; 
