(select
'calls offered' as statistic, 

count(distinct case when call_direction = 'inbound' and (custom_reason_code <> 'general__testing' OR custom_reason_code IS NULL) and is_overflowed = 'FALSE' and (IVR_ACTION = 'group' or IVR_ACTION IS NULL) and IS_CALL_OUTSIDE_BUSINESS_HOURS = 'FALSE' and (CALL_COMPLETION_STATUS NOT IN ('abandoned_in_ivr') or CALL_COMPLETION_STATUS is null) then call_id else null end) as num, 

from edw_prd.marts.mrt_zendesk__fct_ticket_calls_summary

where 

month(date_trunc('month', coalesce(call_created_at, ticket_created_at))) = month(date_trunc('month', current_date())) - 1
and brand_id in ('40954903794580', '36869591739412', '42879080717716') 
and coalesce(call_created_at, ticket_created_at) >= '2025-10-10'
--and merchant_name in ('CB2 Visa Signature® Credit Card', 'Crate & Barrel Visa Signature® Credit Card')
)

UNION

(
select
'calls handled' as statistic, 

count(distinct case when call_direction = 'inbound' and (custom_reason_code <> 'general__testing' OR custom_reason_code IS NULL) and is_overflowed = 'FALSE' and (IVR_ACTION = 'group' or IVR_ACTION IS NULL) and IS_CALL_OUTSIDE_BUSINESS_HOURS = 'FALSE' and CALL_COMPLETION_STATUS IN ('abandoned_on_hold', 'completed') and has_customer_requested_voicemail = FALSE then call_id else null end) as num, 

from edw_prd.marts.mrt_zendesk__fct_ticket_calls_summary

where 

month(date_trunc('month', coalesce(call_created_at, ticket_created_at))) = month(date_trunc('month', current_date())) - 1
and brand_id in ('40954903794580', '36869591739412', '42879080717716') 
and coalesce(call_created_at, ticket_created_at) >= '2025-10-10'
)

UNION

(
select
'calls abandoned' as statistic, 
count(distinct case when call_direction = 'inbound' and (custom_reason_code <> 'general__testing' OR custom_reason_code IS NULL) and is_overflowed = 'FALSE' and (IVR_ACTION = 'group' or IVR_ACTION IS NULL) and IS_CALL_OUTSIDE_BUSINESS_HOURS = 'FALSE' and CALL_COMPLETION_STATUS IN ('abandoned_in_voicemail', 'not_answered', 'abandoned_in_queue') and has_customer_requested_voicemail = FALSE then call_id else null end) as num,

from edw_prd.marts.mrt_zendesk__fct_ticket_calls_summary

where 

month(date_trunc('month', coalesce(call_created_at, ticket_created_at))) = month(date_trunc('month', current_date())) - 1
and brand_id in ('40954903794580', '36869591739412', '42879080717716', '36869591739412')
and coalesce(call_created_at, ticket_created_at) >= '2025-10-10'
)

UNION


(
select
'calls outbound' as statistic, 

count(distinct case when call_direction = 'outbound' then call_id else null end) as num, 
from edw_prd.marts.mrt_zendesk__fct_ticket_calls_summary
where 
month(date_trunc('month', coalesce(call_created_at, ticket_created_at))) = month(date_trunc('month', current_date())) - 1
and brand_id in ('40954903794580', '36869591739412', '42879080717716', '36869591739412') 
and coalesce(call_created_at, ticket_created_at) >= '2025-10-10'
)

UNION

(
select
'avg speed of answer in seconds' as statistic, 

avg(distinct case when call_direction = 'inbound' and (custom_reason_code <> 'general__testing' OR custom_reason_code IS NULL) and is_overflowed = 'FALSE' and (IVR_ACTION = 'group' or IVR_ACTION IS NULL) and IS_CALL_OUTSIDE_BUSINESS_HOURS = 'FALSE' and (CALL_COMPLETION_STATUS NOT IN ('abandoned_in_ivr') or CALL_COMPLETION_STATUS is null) then call_wait_time_seconds else null end) as num, 

from edw_prd.marts.mrt_zendesk__fct_ticket_calls_summary

where 

month(date_trunc('month', coalesce(call_created_at, ticket_created_at))) = month(date_trunc('month', current_date())) - 1
and brand_id in ('40954903794580', '36869591739412', '42879080717716', '36869591739412') 
and coalesce(call_created_at, ticket_created_at) >= '2025-10-10'
)

UNION 

(select
'average handle time in seconds', round(avg(call_talk_time_seconds + call_hold_time_seconds + call_wrap_up_time_seconds),2) as num
from edw_prd.marts.mrt_zendesk__fct_ticket_calls_summary
where 1=1
and call_direction = 'inbound' 
and call_talk_time_seconds > 0 
and month(date_trunc('month', coalesce(call_created_at, ticket_created_at))) = month(date_trunc('month', current_date())) - 1
and brand_id in ('40954903794580', '36869591739412', '42879080717716', '36869591739412') 
and coalesce(call_created_at, ticket_created_at) >= '2025-10-10'
);