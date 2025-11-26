
--needs to be updated every month

select
'Call Center Response Time within 120 seconds' as statistic, 

count(distinct case when call_direction = 'inbound' and (custom_reason_code <> 'general__testing' OR custom_reason_code IS NULL) and is_overflowed = 'FALSE' and (IVR_ACTION = 'group' or IVR_ACTION IS NULL) and IS_CALL_OUTSIDE_BUSINESS_HOURS = 'FALSE' and (CALL_COMPLETION_STATUS NOT IN ('abandoned_in_ivr') or CALL_COMPLETION_STATUS is null) and call_wait_time_seconds <= 120 then call_id else null end)
/
count(distinct case when call_direction = 'inbound' and (custom_reason_code <> 'general__testing' OR custom_reason_code IS NULL) and is_overflowed = 'FALSE' and (IVR_ACTION = 'group' or IVR_ACTION IS NULL) and IS_CALL_OUTSIDE_BUSINESS_HOURS = 'FALSE' and (CALL_COMPLETION_STATUS NOT IN ('abandoned_in_ivr') or CALL_COMPLETION_STATUS is null) then call_id else null end) as num, 

from edw_prd.marts.mrt_zendesk__fct_ticket_calls_summary

where 

month(date_trunc('month', coalesce(call_created_at, ticket_created_at))) = month(date_trunc('month', current_date())) - 1
and custom_reason_code is not null
and brand_id in ('40954903794580', '36869591739412', '42879080717716')

union all

select 'Chat Response Time' as statistic, null as num

union all

select 'Email Response Time' as statistic, null as num

union all

select 'Application System Uptime' as statistic, .99226 as num

union all

select 'Payment Processing' as statistic, .99592 as num

union all

select 'Authorization Availability Linked' as statistic, .9993 as num;
