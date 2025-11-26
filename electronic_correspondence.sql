-- A report that lists all Emails received/sent within a month, along with date received/sent and account identifier.
-- emails can't be sent through braze

select pa.product_account_uuid, ticket_created_at_est as timestamp, 
case when (users.user_signature not like '%Imprint%' or users.user_signature is null) and submitter_role not in ('agent', 'admin') then 'inbound'
else 'outbound' end as direction,
'customer support email' as correspondence_type from EDW_PRD.MARTS.MRT_ZENDESK__FCT_TICKET_CALLS_SUMMARY calls
left join
EDW_PRD.MARTS.MRT_ZENDESK__DIM_USERS users
on calls.submitter_user_id = users.user_id
left join
EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS pa
on calls.custom_imprint_uuid = pa.consumer_uuid
where request_channel = 'email'
and users.user_signature not like '%Imprint%' or users.user_signature is null
and brand_id in ('40954903794580', '36869591739412', '42879080717716')
and month(date_trunc('month', ticket_created_at_est)) = month(date_trunc('month', current_date())) - 1

union 

select pa.product_account_uuid, ticket_created_at_est as timestamp, 'inbound' as direction, 'customer support email' as correspondence_type from EDW_PRD.MARTS.MRT_ZENDESK__FCT_TICKET_CALLS_SUMMARY calls
left join 
EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS pa
on calls.custom_imprint_uuid = pa.consumer_uuid
where request_channel = 'email'
and brand_id in ('40954903794580', '36869591739412', '42879080717716')
and month(date_trunc('month', ticket_created_at_est)) = month(date_trunc('month', current_date())) - 1

union

select pa.product_account_uuid, TIME_AT_EST as timestamp, 'outbound' as direction, 'marketing email' as correspondence_type
from EDW_PRD.STG_BRAZE.STG_BRAZE__EMAIL_EVENT ta
join EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS pa
on ta.external_user_id = pa.consumer_uuid
where 1=1
and pa.merchant_combname = 'CBH' 
and event_type in ('users.messages.email.Send') 
and canvas_id in (select distinct canvas_id from EDW_PRD.stg_braze.stg_braze__canvas where canvas_name like '%CBH%')
and month(date_trunc('month', TIME_AT_EST)) = month(date_trunc('month', current_date())) - 1
 ;
