with ZendeskComplaintsTemp as (
SELECT a.*,
case
            when via_channel in ('native_messaging', 'chat') then 'chat'
            when via_channel in ('web', 'answer_bot_for_web_widget') then 'email'
            else via_channel
        end as channel,
CONVERT_TIMEZONE('America/New_York',a.ticket_created_at) as created_at_est,
CAST(DATE_TRUNC('day',created_at_est) AS DATE) as created_date,
SUBSTR(to_varchar(created_at_est),1,7) as year_month,
DATE_TRUNC('week',created_date) as weekstart,
b.brand_name as brand,
solve_date_1
FROM edw_prd.stg_zendesk.stg_zendesk__tickets a
LEFT JOIN edw_prd.stg_zendesk.stg_zendesk__brands b on a.brand_id = b.brand_id
LEFT JOIN (
SELECT ticket_id, CAST(DATE_TRUNC('day',CONVERT_TIMEZONE('America/New_York',updated_at)) AS DATE) as solve_date_1
FROM
edw_prd.stg_zendesk.stg_zendesk__ticket_field_history a
where field_name in ('status') and field_value in ('solved')
qualify row_number() over (partition by ticket_id order by updated_at desc) = 1
) c on a.ticket_id = c.ticket_id
where custom_complaint = 'TRUE'),

--get UUID for merged tickets to improve coverage
merged_ticket_uuid as(
SELECT distinct
case when custom_imprint_uuid IS NULL OR custom_imprint_uuid = '' then 1 else 0 end as is_null,
ticket_id,
merged_tickets,
custom_imprint_uuid
FROM (
select a.*,SUBSTR(to_varchar(CONVERT_TIMEZONE('America/New_York',a.ticket_created_at)),1,7) as year_month, index as array_index, value as merged_tickets  
from edw_prd.stg_zendesk.stg_zendesk__tickets as a, lateral flatten(input => a.merged_ticket_ids) as merged_tickets
where merged_ticket_ids not like ('[]') and (CUSTOM_DOES_CONTACT_HAVE_ACCOUNT_WITH_IMPRINT NOT IN ('no_uuid_needed') OR CUSTOM_DOES_CONTACT_HAVE_ACCOUNT_WITH_IMPRINT IS NULL))
where is_null != 1),

ZendeskComplaintsSolve as (
SELECT a.*, b.merged_tickets, c.solve_date_1 as solve_date_2
from ZendeskComplaintstemp a
left join merged_ticket_uuid b on a.ticket_id = b.ticket_id
left join (
SELECT ticket_id, CAST(DATE_TRUNC('day',CONVERT_TIMEZONE('America/New_York',updated_at)) AS DATE) as solve_date_1
FROM
edw_prd.stg_zendesk.stg_zendesk__ticket_field_history a
where field_name in ('status') and field_value in ('solved')
qualify row_number() over (partition by ticket_id order by updated_at desc) = 1
) c on b.merged_tickets = c.ticket_id
where a.solve_date_1 is null and c.solve_date_1 is not null
qualify row_number() over (partition by a.ticket_id order by c.solve_date_1 desc) = 1)

SELECT a.custom_issue_summary, a.custom_complaint_summary, a.custom_complaint_resolution, coalesce(a.solve_date_1,solve_date_2) as solve_date
FROM ZendeskComplaintsTemp a
LEFT JOIN ZendeskComplaintsSolve b on a.ticket_id = b.ticket_id
where a.brand_id in ('40954903794580', '36869591739412', '42879080717716') 
and month(date_trunc('month', solve_date)) = month(date_trunc('month', current_date())) - 1
and a.custom_issue_summary <> 'Test'
and date_trunc('day', solve_date) >= '2025-10-10'
and a.custom_criticality IN ('tier_2','tier_3');