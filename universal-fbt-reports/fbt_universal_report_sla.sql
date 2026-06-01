with fbt_brands as (
    select brands.brand_id
    from edw_prd.marts.mrt_zendesk__dim_brands as brands
    inner join edw_prd.marts.mrt_public__dim_products as products
        on brands.merchant_name = products.merchant_name
    where products.bank_partner = 'FIRST BANK & TRUST'
)

, zendesk_ticket_calls_summary as (
    select *
    from edw_prd.marts.mrt_zendesk__fct_ticket_calls_summary
)

, call_center_response_time as (
    select
        'Call Center Response Time within 120 seconds' as statistic
        , count(distinct case
            when call_direction = 'inbound'
                and (custom_reason_code <> 'general__testing' or custom_reason_code is null)
                and is_overflowed = 'FALSE'
                and (ivr_action = 'group' or ivr_action is null)
                and is_call_outside_business_hours = 'FALSE'
                and (call_completion_status not in ('abandoned_in_ivr') or call_completion_status is null)
                and call_wait_time_seconds <= 120
                then call_id
        end)
        /
        nullif(count(distinct case
            when call_direction = 'inbound'
                and (custom_reason_code <> 'general__testing' or custom_reason_code is null)
                and is_overflowed = 'FALSE'
                and (ivr_action = 'group' or ivr_action is null)
                and is_call_outside_business_hours = 'FALSE'
                and (call_completion_status not in ('abandoned_in_ivr') or call_completion_status is null)
                then call_id
        end), 0) as num
    from zendesk_ticket_calls_summary
    where 1 = 1
        and date_trunc('month', coalesce(call_created_at, ticket_created_at)) = dateadd(month, -1, date_trunc('month', current_date()))
        and custom_reason_code is not null
        and brand_id in (select brand_id from fbt_brands)
)

, chat_response_time as (
    select
        'Chat Response Time' as statistic
        , null as num
)

, email_response_time as (
    select
        'Email Response Time' as statistic
        , null as num
)

, application_system_uptime as (
    select
        'Application System Uptime' as statistic
        , 0.9988 as num
)

, payment_processing as (
    select
        'Payment Processing' as statistic
        , 0.9991 as num
)

, authorization_availability_linked as (
    select
        'Authorization Availability Linked' as statistic
        , 0.99998 as num
)

, final as (
    select * from call_center_response_time
    union all
    select * from chat_response_time
    union all
    select * from email_response_time
    union all
    select * from application_system_uptime
    union all
    select * from payment_processing
    union all
    select * from authorization_availability_linked
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_sla'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.statistic
    , final.num
    , 'fbt_universal_report_sla' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
