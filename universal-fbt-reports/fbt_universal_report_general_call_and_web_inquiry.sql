with fbt_brands as (
    select brands.brand_id
    from edw_prd.marts.mrt_zendesk__dim_brands as brands
    inner join edw_prd.marts.mrt_public__dim_products as products
        on brands.merchant_name = products.merchant_name
    where products.bank_partner = 'FIRST BANK & TRUST'
)

, summary as (
    select *
    from edw_prd.marts.mrt_zendesk__fct_ticket_calls_summary
)

, call_statistics as (
    select
        'calls offered' as statistic
        , count(distinct case
            when call_direction = 'inbound'
                and (custom_reason_code <> 'general__testing' or custom_reason_code is null)
                and is_overflowed = 'FALSE'
                and (ivr_action = 'group' or ivr_action is null)
                and is_call_outside_business_hours = 'FALSE'
                and (call_completion_status not in ('abandoned_in_ivr') or call_completion_status is null)
                then call_id
        end) as num
    from summary
    where 1 = 1
        and date_trunc('month', coalesce(call_created_at, ticket_created_at)) = dateadd(month, -1, date_trunc('month', current_date()))
        and brand_id in (select brand_id from fbt_brands)

    union all

    select
        'calls handled' as statistic
        , count(distinct case
            when call_direction = 'inbound'
                and (custom_reason_code <> 'general__testing' or custom_reason_code is null)
                and is_overflowed = 'FALSE'
                and (ivr_action = 'group' or ivr_action is null)
                and is_call_outside_business_hours = 'FALSE'
                and call_completion_status in ('abandoned_on_hold', 'completed')
                and has_customer_requested_voicemail = false
                then call_id
        end) as num
    from summary
    where 1 = 1
        and date_trunc('month', coalesce(call_created_at, ticket_created_at)) = dateadd(month, -1, date_trunc('month', current_date()))
        and brand_id in (select brand_id from fbt_brands)

    union all

    select
        'calls abandoned' as statistic
        , count(distinct case
            when call_direction = 'inbound'
                and (custom_reason_code <> 'general__testing' or custom_reason_code is null)
                and is_overflowed = 'FALSE'
                and (ivr_action = 'group' or ivr_action is null)
                and is_call_outside_business_hours = 'FALSE'
                and call_completion_status in ('abandoned_in_voicemail', 'not_answered', 'abandoned_in_queue')
                and has_customer_requested_voicemail = false
                then call_id
        end) as num
    from summary
    where 1 = 1
        and date_trunc('month', coalesce(call_created_at, ticket_created_at)) = dateadd(month, -1, date_trunc('month', current_date()))
        and brand_id in (select brand_id from fbt_brands)

    union all

    select
        'calls outbound' as statistic
        , count(distinct case
            when call_direction = 'outbound'
                then call_id
        end) as num
    from summary
    where 1 = 1
        and date_trunc('month', coalesce(call_created_at, ticket_created_at)) = dateadd(month, -1, date_trunc('month', current_date()))
        and brand_id in (select brand_id from fbt_brands)

    union all

    select
        'avg speed of answer in seconds' as statistic
        , avg(distinct case
            when call_direction = 'inbound'
                and (custom_reason_code <> 'general__testing' or custom_reason_code is null)
                and is_overflowed = 'FALSE'
                and (ivr_action = 'group' or ivr_action is null)
                and is_call_outside_business_hours = 'FALSE'
                and (call_completion_status not in ('abandoned_in_ivr') or call_completion_status is null)
                then call_wait_time_seconds
        end) as num
    from summary
    where 1 = 1
        and date_trunc('month', coalesce(call_created_at, ticket_created_at)) = dateadd(month, -1, date_trunc('month', current_date()))
        and brand_id in (select brand_id from fbt_brands)

    union all

    select
        'average handle time in seconds' as statistic
        , round(avg(call_talk_time_seconds + call_hold_time_seconds + call_wrap_up_time_seconds), 2) as num
    from summary
    where
        call_direction = 'inbound'
        and call_talk_time_seconds > 0
        and date_trunc('month', coalesce(call_created_at, ticket_created_at)) = dateadd(month, -1, date_trunc('month', current_date()))
        and brand_id in (select brand_id from fbt_brands)
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_general_call_and_web_inquiry'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    call_statistics.statistic
    , call_statistics.num
    , 'fbt_universal_report_general_call_and_web_inquiry' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from call_statistics
left join stakeholder_review
    on 1 = 1
