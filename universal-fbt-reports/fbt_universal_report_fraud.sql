with products as (
    select *
    from edw_prd.marts.mrt_public__dim_products
    where bank_partner = 'FIRST BANK & TRUST'
)

, fbt_brands as (
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

, zendesk_ticket_comments as (
    select *
    from edw_prd.marts.mrt_zendesk__fct_ticket_comments
)

, profiles as (
    select *
    from edw_prd.marts.mrt_public__dim_profiles
)

, product_accounts as (
    select *
    from edw_prd.marts.mrt_public__dim_product_accounts
)

, zendesk_ticket_tag_history as (
    select *
    from edw_prd.stg_zendesk.stg_zendesk__ticket_tag_history
)

, ticket_comments_distinct as (
    select distinct
        ticket_id as ticket_id_2
        , call_from_number
    from zendesk_ticket_comments
    where call_from_number is not null
)

, phone_processing as (
    select
        case when left(regexp_replace(ticket_comments_distinct.call_from_number, '[^0-9]', ''), 1) = '1' and length(regexp_replace(ticket_comments_distinct.call_from_number, '[^0-9]', '')) = 11
                then right(regexp_replace(ticket_comments_distinct.call_from_number, '[^0-9]', ''), 10)
            else regexp_replace(ticket_comments_distinct.call_from_number, '[^0-9]', '')
        end as user_phone_fix
        , zendesk_ticket_calls_summary.call_id
        , zendesk_ticket_calls_summary.ticket_id
    from zendesk_ticket_calls_summary
    left join ticket_comments_distinct
        on zendesk_ticket_calls_summary.ticket_id = ticket_comments_distinct.ticket_id_2
)

, profiles_distinct as (
    select distinct
        consumer_uuid
        , phone
    from profiles
)

, uuid_lookup as (
    select
        phone_processing.call_id
        , phone_processing.user_phone_fix
        , profiles_distinct.consumer_uuid
        , phone_processing.ticket_id
    from phone_processing
    left join profiles_distinct
        on phone_processing.user_phone_fix = profiles_distinct.phone
)

, closed_by_merge_tickets as (
    select distinct ticket_id
    from zendesk_ticket_tag_history
    where tag_name = 'closed_by_merge'
)

, final as (
    select
        product_accounts.product_account_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
        , zendesk_ticket_calls_summary.custom_imprint_uuid
        , coalesce(zendesk_ticket_calls_summary.ticket_created_at_est, zendesk_ticket_calls_summary.call_created_at_est) as fraud_timestamp
        , zendesk_ticket_calls_summary.custom_reason_code
    from zendesk_ticket_calls_summary
    left join uuid_lookup
        on zendesk_ticket_calls_summary.call_id = uuid_lookup.call_id
    left join product_accounts
        on zendesk_ticket_calls_summary.custom_imprint_uuid = product_accounts.consumer_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    left join closed_by_merge_tickets
        on zendesk_ticket_calls_summary.ticket_id = closed_by_merge_tickets.ticket_id
    where zendesk_ticket_calls_summary.brand_id in (select brand_id from fbt_brands)
        and zendesk_ticket_calls_summary.ticket_group = 'Fraud'
        and closed_by_merge_tickets.ticket_id is null
        and zendesk_ticket_calls_summary.custom_reason_code <> 'general__testing'
        and date_trunc('month', coalesce(zendesk_ticket_calls_summary.ticket_created_at_est, zendesk_ticket_calls_summary.call_created_at_est)) = dateadd(month, -1, date_trunc('month', current_date()))
    order by fraud_timestamp
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_fraud'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_fraud' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
