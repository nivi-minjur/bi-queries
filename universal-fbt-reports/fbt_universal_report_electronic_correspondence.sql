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

, product_accounts as (
    select *
    from edw_prd.marts.mrt_public__dim_product_accounts
)

, braze_email_event as (
    select *
    from edw_prd.stg_braze.stg_braze__email_event
)

, inbound_emails as (
    select
        product_accounts.product_account_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
        , zendesk_ticket_calls_summary.custom_imprint_uuid as consumer_uuid
        , zendesk_ticket_calls_summary.ticket_created_at_est as email_timestamp
        , 'inbound' as direction
    from zendesk_ticket_calls_summary
    inner join product_accounts
        on zendesk_ticket_calls_summary.custom_imprint_uuid = product_accounts.consumer_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where zendesk_ticket_calls_summary.request_channel = 'email'
        and date_trunc('month', zendesk_ticket_calls_summary.ticket_created_at_est) = dateadd(month, -1, date_trunc('month', current_date()))
        and zendesk_ticket_calls_summary.brand_id in (select brand_id from fbt_brands)
)

, outbound_emails as (
    select
        product_accounts.product_account_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
        , zendesk_ticket_calls_summary.custom_imprint_uuid as consumer_uuid
        , zendesk_ticket_calls_summary.ticket_created_at_est as email_timestamp
        , 'outbound' as direction
    from zendesk_ticket_calls_summary
    inner join product_accounts
        on zendesk_ticket_calls_summary.custom_imprint_uuid = product_accounts.consumer_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where zendesk_ticket_calls_summary.request_channel = 'email'
        and date_trunc('month', zendesk_ticket_calls_summary.ticket_created_at_est) = dateadd(month, -1, date_trunc('month', current_date()))
        and zendesk_ticket_calls_summary.brand_id in (select brand_id from fbt_brands)
)

, braze_emails as (
    select
        product_accounts.product_account_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
        , braze_email_event.external_user_id as consumer_uuid
        , braze_email_event.time_at_est as email_timestamp
        , 'outbound_braze' as direction
    from braze_email_event
    inner join product_accounts
        on braze_email_event.external_user_id = product_accounts.consumer_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where braze_email_event.event_type in ('users.messages.email.Send')
        and date_trunc('month', braze_email_event.time_at_est) = dateadd(month, -1, date_trunc('month', current_date()))
)

, final as (
    select * from inbound_emails
    union all
    select * from outbound_emails
    union all
    select * from braze_emails
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_electronic_correspondence'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_electronic_correspondence' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
