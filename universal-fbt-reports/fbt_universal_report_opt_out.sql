with products as (
    select *
    from edw_prd.marts.mrt_public__dim_products
    where bank_partner = 'FIRST BANK & TRUST'
)

, braze_subscription_event as (
    select *
    from edw_prd.stg_braze.stg_braze__subscription_event
)

, braze_email_event as (
    select *
    from edw_prd.stg_braze.stg_braze__email_event
)

, product_accounts as (
    select *
    from edw_prd.marts.mrt_public__dim_product_accounts
)

, fbt_consumers as (
    select distinct
        product_accounts.consumer_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
    from product_accounts
    inner join products
        on product_accounts.product_uuid = products.product_uuid
)

, email_unsubscribes as (
    select distinct external_user_id as accnt_id2
    from braze_email_event
    where upper(event_type) = 'USERS.MESSAGES.EMAIL.UNSUBSCRIBE'
)

, final as (
    select distinct
        coalesce(braze_subscription_event.external_user_id, email_unsubscribes.accnt_id2) as consumer_uuid
        , fbt_consumers.product_uuid
        , fbt_consumers.merchant_name
        , braze_subscription_event.channel
        , braze_subscription_event.time_at_est
    from braze_subscription_event
    full outer join email_unsubscribes
        on braze_subscription_event.external_user_id = email_unsubscribes.accnt_id2
    inner join fbt_consumers
        on coalesce(braze_subscription_event.external_user_id, email_unsubscribes.accnt_id2) = fbt_consumers.consumer_uuid
    where upper(braze_subscription_event.subscription_status) = 'UNSUBSCRIBED'
        and date_trunc('month', braze_subscription_event.time_at_est) = dateadd(month, -1, date_trunc('month', current_date()))
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_opt_out'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_opt_out' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
