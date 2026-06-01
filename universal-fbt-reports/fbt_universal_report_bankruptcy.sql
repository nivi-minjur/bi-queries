with products as (
    select *
    from edw_prd.marts.mrt_public__dim_products
    where bank_partner = 'FIRST BANK & TRUST'
)

, product_account_status_events as (
    select *
    from edw_prd.marts.mrt_public__dim_product_account_status_events
)

, product_accounts as (
    select *
    from edw_prd.marts.mrt_public__dim_product_accounts
)

, final as (
    select
        product_account_status_events.product_account_uuid
        , product_accounts.consumer_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
        , product_account_status_events.started_at_est
        , product_account_status_events.status_reason
    from product_account_status_events
    inner join product_accounts
        on product_account_status_events.product_account_uuid = product_accounts.product_account_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where product_account_status_events.status_reason = 'BANKRUPTCY'
        and date_trunc('month', product_account_status_events.started_at_est) = dateadd(month, -1, date_trunc('month', current_date()))
        and product_accounts.is_test_account = false
    qualify row_number() over (partition by product_account_status_events.product_account_uuid order by product_account_status_events.started_at_est asc) = 1
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_bankruptcy'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_bankruptcy' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
