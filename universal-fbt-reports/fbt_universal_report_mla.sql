with products as (
    select *
    from edw_prd.marts.mrt_public__dim_products
    where bank_partner = 'FIRST BANK & TRUST'
)

, service_event_hardpull as (
    select *
    from edw_prd.stg_public.stg_public__service_event_hardpull
)

, product_accounts as (
    select *
    from edw_prd.marts.mrt_public__dim_product_accounts
)

, final as (
    select
        product_accounts.product_account_uuid
        , product_accounts.consumer_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
        , service_event_hardpull.mla_flag
    from service_event_hardpull
    left join product_accounts
        on service_event_hardpull.application_uuid = product_accounts.application_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where service_event_hardpull.mla_flag = true
        and product_accounts.is_test_account = false
        and date_trunc('month', service_event_hardpull.updated_at_est) = dateadd(month, -1, date_trunc('month', current_date()))
    qualify row_number() over (partition by service_event_hardpull.application_uuid order by service_event_hardpull.updated_at_est asc) = 1
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_mla'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_mla' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
