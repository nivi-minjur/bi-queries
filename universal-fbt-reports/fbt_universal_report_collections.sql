with products as (
    select *
    from edw_prd.marts.mrt_public__dim_products
    where bank_partner = 'FIRST BANK & TRUST'
)

, loan_tape as (
    select *
    from edw_prd.marts.rpt_public__fct_loan_tape
)

, product_accounts as (
    select *
    from edw_prd.marts.mrt_public__dim_product_accounts
)

, product_account_status_events as (
    select *
    from edw_prd.marts.mrt_public__dim_product_account_status_events
)

, delinquency_status as (
    select
        product_accounts.product_account_uuid
        , product_accounts.consumer_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
        , dateadd('day', -1 * loan_tape.days_past_due, last_day(dateadd('month', -1, current_date()))) as started_at_est
        , 'DELINQUENT' as status
    from loan_tape
    inner join product_accounts
        on loan_tape.credit_line_uuid = product_accounts.credit_line_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where loan_tape.activity_date = last_day(dateadd('month', -1, current_date()))
        and loan_tape.days_past_due > 0
)

, deceased_cccs_status as (
    select
        product_account_status_events.product_account_uuid
        , product_accounts.consumer_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
        , product_account_status_events.started_at_est
        , case when product_account_status_events.status_reason in ('CCCS', 'DECEASED') then product_account_status_events.status_reason
        end as status
    from product_account_status_events
    inner join product_accounts
        on product_account_status_events.product_account_uuid = product_accounts.product_account_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where product_account_status_events.status_reason in ('CCCS', 'DECEASED')
        and date_trunc('month', product_account_status_events.started_at_est) = dateadd(month, -1, date_trunc('month', current_date()))
        and product_accounts.is_test_account = false
    qualify row_number() over (partition by product_account_status_events.product_account_uuid order by product_account_status_events.started_at_est asc) = 1
)

, final as (
    select *
    from delinquency_status

    union

    select *
    from deceased_cccs_status
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_collections'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_collections' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
