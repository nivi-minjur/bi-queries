with products as (
    select *
    from edw_prd.marts.mrt_public__dim_products
    where bank_partner = 'FIRST BANK & TRUST'
)

, transactions as (
    select *
    from edw_prd.marts.mrt_public__fct_transactions
)

, final as (
    select
        transactions.product_account_uuid
        , transactions.consumer_uuid
        , transactions.product_uuid
        , transactions.merchant_name
        , transactions.transaction_confirmed_at_est
        , transactions.amount * -1 as amount
    from transactions
    inner join products
        on transactions.product_uuid = products.product_uuid
    where 1 = 1
        and date_trunc('month', transactions.created_at_est) = dateadd(month, -1, date_trunc('month', current_date()))
        and transactions.transaction_type = 'CREDIT_RETURN'
        and transactions.purchase_merchant_name = 'Credit balance refunded'
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_credit_balance_refunds'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_credit_balance_refunds' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
