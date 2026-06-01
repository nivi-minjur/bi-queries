with products as (
    select *
    from edw_prd.marts.mrt_public__dim_products
    where bank_partner = 'FIRST BANK & TRUST'
)

, transactions as (
    select *
    from edw_prd.marts.mrt_public__fct_transactions
)

, credit_profile_final_apr_history as (
    select *
    from edw_prd.marts.mrt_public__dim_credit_profile_final_apr_history
)

, product_accounts as (
    select *
    from edw_prd.marts.mrt_public__dim_product_accounts
)

, final as (
    select distinct
        product_accounts.product_account_uuid
        , transactions.consumer_uuid
        , transactions.product_uuid
        , transactions.merchant_name
        , transactions.purchase_merchant_name
        , transactions.transaction_confirmed_at_est
        , transactions.transaction_type
        , transactions.amount * -1 as amount
        , transactions.fee_reason
        , transactions.interest_reason
        , credit_profile_final_apr_history.final_apr
    from transactions
    inner join products
        on transactions.product_uuid = products.product_uuid
    left join credit_profile_final_apr_history
        on transactions.credit_line_uuid = credit_profile_final_apr_history.credit_line_uuid
            and transactions.transaction_confirmed_at_est >= credit_profile_final_apr_history.effective_start_at_est
            and transactions.transaction_confirmed_at_est < coalesce(credit_profile_final_apr_history.effective_end_at_est, '9999-12-31')
    left join product_accounts
        on transactions.credit_line_uuid = product_accounts.credit_line_uuid
    where transactions.transaction_status = 'CONFIRMED'
        and transactions.transaction_type <> 'AUTH'
        and date_trunc('month', transactions.transaction_confirmed_at_est) = dateadd(month, -1, date_trunc('month', current_date()))
        and product_accounts.is_test_account = false
        and transactions.is_stmt_pseudo_txn = false
        and transactions.is_pre_conversion_txn = false
    qualify
        row_number() over (
            partition by transactions.transaction_uuid
            order by coalesce(credit_profile_final_apr_history.effective_end_at_est, '9999-12-31') desc
        ) = 1
    order by transactions.transaction_confirmed_at_est desc
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_transaction_history'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_transaction_history' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
