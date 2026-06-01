with products as (
    select *
    from edw_prd.marts.mrt_public__dim_products
    where bank_partner = 'FIRST BANK & TRUST'
)

, card_status_history as (
    select *
    from edw_prd.marts.mrt_public__dim_card_status_history
)

, transactions as (
    select *
    from edw_prd.marts.mrt_public__fct_transactions
)

, google_sheets_disputes_log as (
    select *
    from edw_prd.stg_google_sheets.stg_google_sheets__disputes_log
)

, twilio_executions as (
    select *
    from edw_prd.stg_twilio.stg_twilio__executions
)

, stg_public_service_event_twilio_execution_log as (
    select *
    from edw_prd.stg_public.stg_public__service_event_twilio_execution_log
)

, twilio_execution_steps as (
    select *
    from edw_prd.stg_twilio.stg_twilio__execution_steps
)

, loan_tape as (
    select *
    from edw_prd.marts.rpt_public__fct_loan_tape
)

, product_accounts as (
    select *
    from edw_prd.marts.mrt_public__dim_product_accounts
)

, daily_account_balance as (
    select *
    from edw_prd.marts.rpt_public__fct_daily_account_balance
)

, product_account_status_events as (
    select *
    from edw_prd.marts.mrt_public__dim_product_account_status_events
)

, card_reissue as (
    select
        datediff('day', card_status_history.last_ingest_at_est, transactions.created_at_est) as date_diff
        , card_status_history.card_uuid
        , card_status_history.product_account_uuid
        , card_status_history.card_status
        , card_status_history.card_type
        , card_status_history.cancel_reason
        , card_status_history.last_ingest_at_est
        , transactions.created_at_est as txn_created_at_est
        , transactions.transaction_uuid
    from card_status_history
    inner join transactions
        on card_status_history.card_uuid = transactions.card_uuid
            and datediff('day', card_status_history.last_ingest_at_est, transactions.created_at_est) <= 0
            and datediff('day', card_status_history.last_ingest_at_est, transactions.created_at_est) >= -3
    where card_status_history.card_status = 'CANCELED'
)

, disputes as (
    select
        transactions_a.transaction_uuid as dispute_transaction_uuid
        , transactions_a.source_transaction_id as posted_transaction_uuid
        , transactions_a.dispute_reason
        , transactions_a.dispute_explanation
        , transactions_c.transaction_uuid as auth_transaction_uuid
        , transactions_b.original_id as transaction_original
        , transactions_a.consumer_uuid as dispute_consumer_uuid
        , transactions_a.transaction_pending_at_est as dispute_start
        , transactions_a.updated_at_est as dispute_updated
        , transactions_a.transaction_status as dispute_status
        , transactions_a.purchase_method
        , transactions_a.purchase_category
        , transactions_a.purchase_category_code
        , transactions_a.merchant_name
        , transactions_a.purchase_merchant_name
        , transactions_a.amount
    from transactions as transactions_a
    left join transactions as transactions_b
        on transactions_a.source_transaction_id = transactions_b.transaction_uuid
    left join transactions as transactions_c
        on transactions_b.original_id = transactions_c.transaction_uuid
    where transactions_a.transaction_type = 'DISPUTE'
)

, disputes_summary as (
    select
        disputes.*
        , google_sheets_disputes_log.product_type
        , google_sheets_disputes_log.dispute_type
        , google_sheets_disputes_log.current_status as dispute_resolution_status
    from disputes
    left join google_sheets_disputes_log
        on disputes.dispute_transaction_uuid = google_sheets_disputes_log.dispute_transaction_uuid
)

, twilio_collections as (
    select
        twilio_executions.execution_sid
        , twilio_executions.account_sid
        , twilio_executions.date_created
        , stg_public_service_event_twilio_execution_log.consumer_uuid
        , twilio_execution_steps.transitioned_to as execution_result
    from twilio_executions
    inner join stg_public_service_event_twilio_execution_log
        on twilio_executions.execution_sid = stg_public_service_event_twilio_execution_log.execution_sid
    left join twilio_execution_steps
        on twilio_executions.execution_sid = twilio_execution_steps.execution_sid
            and twilio_execution_steps.transitioned_to in ('payment_success', 'payment_failure', 'call_ended', 'Ended')
    where twilio_executions.flow_id = 'FWb8dc994e196fb45768d303a02db132e1'
    qualify row_number() over (partition by twilio_executions.execution_sid order by twilio_execution_steps.date_created desc) = 1
)

, loan_tape_final as (
    select
        product_accounts.product_account_uuid
        , product_accounts.consumer_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
        , loan_tape.activity_date
        , loan_tape.days_past_due
        , loan_tape.current_balance
        , loan_tape.credit_limit
        , loan_tape.minimum_payment_due
        , loan_tape.past_due_amount
    from loan_tape
    inner join product_accounts
        on loan_tape.credit_line_uuid = product_accounts.credit_line_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where to_date(loan_tape.activity_date) = last_day(dateadd('month', -1, current_date()))
)

, charge_off_accounts as (
    select
        daily_account_balance.product_account_uuid
        , daily_account_balance.consumer_uuid
        , daily_account_balance.eod_balance
        , daily_account_balance.eod_status
    from daily_account_balance
    inner join product_accounts
        on daily_account_balance.product_account_uuid = product_accounts.product_account_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where daily_account_balance.eod_status = 'CHARGE_OFF'
        and daily_account_balance.balance_date = last_day(dateadd('month', -1, current_date()))
)

, disputes_final as (
    select
        disputes_summary.*
        , product_accounts.product_account_uuid
        , product_accounts.product_uuid as pa_product_uuid
    from disputes_summary
    inner join product_accounts
        on disputes_summary.dispute_consumer_uuid = product_accounts.consumer_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where disputes_summary.dispute_status = 'PENDING'
        and date_trunc('month', disputes_summary.dispute_start) = dateadd(month, -1, date_trunc('month', current_date()))
)

, card_reissue_final as (
    select
        card_reissue.*
        , product_accounts.product_uuid
        , product_accounts.merchant_name
    from card_reissue
    inner join product_accounts
        on card_reissue.product_account_uuid = product_accounts.product_account_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
)

, twilio_final as (
    select
        twilio_collections.*
        , product_accounts.product_account_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
    from twilio_collections
    inner join product_accounts
        on twilio_collections.consumer_uuid = product_accounts.consumer_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where date_trunc('month', twilio_collections.date_created) = dateadd(month, -1, date_trunc('month', current_date()))
)

, final as (
    select
        'total_accounts' as statistic
        , count(distinct loan_tape_final.product_account_uuid) as num
    from loan_tape_final

    union all

    select
        'total_receivables' as statistic
        , sum(loan_tape_final.current_balance) as num
    from loan_tape_final

    union all

    select
        'accounts_30_days_past_due' as statistic
        , count(distinct case when loan_tape_final.days_past_due between 1 and 59 then loan_tape_final.product_account_uuid end) as num
    from loan_tape_final

    union all

    select
        'dollars_30_days_past_due' as statistic
        , sum(case when loan_tape_final.days_past_due between 1 and 59 then loan_tape_final.current_balance else 0 end) as num
    from loan_tape_final

    union all

    select
        'accounts_60_days_past_due' as statistic
        , count(distinct case when loan_tape_final.days_past_due between 60 and 89 then loan_tape_final.product_account_uuid end) as num
    from loan_tape_final

    union all

    select
        'dollars_60_days_past_due' as statistic
        , sum(case when loan_tape_final.days_past_due between 60 and 89 then loan_tape_final.current_balance else 0 end) as num
    from loan_tape_final

    union all

    select
        'accounts_90_days_past_due' as statistic
        , count(distinct case when loan_tape_final.days_past_due between 90 and 119 then loan_tape_final.product_account_uuid end) as num
    from loan_tape_final

    union all

    select
        'dollars_90_days_past_due' as statistic
        , sum(case when loan_tape_final.days_past_due between 90 and 119 then loan_tape_final.current_balance else 0 end) as num
    from loan_tape_final

    union all

    select
        'accounts_120_days_past_due' as statistic
        , count(distinct case when loan_tape_final.days_past_due between 120 and 149 then loan_tape_final.product_account_uuid end) as num
    from loan_tape_final

    union all

    select
        'dollars_120_days_past_due' as statistic
        , sum(case when loan_tape_final.days_past_due between 120 and 149 then loan_tape_final.current_balance else 0 end) as num
    from loan_tape_final

    union all

    select
        'accounts_150_plus_days_past_due' as statistic
        , count(distinct case when loan_tape_final.days_past_due >= 150 then loan_tape_final.product_account_uuid end) as num
    from loan_tape_final

    union all

    select
        'dollars_150_plus_days_past_due' as statistic
        , sum(case when loan_tape_final.days_past_due >= 150 then loan_tape_final.current_balance else 0 end) as num
    from loan_tape_final

    union all

    select
        'charge_off_accounts' as statistic
        , count(distinct charge_off_accounts.product_account_uuid) as num
    from charge_off_accounts

    union all

    select
        'charge_off_dollars' as statistic
        , sum(charge_off_accounts.eod_balance) as num
    from charge_off_accounts

    union all

    select
        'disputes_count' as statistic
        , count(distinct disputes_final.dispute_transaction_uuid) as num
    from disputes_final

    union all

    select
        'disputes_dollars' as statistic
        , sum(disputes_final.amount) as num
    from disputes_final

    union all

    select
        'card_reissue_count' as statistic
        , count(distinct card_reissue_final.card_uuid) as num
    from card_reissue_final

    union all

    select
        'collections_calls_attempted' as statistic
        , count(distinct twilio_final.execution_sid) as num
    from twilio_final

    union all

    select
        'collections_calls_payment_success' as statistic
        , count(distinct case when twilio_final.execution_result = 'payment_success' then twilio_final.execution_sid end) as num
    from twilio_final
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_dq'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_dq' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
