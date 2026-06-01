with products as (
    select *
    from edw_prd.marts.mrt_public__dim_products
    where bank_partner = 'FIRST BANK & TRUST'
)

, applications as (
    select *
    from edw_prd.marts.mrt_public__dim_applications
)

, ofac_backfill as (
    select
        application_uuid
        , ofac as ofac_score
    from edw_prd.stg_public.stg_public__backfilled_service_events
    qualify row_number() over (partition by application_uuid order by updated_at desc, created_at desc) = 1
)

, fraud_score as (
    select
        application_uuid
        , max(emailage_risk_band) as emailage_risk_score
        , max(ofac_score) as ofac_score
        , max(pep_score) as pep_score
        , max(sentilink_abuse_score) as sentilink_abuse_score
        , max(sentilink_id_theft_score) as sentilink_id_theft_abuse_score
    from edw_prd.intermediate.int_public__service_event_create_journey_application_event_flatten
    group by 1
)

, final as (
    select
        applications.application_uuid
        , applications.product_uuid
        , applications.merchant_name
        , applications.applicant_first_name
        , applications.applicant_last_name
        , applications.applicant_address_line1
        , applications.applicant_address_line2
        , applications.applicant_address_city
        , applications.applicant_address_state
        , applications.applicant_address_zip
        , applications.applicant_email
        , applications.applicant_phone
        , applications.application_submitted_at_est
        , applications.application_status
        , applications.card_apr
        , applications.applicant_housing_type
        , applications.applicant_housing_cost
        , iff(applications.application_pending_at_est is not null, 'Y', 'N') as application_previously_pended_flag
        , applications.card_limit as initial_credit_limit
        , applications.applicant_income as income
        , date_from_parts(applications.applicant_birth_year, applications.applicant_birth_month, applications.applicant_birth_day) as dob
        , applications.applicant_age
        , applications.vantage_score
        , applications.fico_score
        , applications.early_pay_default_score
        , applications.new_account_score
        , fraud_score.emailage_risk_score
        , coalesce(fraud_score.ofac_score, ofac_backfill.ofac_score) as ofac_score
        , fraud_score.pep_score
        , fraud_score.sentilink_abuse_score
        , fraud_score.sentilink_id_theft_abuse_score
    from applications
    inner join products
        on applications.product_uuid = products.product_uuid
    left join fraud_score
        on applications.application_uuid = fraud_score.application_uuid
    left join ofac_backfill
        on applications.application_uuid = ofac_backfill.application_uuid
    where date_trunc('month', applications.application_submitted_at_est) = dateadd(month, -1, date_trunc('month', current_date()))
        and applications.is_test_account = false
    order by 15, 16
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_application_cip'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_application_cip' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
