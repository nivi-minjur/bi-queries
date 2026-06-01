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

, zendesk_tickets as (
    select *
    from edw_prd.stg_zendesk.stg_zendesk__tickets
)

, zendesk_brands as (
    select *
    from edw_prd.stg_zendesk.stg_zendesk__brands
)

, zendesk_ticket_field_history as (
    select *
    from edw_prd.stg_zendesk.stg_zendesk__ticket_field_history
)

, product_accounts as (
    select *
    from edw_prd.marts.mrt_public__dim_product_accounts
)

, ticket_solve_dates as (
    select
        ticket_id
        , cast(date_trunc('day', convert_timezone('America/New_York', updated_at)) as date) as solve_date_1
    from zendesk_ticket_field_history
    where field_name in ('status')
        and field_value in ('solved')
    qualify row_number() over (partition by ticket_id order by updated_at desc) = 1
)

, zendesk_complaints_temp as (
    select
        zendesk_tickets.*
        , case
            when zendesk_tickets.via_channel in ('native_messaging', 'chat') then 'chat'
            when zendesk_tickets.via_channel in ('web', 'answer_bot_for_web_widget') then 'email'
            else zendesk_tickets.via_channel
        end as channel
        , convert_timezone('America/New_York', zendesk_tickets.ticket_created_at) as created_at_est
        , cast(date_trunc('day', convert_timezone('America/New_York', zendesk_tickets.ticket_created_at)) as date) as created_date
        , substr(to_varchar(convert_timezone('America/New_York', zendesk_tickets.ticket_created_at)), 1, 7) as year_month
        , date_trunc('week', cast(date_trunc('day', convert_timezone('America/New_York', zendesk_tickets.ticket_created_at)) as date)) as weekstart
        , zendesk_brands.brand_name as brand
        , ticket_solve_dates.solve_date_1
    from zendesk_tickets
    left join zendesk_brands
        on zendesk_tickets.brand_id = zendesk_brands.brand_id
    left join ticket_solve_dates
        on zendesk_tickets.ticket_id = ticket_solve_dates.ticket_id
    where zendesk_tickets.custom_complaint = 'TRUE'
)

, merged_ticket_data as (
    select
        zendesk_tickets.*
        , substr(to_varchar(convert_timezone('America/New_York', zendesk_tickets.ticket_created_at)), 1, 7) as year_month
        , index as array_index
        , value as merged_tickets
    from zendesk_tickets
    , lateral flatten(input => zendesk_tickets.merged_ticket_ids)
    where zendesk_tickets.merged_ticket_ids not like ('[]')
        and (
            zendesk_tickets.custom_does_contact_have_account_with_imprint not in ('no_uuid_needed')
            or zendesk_tickets.custom_does_contact_have_account_with_imprint is null
        )
)

, merged_ticket_uuid as (
    select distinct
        case when merged_ticket_data.custom_imprint_uuid is null or merged_ticket_data.custom_imprint_uuid = '' then 1 else 0 end as is_null
        , merged_ticket_data.ticket_id
        , merged_ticket_data.merged_tickets
        , merged_ticket_data.custom_imprint_uuid
    from merged_ticket_data
    where case when merged_ticket_data.custom_imprint_uuid is null or merged_ticket_data.custom_imprint_uuid = '' then 1 else 0 end != 1
)

, merged_ticket_solve_dates as (
    select
        ticket_id
        , cast(date_trunc('day', convert_timezone('America/New_York', updated_at)) as date) as solve_date_1
    from zendesk_ticket_field_history
    where field_name in ('status')
        and field_value in ('solved')
    qualify row_number() over (partition by ticket_id order by updated_at desc) = 1
)

, zendesk_complaints_solve as (
    select
        zendesk_complaints_temp.*
        , merged_ticket_uuid.merged_tickets
        , merged_ticket_solve_dates.solve_date_1 as solve_date_2
    from zendesk_complaints_temp
    left join merged_ticket_uuid
        on zendesk_complaints_temp.ticket_id = merged_ticket_uuid.ticket_id
    left join merged_ticket_solve_dates
        on merged_ticket_uuid.merged_tickets = merged_ticket_solve_dates.ticket_id
    where zendesk_complaints_temp.solve_date_1 is null
        and merged_ticket_solve_dates.solve_date_1 is not null
    qualify row_number() over (partition by zendesk_complaints_temp.ticket_id order by merged_ticket_solve_dates.solve_date_1 desc) = 1
)

, final as (
    select
        zendesk_complaints_temp.custom_imprint_uuid
        , product_accounts.product_uuid
        , product_accounts.merchant_name
        , zendesk_complaints_temp.custom_issue_summary
        , zendesk_complaints_temp.custom_complaint_summary
        , zendesk_complaints_temp.custom_complaint_resolution
        , coalesce(zendesk_complaints_temp.solve_date_1, zendesk_complaints_solve.solve_date_2) as solve_date
    from zendesk_complaints_temp
    left join zendesk_complaints_solve
        on zendesk_complaints_temp.ticket_id = zendesk_complaints_solve.ticket_id
    left join product_accounts
        on zendesk_complaints_temp.custom_imprint_uuid = product_accounts.consumer_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    where zendesk_complaints_temp.brand_id in (select brand_id from fbt_brands)
        and date_trunc('month', coalesce(zendesk_complaints_temp.solve_date_1, zendesk_complaints_solve.solve_date_2)) = dateadd(month, -1, date_trunc('month', current_date()))
        and zendesk_complaints_temp.custom_issue_summary != 'Test'
        and zendesk_complaints_temp.custom_criticality = 'tier_1'
)

, stakeholder_review as (

    select coalesce(status = 'yes', false) as is_approved
    from edw_prd.stg_google_sheets.stg_google_sheets__fbt_monthly_stakeholder_review
    where report_table_name = 'fbt_universal_report_complaints_tier_1'
        and report_month = last_day(dateadd('month', -1, current_date()))

)

select
    final.*
    , 'fbt_universal_report_complaints_tier_1' as table_name
    , last_day(dateadd('month', -1, current_date())) as report_month
    , last_day(current_date()) as approval_month
    , coalesce(stakeholder_review.is_approved, false) as is_approved
from final
left join stakeholder_review
    on 1 = 1
