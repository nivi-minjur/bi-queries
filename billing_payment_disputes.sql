-- A monthly report on all billing and payment disputes received in the prior month, to include date of dispute, account identifier and volume of disputes.
WITH uuid_lookup as (SELECT 
        call_id,
        user_phone_fix,
        consumer_uuid,
        ticket_id
    FROM
        (
            SELECT
                case
                    when left(REGEXP_REPLACE(call_from_number, '[^0-9]', ''), 1) = '1'
                    and length(REGEXP_REPLACE(call_from_number, '[^0-9]', '')) = 11 then right(
                        REGEXP_REPLACE(call_from_number, '[^0-9]', ''),
                        10
                    )
                    else REGEXP_REPLACE(call_from_number, '[^0-9]', '')
                end as user_phone_fix,
                call_id,
                ticket_id
            FROM
                EDW_PRD.MARTS.MRT_ZENDESK__FCT_TICKET_CALLS_SUMMARY calls
                left join (
                    SELECT
                        distinct ticket_id as ticket_id_2,
                        call_from_number
                    FROM
                        EDW_PRD.MARTS.MRT_ZENDESK__FCT_TICKET_COMMENTS
                    where
                        call_from_number is not null
                ) ticket_comments on calls.ticket_id = ticket_comments.ticket_id_2
        ) a
        left join (
            SELECT
                distinct consumer_uuid,
                phone
            FROM
                edw_prd.marts.mrt_public__dim_profiles
        ) c on a.user_phone_fix = c.phone
),
disputes as (
    select
        pa.product_account_uuid,
        calls.custom_imprint_uuid,
        coalesce(ticket_created_at_est, call_created_at_est) as dispute_timestamp,
        calls.custom_reason_code,
        calls.ticket_id,
        case
            when calls.brand_id in (
                '40954903794580',
                '36869591739412',
                '42879080717716'
            ) then 'CBH'
            else 'not CBH'
        end as program_name,
        pa.merchant_name
    from
        EDW_PRD.MARTS.MRT_ZENDESK__FCT_TICKET_CALLS_SUMMARY calls
        left join uuid_lookup uuid on calls.call_id = uuid.call_id
        left join EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS pa on calls.custom_imprint_uuid = pa.consumer_uuid
        and program_name = pa.merchant_combname
        left join (
            select
                distinct ticket_id
            from
                EDW_PRD.STG_ZENDESK.STG_ZENDESK__TICKET_TAG_HISTORY
            where
                tag_name = 'closed_by_merge'
        ) ta on ta.ticket_id = calls.ticket_id
    where
        brand_id in (
            '40954903794580',
            '36869591739412',
            '42879080717716'
        )
        and ticket_group = 'Disputes'
        and ta.ticket_id is null
        and custom_reason_code <> 'general__testing'
        and date_trunc('day', dispute_timestamp) >= '2025-10-10'
        and month(date_trunc('month', dispute_timestamp)) = month(date_trunc('month', current_date())) - 1
    order by
        dispute_timestamp
)
select
    product_account_uuid,
    custom_imprint_uuid,
    dispute_timestamp,
    custom_reason_code,
    merchant_name
from
    disputes;
