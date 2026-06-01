with products as (
    select *
    from edw_prd.marts.mrt_public__dim_products
    where bank_partner = 'FIRST BANK & TRUST'
)

, profiles as (

    select *
    from edw_prd.marts.mrt_public__dim_profiles
)

, product_accounts as (

    select *
    from edw_prd.marts.mrt_public__dim_product_accounts

    where is_booked
        and not is_test_account
)

, primary_profile_ssn as (

    select
        consumer_uuid
        , social_security_number
    from edw_prd.marts.mrt_cbh__dim_primary_profile_reconcile_merged
    where social_security_number is not null
    qualify row_number() over (
        partition by consumer_uuid
        order by social_security_number
    ) = 1
)

, dim_applications as (

    select *
    from edw_prd.marts.mrt_public__dim_applications

)

, events_ssn_details as (
    select
        consumer_uuid
        , application_uuid
        , coalesce(document_ssn, latest_document_ssn) as final_document_ssn
        , document_ssn_metadata_timestamp
    from edw_prd.intermediate.int_public__service_event_create_journey_application_pii_entity_flatten
    where (
        document_ssn is not null
        or latest_document_ssn is not null
    )
    qualify row_number()
        over (partition by application_uuid order by document_ssn_metadata_timestamp desc)
    = 1
)

, events_ssn as (

    select
        dim_applications.consumer_uuid
        , events_ssn_details.final_document_ssn
    from events_ssn_details
    inner join dim_applications
        on events_ssn_details.application_uuid = dim_applications.application_uuid
    qualify row_number()
        over (partition by dim_applications.consumer_uuid order by events_ssn_details.document_ssn_metadata_timestamp desc)
    = 1
)

, final as (

    select distinct
        profiles.consumer_uuid as customer_id
        , coalesce(
            profiles.ssn
            , primary_profile_ssn.social_security_number
            , events_ssn.final_document_ssn
        ) as tax_id
        , null as corporate_name
        , profiles.first_name
        , profiles.last_name
        , null as second_name
        , null as third_name
        , to_varchar(profiles.date_of_birth, 'DD/MM/YYYY') as date_of_birth
        , profiles.address_line1 as street_address_one
        , profiles.address_line2 as street_address_two
        , profiles.address_city as city
        , profiles.address_state as state
        , profiles.address_zipcode as zip_code
        , profiles.email as email_address
        , product_accounts.product_uuid
        , product_accounts.merchant_name

    from profiles
    inner join product_accounts
        on profiles.consumer_uuid = product_accounts.consumer_uuid
    inner join products
        on product_accounts.product_uuid = products.product_uuid
    left join primary_profile_ssn
        on profiles.consumer_uuid = primary_profile_ssn.consumer_uuid
    left join events_ssn
        on profiles.consumer_uuid = events_ssn.consumer_uuid
)

select * from final
