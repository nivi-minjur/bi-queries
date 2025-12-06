select product_account_uuid, mla_flag
FROM EDW_PRD.stg_public.stg_public__service_event_hardpull a
left join EDW_PRD.MARTS.MRT_PUBLIC__DIM_PRODUCT_ACCOUNTS b 
on a.application_uuid =b.application_uuid
where merchant_combname = 'CBH'
and mla_flag = TRUE
and is_test_account = FALSE
and month(date_trunc('month', a.updated_at_est)) = month(date_trunc('month', current_date())) - 1
qualify row_number() over (partition by a.application_uuid ORDER BY a.updated_at_est asc) = 1
