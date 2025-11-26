--A report of all applications received within the prior month and the status of the applications (i.e. approved, declined, in review, incomplete and all other application statuses). Report should include volume of applications received each month.

select application_uuid, application_status, application_submitted_at_est from 
EDW_PRD.marts.mrt_public__dim_applications 
where merchant_combname = 'CBH' --inclusive of CBH and CB2
and date_trunc('day', application_submitted_at_est) >= '2025-10-10' --only for october, filter out preconversion apps
and month(date_trunc('month', application_submitted_at_est)) = month(date_trunc('month', current_date())) - 1
and is_test_account = False
order by 3,2;