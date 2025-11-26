--An account level report that includes the  type of opt out requested, account number, date opt out requested, date opt out completed and how the request was received. Report to also include total volume of opt outs in month being reported.

SELECT DISTINCT
    COALESCE(A.EXTERNAL_USER_ID, B.ACCNT_ID2) AS consumer_uuid, channel, time_at_est
FROM EDW_PRD.STG_BRAZE.STG_BRAZE__SUBSCRIPTION_EVENT AS A
FULL OUTER JOIN (
                    SELECT DISTINCT
                        EXTERNAL_USER_ID AS ACCNT_ID2
                    FROM EDW_PRD.STG_BRAZE.STG_BRAZE__EMAIL_EVENT
                    WHERE UPPER(EVENT_TYPE) = 'USERS.MESSAGES.EMAIL.UNSUBSCRIBE') AS B
    ON A.EXTERNAL_USER_ID = B.ACCNT_ID2
WHERE UPPER(SUBSCRIPTION_STATUS) = 'UNSUBSCRIBED'
and month(date_trunc('month', time_at_est)) = month(date_trunc('month', current_date())) - 1
and external_user_id in (select consumer_uuid from EDW_PRD.marts.mrt_public__dim_product_accounts where merchant_combname = 'CBH');