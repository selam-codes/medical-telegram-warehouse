-- Business rule: no message should be dated in the future.
-- This query must return 0 rows for the test to pass.

select *
from {{ ref('stg_telegram_messages') }}
where message_date > current_timestamp
