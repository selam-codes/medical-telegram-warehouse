-- Business rule: view and forward counts can never be negative.
-- This query must return 0 rows for the test to pass.

select *
from {{ ref('fct_messages') }}
where views < 0 or forwards < 0
