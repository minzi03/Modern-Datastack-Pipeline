{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    cdc_op,
    cdc_ts,
    ingested_at,
    dbt_valid_from as effective_from,
    dbt_valid_to as effective_to,
    case
        when dbt_valid_to is null then true
        else false
    end as is_current
from {{ ref('customers_snapshot') }}