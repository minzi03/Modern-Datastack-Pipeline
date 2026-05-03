{{ config(materialized='table') }}

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
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
from {{ ref('accounts_snapshot') }}