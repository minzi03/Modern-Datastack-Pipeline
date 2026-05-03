{{ config(materialized='view') }}

with source_data as (
    select
        id::string as account_id,
        customer_id::string as customer_id,
        account_type::string as account_type,
        balance::float as balance,
        currency::string as currency,
        created_at::timestamp as created_at,
        _cdc_op::string as cdc_op,
        _cdc_ts::number as cdc_ts,
        _is_deleted::boolean as is_deleted,
        _ingested_at::timestamp as ingested_at
    from {{ source('raw', 'accounts') }}
    where id is not null
),

ranked as (
    select
        account_id,
        customer_id,
        account_type,
        balance,
        currency,
        created_at,
        cdc_op,
        cdc_ts,
        is_deleted,
        ingested_at,
        row_number() over (
            partition by account_id
            order by cdc_ts desc, ingested_at desc
        ) as rn
    from source_data
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    cdc_op,
    cdc_ts,
    is_deleted,
    ingested_at
from ranked
where rn = 1
  and is_deleted = false