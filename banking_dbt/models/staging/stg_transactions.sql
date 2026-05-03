{{ config(materialized='view') }}

with source_data as (
    select
        id::string as transaction_id,
        account_id::string as account_id,
        txn_type::string as transaction_type,
        amount::float as amount,
        related_account_id::string as related_account_id,
        status::string as status,
        created_at::timestamp as transaction_time,
        _cdc_op::string as cdc_op,
        _cdc_ts::number as cdc_ts,
        _is_deleted::boolean as is_deleted,
        _ingested_at::timestamp as ingested_at
    from {{ source('raw', 'transactions') }}
    where id is not null
),

ranked as (
    select
        transaction_id,
        account_id,
        transaction_type,
        amount,
        related_account_id,
        status,
        transaction_time,
        cdc_op,
        cdc_ts,
        is_deleted,
        ingested_at,
        row_number() over (
            partition by transaction_id
            order by cdc_ts desc, ingested_at desc
        ) as rn
    from source_data
)

select
    transaction_id,
    account_id,
    transaction_type,
    amount,
    related_account_id,
    status,
    transaction_time,
    cdc_op,
    cdc_ts,
    is_deleted,
    ingested_at
from ranked
where rn = 1
  and is_deleted = false