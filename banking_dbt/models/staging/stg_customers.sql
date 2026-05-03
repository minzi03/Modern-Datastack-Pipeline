{{ config(materialized='view') }}

with source_data as (
    select
        id::string as customer_id,
        first_name::string as first_name,
        last_name::string as last_name,
        email::string as email,
        created_at::timestamp as created_at,
        _cdc_op::string as cdc_op,
        _cdc_ts::number as cdc_ts,
        _is_deleted::boolean as is_deleted,
        _ingested_at::timestamp as ingested_at
    from {{ source('raw', 'customers') }}
    where id is not null
),

ranked as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        created_at,
        cdc_op,
        cdc_ts,
        is_deleted,
        ingested_at,
        row_number() over (
            partition by customer_id
            order by cdc_ts desc, ingested_at desc
        ) as rn
    from source_data
)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    cdc_op,
    cdc_ts,
    is_deleted,
    ingested_at
from ranked
where rn = 1
  and is_deleted = false