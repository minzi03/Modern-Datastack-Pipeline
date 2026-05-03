{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

with source_data as (

    select
        transaction_id,
        account_id,
        amount,
        transaction_type,
        related_account_id,
        status,
        transaction_time,
        cdc_op,
        cdc_ts,
        is_deleted,
        ingested_at
    from {{ ref('stg_transactions') }}
    where is_deleted = false

),

joined as (

    select
        t.transaction_id,
        t.account_id,
        a.customer_id,
        t.amount,
        t.related_account_id,
        t.status,
        t.transaction_type,
        t.transaction_time,
        t.cdc_op,
        t.cdc_ts,
        t.ingested_at
    from source_data t
    left join {{ ref('dim_accounts') }} a
        on t.account_id = a.account_id
       and a.is_current = true

)

select *
from joined

{% if is_incremental() %}
where cdc_ts > (
    select coalesce(max(cdc_ts), 0)
    from {{ this }}
)
{% endif %}