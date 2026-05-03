{% snapshot accounts_snapshot %}
{{
    config(
      target_schema='ANALYTICS',
      unique_key='account_id',
      strategy='check',
      check_cols=['customer_id', 'account_type', 'balance', 'currency'],
      invalidate_hard_deletes=True
    )
}}

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
from {{ ref('stg_accounts') }}

{% endsnapshot %}