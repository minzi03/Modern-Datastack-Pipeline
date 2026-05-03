{% snapshot customers_snapshot %}
{{
    config(
      target_schema='ANALYTICS',
      unique_key='customer_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'email'],
      invalidate_hard_deletes=True
    )
}}

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
from {{ ref('stg_customers') }}

{% endsnapshot %}