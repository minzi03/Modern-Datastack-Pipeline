{{ config(materialized='view') }}

select *
from {{ ref('dim_customers') }}
where is_current = true