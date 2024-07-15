{% snapshot sp_vendor %}

{{
    config(
        target_schema='snapshots',
        unique_key='vendor_id',
        strategy='check',
        check_cols=['standard_price']
    )
}}

select
    *
from {{ ref('stg_vendor') }}

{% endsnapshot %}