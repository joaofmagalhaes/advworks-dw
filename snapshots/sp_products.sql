{% snapshot sp_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['category', 'subcategory', 'color', 'size']
    )
}}

select
    *
from {{ ref('stg_products') }}

{% endsnapshot %}