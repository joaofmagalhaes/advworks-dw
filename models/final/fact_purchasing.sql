with dimension_ids as (
    select distinct
        pur.order_id,
        pur.orderqty,
        pur.qt_received,
        pur.unit_price,
        pur.due_date,        
        pur.revision_number,
        pur.delivery_status,
        pur.order_date,
        pur.delivery_date,
        pur.sub_total,
        pur.tax_amount,
        pur.freight,
        pur.modified_date,
        v.vendor_id,
        e.employee_id,
        p.product_id,
        s.delivery_method_id
    from {{ ref('stg_purchasing') }} pur
        left join {{ ref('stg_vendor') }} v on pur.vendor_id = v.vendor_id
        left join {{ ref('stg_employee') }} e on pur.employee_id = e.employee_id
        left join {{ ref('stg_shipment') }} s on pur.delivery_method_id = s.delivery_method_id
        left join {{ ref('stg_products') }} p on pur.product_id = p.product_id
    order by order_id    
),


surrogate_keys as (
    select
        order_id,
        dv.sk_vendor as sk_vendor,
        de.sk_employee as sk_employee,
        dp.sk_product as sk_product,
        ds.sk_shipment as sk_shipment,
        o_dd.sk_date as sk_order_date,
        s_dd.sk_date as sk_shipment_date,
        delivery_status,
        due_date,
        orderqty,
        qt_received,
        unit_price,
        sub_total,
        tax_amount,
        freight
    from dimension_ids dim
        join {{ ref('dim_date') }} o_dd on dim.order_date = o_dd.date
        left join {{ ref('dim_date') }} s_dd on dim.delivery_date = s_dd.date
        join {{ ref('dim_vendor') }} dv on dim.vendor_id = dv.vendor_id
            and dim.order_date between dv.valid_from and dv.valid_to
        left join {{ ref('dim_products') }} dp on dim.product_id = dp.product_id
            and dim.order_date between dp.valid_from and dp.valid_to
        left join {{ ref('dim_employee') }} de on dim.employee_id = de.employee_id
            and dim.order_date between de.valid_from and de.valid_to
        left join {{ ref('dim_shipment') }} ds on dim.delivery_method_id = ds.delivery_method_id
            and dim.order_date between ds.valid_from and ds.valid_to
),



final as (
    select
        order_id,
        sk_vendor,
        sk_employee,
        sk_product,
        sk_shipment,
        sk_order_date,
        sk_shipment_date,
        delivery_status,
        due_date,
        orderqty,
        qt_received,
        unit_price,
        sub_total,
        tax_amount,
        freight
    from surrogate_keys
)

select
    *
from final