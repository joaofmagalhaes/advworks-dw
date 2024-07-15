select 
    po.purchaseorderid as order_id,
    po.productid as product_id,
    po.orderqty:: numeric(6,2),
    po.receivedqty as qt_received,
    po.unitprice as unit_price,
    DATE(po.duedate) as due_date,
    poh.revisionnumber as revision_number,
	   CASE WHEN poh.status = 1 THEN 'Pending'
	        WHEN poh.status = 2 THEN 'Approved'
			WHEN poh.status = 3 THEN 'Rejected'
			ELSE 'Complete'
		END as delivery_status,
    poh.employeeid as employee_id,
	poh.vendorid as vendor_id,
	poh.shipmethodid as delivery_method_id,
	poh.orderdate as order_date,
	poh.shipdate as delivery_date,
	poh.subtotal as sub_total,
	poh.taxamt as tax_amount,
	poh.freight as freight,
	poh.modifieddate as modified_date
from {{ source("purchasing", "purchaseorderdetail") }} po
    left join {{ source("purchasing", "purchaseorderheader")}} poh on po.purchaseorderid = poh.purchaseorderid
order by order_id
