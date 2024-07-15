select 
p.productid as product_ID,
p.name as product_name,
p.productnumber as product_number,
coalesce (nullif(pcat.name,''), 'out of stock') as category,
coalesce (nullif(psubc.name,''), 'out of stock') as subcategory,
coalesce (nullif(p.color,''), 'out of stock') as color,
coalesce (nullif(p.size,''), 'out of stock') as size
    from {{ source("production", "product") }} p
        left join {{ source("production", "productsubcategory") }} psubc on p.productsubcategoryid = psubc.productsubcategoryid
	    left join {{ source("production", "productcategory") }} pcat on psubc.productcategoryid = pcat.productcategoryid