SELECT 
    shipmethodid as delivery_method_id,
    name as delivery_name,
	shipbase as delivery_base,
	shiprate as delivery_rate,
	modifieddate as modified_date
from {{ source("purchasing", "shipmethod") }} 