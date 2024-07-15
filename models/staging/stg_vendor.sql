select 
v.businessentityid as vendor_ID,
v.name as name,
v.accountnumber as account_number,
v.creditrating as credit_rating,
v.preferredvendorstatus as preferred_vendor_status,
v.activeflag as active_flag,
v.purchasingwebserviceurl as purchasing_web_service_url,
pv.standardprice as standard_price
from {{ source("purchasing", "vendor") }} v
        left join {{ source("purchasing", "productvendor") }} pv on v.businessentityid = pv.businessentityid
        left join {{ source("person", "businessentity") }} be on be.businessentityid = v.businessentityid