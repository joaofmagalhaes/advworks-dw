select
    e.businessentityid as employee_id,
    e.jobtitle as job_title,
    e.birthdate as birth_date,
    e.maritalstatus as marital_status,
    e.gender,
    e.salariedflag as salaried_flag,
    e.vacationhours as vacation_hours,
    e.sickleavehours as sick_leave_hours,
    e.modifieddate as employee_last_update,
    p.firstname as first_name,
    p.middlename as middle_name,
    p.lastname as last_name,
    GREATEST(e.modifieddate, p.modifieddate) as last_update
from {{ source("humanresources", "employee") }} e
    left join {{ source("person", "person") }} p on e.businessentityid = p.businessentityid
