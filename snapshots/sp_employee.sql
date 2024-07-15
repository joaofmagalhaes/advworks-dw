{% snapshot sp_employee %}

{{
    config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='check',
        check_cols=['job_title', 'birth_date', 'marital_status', 'gender', 'salaried_flag', 'vacation_hours', 'sick_leave_hours', 'employee_last_update']
    )
}}

select
    *
from {{ ref('stg_employee') }}

{% endsnapshot %}