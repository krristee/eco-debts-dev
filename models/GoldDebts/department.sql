
select * from {{ ref('NAV_department') }}

UNION ALL

select * from {{ ref('FO_department') }}