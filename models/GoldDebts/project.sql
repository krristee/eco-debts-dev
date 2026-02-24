
select * from {{ ref('NAV_project') }}

UNION ALL

select * from {{ ref('FO_project') }}