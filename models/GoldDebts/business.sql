
select * from {{ ref('NAV_business') }}

UNION ALL

select * from {{ ref('FO_business') }}