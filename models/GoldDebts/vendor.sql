
select * from {{ ref('NAV_vendor') }}

UNION ALL

select * from {{ ref('FO_vendor') }}