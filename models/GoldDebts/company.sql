
select * from {{ ref('NAV_company') }}

UNION ALL

select * from {{ ref('FO_company') }}