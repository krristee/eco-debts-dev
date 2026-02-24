
select * from {{ ref('NAV_customer') }}

UNION ALL

select * from {{ ref('FO_customer') }}