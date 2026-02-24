
select * from {{ ref('NAV_accruals') }}

--UNION ALL

--select * from {{ ref('FO_accruals') }} -- dubliuojasi, nes duomenys iš excel