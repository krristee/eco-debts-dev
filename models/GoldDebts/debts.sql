
select * from {{ ref('NAV_debts') }}

UNION ALL

select * from {{ ref('FO_debts') }}

UNION ALL

select * from {{ ref('Nav_debts_closing_ECO_EPR') }}

UNION ALL

select * from {{ ref('FO_debts_closing_SP') }}