SELECT 
    c."Id"                                                                             AS "Id",
    'CustomerLedger'                                                                   AS "type",
    max(dim."xa_padalinysvalue")                                                       AS "department_code",
    max(dim."xb_projektasvalue")                                                       AS "project_code",
   
from {{ source("FOD", "CUSTTRANS") }}                                    c
left join {{ source("FOD", "CUSTINVOICEJOUR") }}                         j on c."voucher" = j."ledgervoucher" and c."transdate" = j."invoicedate" and c."accountnum" = j."invoiceaccount"
left join {{ source("FOD", "GENERALJOURNALENTRY") }}                     dh on c."voucher" = dh."subledgervoucher" and c."transdate" = dh."accountingdate" and upper(j."dataareaid") = upper(dh."subledgervoucherdataareaid")
left join {{ source("FOD", "GENERALJOURNALACCOUNTENTRY") }}              dk on dh."recid" = dk."generaljournalentry"
left join {{ source("FOD", "DIMENSIONATTRIBUTEVALUECOMBINATION") }}      dim on dim."recid" = dk."ledgerdimension"
left join {{ source("FOD", "MAINACCOUNT") }}                             acc on dk."mainaccount" = acc."recid"
LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as p_company    on EQUAL_NULL(p_company."Company code",upper(c."dataareaid")) and p_company."Data Source" = 'FO'
where LEFT(acc."mainaccountid", 1) in (5) and p_company."Company_load" = 'TRUE'
group by c."Id"

UNION ALL 

SELECT 
    c."Id"                                                                             AS "Id",
    'VendorLedger'                                                                     AS "type",
    max(dim."xa_padalinysvalue")                                                       AS "department_code",
    max(dim."xb_projektasvalue")                                                       AS "project_code",
   
from {{ source("FOD", "VENDTRANS") }}                                    c
left join {{ source("FOD", "VENDINVOICEJOUR") }}                         j on c."voucher" = j."ledgervoucher" and c."transdate" = j."invoicedate" and c."accountnum" = j."invoiceaccount"
left join {{ source("FOD", "GENERALJOURNALENTRY") }}                     dh on c."voucher" = dh."subledgervoucher" and c."transdate" = dh."accountingdate" and upper(j."dataareaid") = upper(dh."subledgervoucherdataareaid")
left join {{ source("FOD", "GENERALJOURNALACCOUNTENTRY") }}              dk on dh."recid" = dk."generaljournalentry"
left join {{ source("FOD", "DIMENSIONATTRIBUTEVALUECOMBINATION") }}      dim on dim."recid" = dk."ledgerdimension"
left join {{ source("FOD", "MAINACCOUNT") }}                             acc on dk."mainaccount" = acc."recid"
LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as p_company    on EQUAL_NULL(p_company."Company code",upper(c."dataareaid")) and p_company."Data Source" = 'FO'
where LEFT(acc."mainaccountid", 1) in (6) and p_company."Company_load" = 'TRUE'
group by c."Id"