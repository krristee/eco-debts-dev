select 
    upper(ci."dataarea") as "Com", 
    cc."Company name" as "Name" 
FROM {{ source("FOD", "COMPANYINFO") }} ci
LEFT JOIN {{ref ('FO_map_company_code') }} cc on upper(ci."dataarea") = cc."Company code"
left join {{ref ('FO_map_NAV_BC') }} nb on nb."DATAREAD" = upper(ci."dataarea")
LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as p_company    on EQUAL_NULL(p_company."Company code",cc."Company code") and p_company."Data Source" = 'FO'
WHERE p_company."Company_load" = 'TRUE'