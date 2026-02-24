WITH company_tmp as (
    SELECT
        nb."Com"                                        AS "id",
        IFNULL(cc."Company code",'')                    AS "code",
        cc."Company name"			                    AS "name",
        null 	                                        AS "country_code"
FROM {{ source("FOD", "COMPANYINFO") }} ci
LEFT JOIN {{ref ('FO_map_company_code') }} cc on upper(ci."dataarea") = cc."Company code"
left join {{ref ('FO_map_NAV_BC') }} nb on nb."DATAREAD" = upper(ci."dataarea")
LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as p_company    on EQUAL_NULL(p_company."Company code",cc."Company code") and p_company."Data Source" = 'FO'
WHERE p_company."Company_load" = 'TRUE' 
)

select ifnull("id", ' ')    AS "company.id",
ifnull("code", ' ')         AS "company.code",
ifnull("name", ' ')         AS "company.name",
ifnull("country_code", ' ') AS "company.country_code",
'FO'                        AS "company.system"
from company_tmp
