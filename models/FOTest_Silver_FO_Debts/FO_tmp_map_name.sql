    SELECT
        'Customer'                                        AS "Ledger",
        trim(c."organizationcode_ecs")                    AS "Registration_No",
        IFNULL(dp."name",'')                              AS "Name",
        coalesce(cc."Company code", 'SA')                 AS "Company code",
        dp."modifiedon"                                   as "timestamp"
    FROM {{ source("FOD", "CUSTTABLE") }} c
    left join {{ source("FOD", "DIRPARTYTABLE") }} dp on c."party" = dp."recid" 
    left join {{ref('FO_map_NAV_BC')}}  nb on nb.DATAREAD = upper(c."dataareaid")
    --LEFT JOIN {{ref('FO_map_company_name')}} cn on nb."Com" = cn."Com"
    LEFT JOIN {{ref('FO_map_company_code')}} cc on EQUAL_NULL(upper(c."dataareaid"), cc."Company code") --cn."Name" = cc."Company name"
    LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as p_company    on EQUAL_NULL(p_company."Company code",upper(c."dataareaid")) and p_company."Data Source" = 'FO'
WHERE p_company."Company_load" = 'TRUE'
and LENGTH(trim(c."organizationcode_ecs")) > 4 AND LENGTH(TRIM(dp."name") ) > 0 --AND "Blocked" <> 3

union all

    SELECT
        'Vendor'                                          AS "Ledger",
        trim(v."organizationcode_ecs")                    AS "Registration No_",
        IFNULL(dp."name",'')                              AS "Name",
        coalesce(cc."Company code", 'SA')                 AS "Company code",
        dp."modifiedon"                                   AS "timestamp"
    FROM {{ source("FOD", "VENDTABLE") }} v
    left join {{ source("FOD", "DIRPARTYTABLE") }} dp on v."party" = dp."recid" 
    left join {{ref('FO_map_NAV_BC')}}  nb on nb.DATAREAD = upper(v."dataareaid")
    --LEFT JOIN {{ref('FO_map_company_name')}} cn on EQUAL_NULL(nb."Com", cn."Com")
    LEFT JOIN {{ref('FO_map_company_code')}} cc on EQUAL_NULL(upper(v."dataareaid"), cc."Company code") --EQUAL_NULL(cn."Name",cc."Company name")	 
    LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as p_company    on EQUAL_NULL(p_company."Company code",upper(v."dataareaid")) and p_company."Data Source" = 'FO'
WHERE p_company."Company_load" = 'TRUE'
and LENGTH(trim(v."organizationcode_ecs")) > 4 AND LENGTH(TRIM(dp."name") ) > 0 --AND "Blocked" <> 3

