SELECT
    	nb."Com",
    	1                               AS "source_type",
    	c."accountnum"                  AS "No_",
        trim(c."organizationcode_ecs")  AS "Registration No_",
        coalesce(map."Name",dp."name")  AS "Name",
        c."custgroup"                   AS "cust_vend_registration_group"
    FROM {{source("FOD", "CUSTTABLE")}} c
    left join {{source("FOD", "DIRPARTYTABLE")}} dp on c."party" = dp."recid" 
    left join {{ref('FO_map_NAV_BC')}} nb on nb.DATAREAD = upper(c."dataareaid")
    LEFT JOIN {{ref('FO_map_name')}} map on trim(c."organizationcode_ecs") = map."Registration_No"
    LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as p_company    on EQUAL_NULL(p_company."Company code",upper(c."dataareaid")) and p_company."Data Source" = 'FO'
    WHERE p_company."Company_load" = 'TRUE'
    
    UNION ALL

    SELECT
    	nb."Com",
    	2                               AS source_type,
    	v."accountnum"                  AS "No_",
        trim(v."organizationcode_ecs")  AS "Registration No_",
        coalesce(map."Name",dp."name")  AS "Name",
        v."vendgroup"                   AS "cust_vend_registration_group"  
    FROM {{source("FOD", "VENDTABLE")}} v
    left join {{source("FOD", "DIRPARTYTABLE")}} dp on v."party" = dp."recid" 
    left join {{ref ('FO_map_NAV_BC') }} nb on nb.DATAREAD = upper(v."dataareaid")
    LEFT JOIN {{ref('FO_map_name')}} map on trim(v."organizationcode_ecs") = map."Registration_No"
    LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as p_company    on EQUAL_NULL(p_company."Company code",upper(v."dataareaid")) and p_company."Data Source" = 'FO'
    WHERE p_company."Company_load" = 'TRUE'

    UNION ALL

    SELECT
        nb."Com",
    	3               AS source_type,
        a."accountid"   as "No_",
        null            AS "Registration No_",
        a."name"        as "Name",
        null AS "cust_vend_registration_group"
    FROM {{source("FOD", "BANKACCOUNTTABLE")}} a
    left join {{ref ('FO_map_NAV_BC') }} nb on nb.DATAREAD = upper(a."dataareaid")
    LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as p_company    on EQUAL_NULL(p_company."Company code",upper(a."dataareaid")) and p_company."Data Source" = 'FO'
    WHERE p_company."Company_load" = 'TRUE'

    UNION ALL

    SELECT
        nb."Com",
    	4           AS source_type,
        a."assetid" as "No_",
        null        AS "Registration No_",
        a."name"    AS "Name",
        null AS "cust_vend_registration_group"
    FROM {{source("FOD", "ASSETTABLE")}} a
    left join {{ref ('FO_map_NAV_BC') }} nb on nb.DATAREAD = upper(a."dataareaid")
    LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as p_company    on EQUAL_NULL(p_company."Company code",upper(a."dataareaid")) and p_company."Data Source" = 'FO'
    WHERE p_company."Company_load" = 'TRUE'