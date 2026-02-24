/*company:
      LOAD
      	  [Company_QLIK]														AS id,
          APPLYMAP('map_company_code',[Name],'$(vL.unknown_value)') 			AS code,
          [Name]																AS name,
          [Country Code] 														AS country_code // map country
      FROM [$(vG.ExtractNAVQVDPath)2.MD/1.ALL/Company Information.qvd] (qvd);*/

WITH company_tmp as (
    SELECT
      	"Com"	            AS "id",
        cc."Company code"   AS "code",
        "Name"			    AS "name",
        "Country Code" 	    AS "country_code"
FROM {{ source("NAV", "Company Information") }} ci
LEFT JOIN {{ref ('map_company_code') }} cc on ci."Name" = cc."Company name"
--where "id" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta')
LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}  as d_company    on EQUAL_NULL(d_company."Company code",cc."Company code") 
WHERE d_company."Company_load" = 'TRUE' and d_company."Data Source" = 'NAV' 

)

select ifnull("id", ' ')    AS "company.id",
ifnull("code", ' ')         AS "company.code",
ifnull("name", ' ')         AS "company.name",
ifnull("country_code", ' ') AS "company.country_code",
'NAV'                       AS "company.system"
from company_tmp