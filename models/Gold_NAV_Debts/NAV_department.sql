/* department:
      LOAD
          Code																	AS code,
          Name																	AS name_original,
          APPLYMAP('map_department_name',Code,'$(vL.unknown_value)') 			AS name,
          Code &' '&  APPLYMAP('map_department_name',Code,'$(vL.unknown_value)') 
          																		AS code_name,
          [Company_QLIK] 														AS qlik_company_id          
      FROM [$(vG.ExtractNAVQVDPath)\2.MD\1.ALL\Dimension Value.qvd](qvd)
      WHERE "Dimension Code"='PADALINYS';*/

with department as (
SELECT
    d."Code"	                                            AS "code",
    d."Name"	                                            AS "name_original",
    dn."Department name"                                    AS "name",
    concat(d."Code", ' ', ifnull(dn."Department name", '')) AS "code_name",
    d."Com" 				                                AS "Com"          
FROM {{ source("NAV", "Dimension Value") }} d
LEFT JOIN {{ref ('map_department_name') }} dn on d."Code" = dn."Department code"
LEFT JOIN {{ ref('NAV_company') }}  company ON EQUAL_NULL(company."company.id",d."Com")
WHERE d."Dimension Code"='PADALINYS'
and company."company.id" = d."Com"   --and d."Com" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta')
)

SELECT 
ifnull("code", ' ')          as "department.code",
ifnull("name_original", ' ') as "department.name_original",
ifnull("name", ' ')          as "department.name",
ifnull("code_name", ' ')     as "department.code_name",
ifnull("Com", ' ')           as "department.Com",
'NAV'                        AS "department.system"
FROM department