/*project:
      LOAD
          Code															AS code,
          Name															AS name_original,
          APPLYMAP('map_project_name',Code,'$(vL.unknown_value)') 		AS name,
          Code & ' ' & APPLYMAP('map_project_name',Code,'$(vL.unknown_value)') 
          																AS code_name,
          [Company_QLIK] 												AS qlik_company_id
      FROM [$(vG.ExtractNAVQVDPath)\2.MD\1.ALL\Dimension Value.qvd](qvd)
      WHERE "Dimension Code"='PROJEKTAS';*/

with project as (
SELECT
    d."Code"	                                        AS "code",
    d."Name"	                                        AS "name_original",
    p."Project name"                                    AS "name",
    concat(d."Code", ' ', IFNULL(p."Project name",''))  AS "code_name",
    d."Com" 				                            AS "Com" 
FROM {{ source("NAV", "Dimension Value") }} d
LEFT JOIN {{ref ('map_project_name') }} p on d."Code" = p."Project code"
WHERE "Dimension Code"='PROJEKTAS'
and d."Com" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta')
--LEFT JOIN {{ ref('NAV_company') }}  company ON EQUAL_NULL(company."company.id",d."Com")
--WHERE d."Dimension Code"='PADALINYS'
--and company."company.id" = d."Com"   -- d."Com" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta')
)

SELECT 
ifnull("code", ' ')          as "project.code",
ifnull("name_original", ' ') as "project.name_original",
ifnull("name", ' ')          as "project.name",
ifnull("code_name", ' ')     as "project.code_name",
ifnull("Com", ' ')           as "project.Com",
'NAV'                        AS "project.system"
FROM project