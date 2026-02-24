with department_name as (
    SELECT 
    "Department code",
    "Department name"
FROM (
    SELECT
        "Department code",
        "Department name",
        ROW_NUMBER() OVER (PARTITION BY "Department code" ORDER BY "Department name" ASC) AS row_num
    FROM {{ source("FF", "LOCATION") }}
    ) 
WHERE row_num = 1
),


department as (
SELECT
    d."Code"	                                            AS "code",
    null                                                    AS "name_original",
    dn."Department name"                                    AS "name",
    concat(d."Code", ' ', ifnull(dn."Department name", '')) AS "code_name",
    nb."Com" 				                                AS "Com"          
FROM (select distinct "xa_padalinysvalue" as "Code",
        from {{ source("FOD", "DIMENSIONATTRIBUTEVALUECOMBINATION") }} ) d
cross join {{ref('FO_map_NAV_BC') }}       nb 
LEFT JOIN department_name                   dn      on EQUAL_NULL(d."Code", dn."Department code")
LEFT JOIN {{ref('FO_company') }}           company ON EQUAL_NULL(company."company.id",nb."Com")
WHERE d."Code" <> '' and company."company.id" = nb."Com" --in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta','UAB Marijampolės švara')
)

SELECT 
ifnull("code", ' ')          as "department.code",
ifnull("name_original", ' ') as "department.name_original",
ifnull("name", ' ')          as "department.name",
ifnull("code_name", ' ')     as "department.code_name",
ifnull("Com", ' ')           as "department.Com",
'FO'                         as "department.system"
FROM department