with project_name as (SELECT  
                        "Project code",  
                        "Project name"  
                      FROM (  
                            SELECT  
                                "Project code"       as "Project code",  
                                "Project name"       as "Project name",  
                                ROW_NUMBER() OVER (PARTITION BY "Project code" ORDER BY "Project name" ASC) AS row_num  
                            FROM {{ source("FF", "BUSINESS_CODE") }} )  
                            WHERE row_num = 1),

project as (
            SELECT
                d."Code"	                                        AS "code",
                null                                                as "name_original",
                p."Project name"                                    AS "name",
                concat(d."Code", ' ', IFNULL(p."Project name",''))  AS "code_name",
                nb."Com"				                            AS "Com" 
            FROM (select distinct "xb_projektasvalue" as "Code",
        from  {{ source("FOD", "DIMENSIONATTRIBUTEVALUECOMBINATION") }} ) d
            cross join {{ref('FO_map_NAV_BC') }} nb 
            LEFT JOIN project_name                      p on EQUAL_NULL(d."Code" , p."Project code")
            LEFT JOIN {{ref('FO_company') }}  company ON EQUAL_NULL(company."company.id",nb."Com")
            WHERE d."Code" <> '' and company."company.id" = nb."Com" -- d."Com" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta','UAB Marijampolės švara')
            )

SELECT 
ifnull("code", ' ')          as "project.code",
ifnull("name_original", ' ') as "project.name_original",
ifnull("name", ' ')          as "project.name",
ifnull("code_name", ' ')     as "project.code_name",
ifnull("Com", ' ')           as "project.Com",
'FO'                         as "project.system"
FROM project

