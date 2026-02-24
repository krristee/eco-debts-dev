/* map_company_id:
   	MAPPING LOAD
      company.code,
      company.id
 	FROM [$(vG.TransformMasterDataQVDPath)\2.MD/company.qvd](qvd);*/

SELECT 
    "code",
    "id"
FROM (
    SELECT
        "company.code"  as "code",
        "company.id"    as "id",
        ROW_NUMBER() OVER (PARTITION BY "company.code" ORDER BY "company.id" ASC) AS row_num
    FROM {{ref ('NAV_company') }}
) 
WHERE row_num = 1