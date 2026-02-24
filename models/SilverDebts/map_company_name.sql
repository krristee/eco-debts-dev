/*map_company_name:
   	MAPPING LOAD
      [Company_QLIK],
      [Name]
 	FROM [$(vG.ExtractNAVQVDPath)2.MD/1.ALL/Company Information.qvd] (qvd);*/

    
SELECT 
    "Com",
    "Name"
FROM(
    select "Com",
        "Name",
        ROW_NUMBER() OVER (PARTITION BY "Com" ORDER BY "Name" ASC) AS row_num

    FROM {{ source("NAV", "Company Information") }}
) 
WHERE row_num = 1