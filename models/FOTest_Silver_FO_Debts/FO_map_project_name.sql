SELECT  
    "Project code",  
    "Project name"  
FROM (  
    SELECT  
        "Project code"       as "Project code",  
        "Project name"       as "Project name",  
        ROW_NUMBER() OVER (PARTITION BY "Project code" ORDER BY "Project name" ASC) AS row_num  
    FROM {{ source("FF", "BUSINESS_CODE") }} )  
WHERE row_num = 1