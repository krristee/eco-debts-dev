SELECT  
    "Project code",  
    "Sub business code"  
FROM (  
    SELECT  
        "Project code"            as "Project code",  
        "Sub business code"       as "Sub business code",  
        ROW_NUMBER() OVER (PARTITION BY "Project code" ORDER BY "Sub business code" ASC) AS row_num  
    FROM {{ source("FF", "SUB_BUSINESS_CODE") }} )  
WHERE row_num = 1