SELECT  
    "Sub business code",  
    "Sub business name"  
FROM (  
    SELECT  
        "Sub business code"            as "Sub business code",  
        "Sub business name"       as "Sub business name",  
        ROW_NUMBER() OVER (PARTITION BY "Sub business code" ORDER BY "Sub business name" ASC) AS row_num  
    FROM {{ source("FF", "SUB_BUSINESS_NAME") }} )  
WHERE row_num = 1
