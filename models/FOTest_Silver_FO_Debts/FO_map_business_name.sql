SELECT  
    "Business code",  
    "Business name"  
FROM (  
    SELECT  
        "Business code"       as "Business code",  
        "Business name"       as "Business name",  
        ROW_NUMBER() OVER (PARTITION BY "Business code" ORDER BY "Business name" ASC) AS row_num  
    FROM {{ source("FF", "BUSINESS_NAME") }} )  
WHERE row_num = 1