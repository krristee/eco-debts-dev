SELECT 
    "Company name",
    "Company code"
FROM (
    SELECT 
        "Company name",
        "Company code",
        ROW_NUMBER() OVER (PARTITION BY "Company name" ORDER BY "Company code" ASC) AS row_num
    FROM {{ source("FF", "COMPANY_NAME") }}
) 
WHERE row_num = 1