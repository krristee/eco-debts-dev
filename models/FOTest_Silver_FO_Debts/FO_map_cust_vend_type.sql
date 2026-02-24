SELECT 
    "Posting group",
    "Customer / Vendor Type"
FROM (
    SELECT 
        UPPER(TRIM("Posting group")) AS "Posting group",
       "Customer / Vendor Type",
        ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM("Posting group")) ORDER BY "Customer / Vendor Type" ASC) AS row_num
    FROM {{ source("FF", "FINANCE_CUST_VEND_TYPE") }}
) 
WHERE row_num = 1