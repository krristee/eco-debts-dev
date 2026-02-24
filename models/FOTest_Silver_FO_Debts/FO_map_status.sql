SELECT 
    "Salesperson code",
    "Status actual"
FROM (
    SELECT
        "Salesperson code",
        "Status actual",
        ROW_NUMBER() OVER (PARTITION BY "Salesperson code" ORDER BY "Status actual" ASC) AS row_num
    FROM {{ source("FF", "FINANCE_CUSTOMER_DEBT_STATUS") }}
    WHERE  LEN(TRIM("Status actual"))>0
) 
WHERE row_num = 1