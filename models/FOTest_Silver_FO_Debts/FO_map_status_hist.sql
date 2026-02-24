SELECT 
    "Salesperson code",
    "Status Historical"
FROM (
    SELECT
        "Salesperson code",
        "Status Historical",
        ROW_NUMBER() OVER (PARTITION BY "Salesperson code" ORDER BY "Status Historical" ASC) AS row_num
    FROM {{ source("FF", "FINANCE_CUSTOMER_DEBT_STATUS") }}
    WHERE  LEN(TRIM("Status Historical"))>0
) 
WHERE row_num = 1