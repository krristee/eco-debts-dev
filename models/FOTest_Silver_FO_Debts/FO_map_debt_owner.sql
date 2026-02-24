SELECT 
    "Salesperson code",
    "Debt owner"
FROM (
    SELECT
        "Salesperson code",
        "Debt owner",
        ROW_NUMBER() OVER (PARTITION BY "Salesperson code" ORDER BY "Debt owner" ASC) AS row_num
    FROM {{ source("FF", "FINANCE_CUSTOMER_DEBT_STATUS") }}
) 
WHERE row_num = 1