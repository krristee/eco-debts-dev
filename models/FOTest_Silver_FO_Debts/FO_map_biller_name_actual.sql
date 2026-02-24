SELECT 
    "Biller NAV",
    "Biller actual"
FROM (
    SELECT
        concat(c."company.id", '|', "Biller NAV") as "Biller NAV",
        "Biller actual",
        ROW_NUMBER() OVER (PARTITION BY concat(c."company.id", '|', "Biller NAV") ORDER BY "Biller actual" ASC) AS row_num
    FROM {{ source("FF", "FINANCE_BILLER_ACTUAL") }} fb
    LEFT JOIN {{ref ('FO_company') }} c on fb."Company code" = c."company.code"
) 
WHERE row_num = 1