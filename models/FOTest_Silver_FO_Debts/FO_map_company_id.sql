SELECT 
    "code",
    "id"
FROM (
    SELECT
        "company.code"  as "code",
        "company.id"    as "id",
        ROW_NUMBER() OVER (PARTITION BY "company.code" ORDER BY "company.id" ASC) AS row_num
    FROM {{ref ('FO_company') }}
) 
WHERE row_num = 1