SELECT 
    "Id",
    "department_code"
FROM (
    SELECT
        "Id",
        "department_code",
        ROW_NUMBER() OVER (PARTITION BY "Id" ORDER BY "department_code" ASC) AS row_num
    FROM {{ref ('FO_tmp_dim_from_gl') }}
    WHERE LEN(TRIM("department_code"))>0
    ) 
WHERE row_num = 1