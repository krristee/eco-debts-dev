SELECT 
    "Id",
    "project_code"
FROM (
    SELECT
        concat("type",'|',"Id") as "Id",
        "project_code",
        ROW_NUMBER() OVER (PARTITION BY concat("type",'|',"Id") ORDER BY "project_code" ASC) AS row_num
    FROM {{ref ('FO_tmp_dim_from_gl') }}
    WHERE LEN(TRIM("project_code"))>0
) 
WHERE row_num = 1