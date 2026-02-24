SELECT 
    "id_by_external_doc_no",
    "department_code"
FROM (
    SELECT
        "id_by_external_doc_no",
        "department_code",
        ROW_NUMBER() OVER (PARTITION BY "id_by_external_doc_no" ORDER BY "timestamp","department_code" ASC) AS row_num
    FROM {{ref ('tmp_dim_from_gl') }}
    WHERE LEN(TRIM("department_code"))>0
    ) 
WHERE row_num = 1