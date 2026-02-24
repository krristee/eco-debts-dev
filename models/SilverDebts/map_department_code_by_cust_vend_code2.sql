/*  map_department_code_by_cust_vend_code2:
    MAPPING LOAD
        id_by_cust_vend_code,
        department_code
    RESIDENT tmp_dim_from_gl
    WHERE LEN(TRIM(department_code))>0
    ORDER BY timestamp DESC;*/


SELECT 
    "id_by_cust_vend_code",
    "department_code"
FROM (
    SELECT
        "id_by_cust_vend_code",
        "department_code",
        ROW_NUMBER() OVER (PARTITION BY "id_by_cust_vend_code" ORDER BY "timestamp" DESC,"department_code" ASC) AS row_num
    FROM {{ref ('tmp_dim_from_gl') }}
    WHERE LEN(TRIM("department_code"))>0
    ) 
WHERE row_num = 1