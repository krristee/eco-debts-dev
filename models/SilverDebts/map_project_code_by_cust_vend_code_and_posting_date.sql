/*map_project_code_by_cust_vend_code_and_posting_date:
    MAPPING LOAD
        id_by_cust_vend_code_and_posting_date,
        project_code
    RESIDENT tmp_dim_from_gl
    WHERE LEN(TRIM(project_code))>0
    ORDER BY timestamp DESC;*/

SELECT 
    "id_by_cust_vend_code_and_posting_date",
    "project_code"
FROM (
    SELECT
        "id_by_cust_vend_code_and_posting_date",
        "project_code",
        ROW_NUMBER() OVER (PARTITION BY "id_by_cust_vend_code_and_posting_date" ORDER BY "timestamp" DESC,"project_code" ASC) AS row_num
    FROM {{ref ('tmp_dim_from_gl') }}
    WHERE LEN(TRIM("project_code"))>0
) 
WHERE row_num = 1