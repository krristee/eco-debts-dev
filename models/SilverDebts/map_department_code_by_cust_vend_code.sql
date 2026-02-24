/* map_department_code_by_cust_vend_code:
    MAPPING LOAD
        type &'|'& id_by_cust_vend_code,
        department_code
    RESIDENT tmp_dim_from_gl
    WHERE MATCH(LEFT(account_group,1),5,6)>0 AND LEN(TRIM(department_code))>0
    ORDER BY timestamp DESC;*/


SELECT 
    "id",
    "department_code"
FROM (
    SELECT
        concat("type", '|', "id_by_cust_vend_code") as "id",
        "department_code",
        ROW_NUMBER() OVER (PARTITION BY concat("type", '|', "id_by_cust_vend_code") ORDER BY "timestamp" DESC,"department_code" ASC) AS row_num
    FROM {{ref ('tmp_dim_from_gl') }}
    WHERE LEFT("account_group",1) in ('5','6') AND LEN(TRIM("department_code"))>0
    ) 
WHERE row_num = 1