SELECT 
    "id_by_external_doc_no",
    "biller_name"
FROM (
    SELECT
        "id_by_external_doc_no",
        "biller_name",
        ROW_NUMBER() OVER (PARTITION BY "id_by_external_doc_no" ORDER BY "timestamp","biller_name" ASC) AS row_num
    FROM {{ref ('tmp_dim_from_gl') }}
    WHERE LEN(TRIM("biller_name"))>0
    ) 
WHERE row_num = 1