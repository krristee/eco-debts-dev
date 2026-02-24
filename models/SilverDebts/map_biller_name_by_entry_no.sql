/* map_biller_name_by_entry_no:
    MAPPING LOAD
  		id_by_entry_no,
        biller_name
    RESIDENT tmp_dim_from_gl
    WHERE LEN(TRIM(biller_name))>0
    ORDER BY timestamp ASC;*/

SELECT 
    "id_by_entry_no",
    "biller_name"
FROM (
    SELECT
        "id_by_entry_no",
        "biller_name",
        ROW_NUMBER() OVER (PARTITION BY "id_by_entry_no" ORDER BY "timestamp","biller_name" ASC) AS row_num
    FROM {{ref ('tmp_dim_from_gl') }}
    WHERE LEN(TRIM("biller_name"))>0
    ) 
WHERE row_num = 1    