SELECT 
    "id",
    "Name"
FROM (
    SELECT 
        CONCAT("Com", '|', "source_type", '|', "No_") AS "id",
        "Name",
        ROW_NUMBER() OVER (PARTITION BY CONCAT("Com", '|', "source_type", '|', "No_") ORDER BY "Name" ASC) AS row_num
    FROM {{ref ('FO_tmp_cust_vend') }}
) 
WHERE row_num = 1