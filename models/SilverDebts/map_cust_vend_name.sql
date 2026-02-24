/*map_cust_vend_name:
    MAPPING LOAD
    	Company_QLIK &'|'& source_type &'|'& No_ AS id,
        Name
    RESIDENT tmp_cust_vend;
    DROP TABLE tmp_cust_vend;*/


SELECT 
    "id",
    "Name"
FROM (
    SELECT 
        CONCAT("Com", '|', "source_type", '|', "No_") AS "id",
        "Name",
        ROW_NUMBER() OVER (PARTITION BY CONCAT("Com", '|', "source_type", '|', "No_") ORDER BY "Name" ASC) AS row_num
    FROM {{ref ('tmp_cust_vend') }}
) 
WHERE row_num = 1