/*map_biller_name:
    MAPPING LOAD
  		type &'|'& id,
        biller_name
    RESIDENT tmp_dim_from_gl
    WHERE LEFT(account_group,1)=5 AND LEN(TRIM(biller_name))>0
    ORDER BY timestamp ASC;*/



SELECT 
    "id",
    "biller_name"
FROM (
    SELECT
        concat("type", '|', "id") as "id",
        "biller_name",
        ROW_NUMBER() OVER (PARTITION BY concat("type", '|', "id") ORDER BY "timestamp","biller_name" ASC) AS row_num
    FROM {{ref ('tmp_dim_from_gl') }}
    WHERE LEFT("account_group",1) ='5' AND LEN(TRIM("biller_name"))>0
    ) 
WHERE row_num = 1