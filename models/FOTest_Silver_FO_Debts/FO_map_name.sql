SELECT 
    "Registration_No",
    "Name"
FROM (
    SELECT 
        "Registration_No",
        "Name",
        ROW_NUMBER() OVER (PARTITION BY "Registration_No" ORDER BY "Priority" asc , "timestamp" desc) AS row_num
    FROM {{ref ('FO_tmp_map_name2') }}
) 
WHERE row_num = 1