/*    map_name:
    MAPPING LOAD
//     rowno() as order,
//     Ledger,
//     Priority,
    	"Registration No_",
    	Name
    RESIDENT tmp_map_name
    ORDER BY Priority ASC;
    DROP TABLE tmp_map_name;*/

SELECT 
    "Registration_No",
    "Name"
FROM (
    SELECT 
        "Registration_No",
        "Name",
        ROW_NUMBER() OVER (PARTITION BY "Registration_No" ORDER BY "Priority" desc, "timestamp" ASC) AS row_num
    FROM {{ref ('tmp_map_name2') }}
) 
WHERE row_num = 1