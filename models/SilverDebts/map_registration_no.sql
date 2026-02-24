/*map_registration_no:
    MAPPING LOAD
    	id,
        registration_no
    RESIDENT tmp_registration_no;*/

    
SELECT 
    "id",
    "registration_no"
FROM (
    SELECT
        "id",
        "registration_no",
        ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "registration_no" ASC) AS row_num
    FROM {{ref ('tmp_registration_no') }}
) 
WHERE row_num = 1