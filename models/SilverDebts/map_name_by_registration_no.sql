/*map_name_by_registration_no:
    MAPPING LOAD
        registration_no,
        name
    RESIDENT tmp_registration_no;
    DROP TABLE tmp_registration_no;*/

SELECT 
    "registration_no",
    "name"
FROM (
    SELECT
        "registration_no",
        "name",
        ROW_NUMBER() OVER (PARTITION BY "registration_no" ORDER BY "name" ASC) AS row_num
    FROM {{ref ('tmp_registration_no') }}
) 
WHERE row_num = 1