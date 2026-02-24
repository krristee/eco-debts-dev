SELECT 
    "registration_no",
    "name"
FROM (
    SELECT
        "registration_no",
        "name",
        ROW_NUMBER() OVER (PARTITION BY "registration_no" ORDER BY CASE WHEN "name" = ' ' THEN 1 ELSE 0 END ASC , "name" ASC ) AS row_num
    FROM {{ref ('FO_tmp_registration_no') }}
    where "registration_no" <> ''
) 
WHERE row_num = 1