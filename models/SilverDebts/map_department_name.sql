/* map_department_name:
	MAPPING LOAD
    	[Department code],
		[Department name]
 	FROM [$(vG.ExtractFlatFilesQVDPath)1.Common\location.qvd] (qvd);*/


SELECT 
    "Department code",
    "Department name"
FROM (
    SELECT
        "Department code",
        "Department name",
        ROW_NUMBER() OVER (PARTITION BY "Department code" ORDER BY "Department name" ASC) AS row_num
    FROM {{ source("FF", "LOCATION") }} 
    ) 
WHERE row_num = 1