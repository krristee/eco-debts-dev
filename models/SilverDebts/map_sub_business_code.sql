/*  map_sub_business_code:
    MAPPING LOAD
    	[Project code],
        [Sub business code]
	FROM [$(vG.ExtractFlatFilesQVDPath)1.Common\sub_business_code.qvd] (qvd);  */


SELECT  
    "Project code",  
    "Sub business code"  
FROM (  
    SELECT  
        "Project code"            as "Project code",  
        "Sub business code"       as "Sub business code",  
        ROW_NUMBER() OVER (PARTITION BY "Project code" ORDER BY "Sub business code" ASC) AS row_num  
    FROM {{ source("FF", "SUB_BUSINESS_CODE") }} )  
WHERE row_num = 1