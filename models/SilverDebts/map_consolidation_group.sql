/*map_consolidation_group:
    MAPPING LOAD
    	APPLYMAP('map_QLIK_company',"Company code",'$(vL.unknown_value)') &'|'& "Customer / Vendor NAV code" as id,
        "Consolidation Group"
 	FROM [$(vG.ExtractFlatFilesQVDPath)1.Common\consolidation_group.qvd] (qvd);*/



SELECT 
    "id",
    "Consolidation Group"
FROM(
    select 
        concat("Company code", '|', "Customer / Vendor NAV code") as "id",
        "Consolidation Group",
        ROW_NUMBER() OVER (PARTITION BY concat("Company code", '|', "Customer / Vendor NAV code") ORDER BY "Consolidation Group" ASC) AS row_num
    FROM {{ source("FF", "CONSOLIDATION_GROUP") }}
    ) 
WHERE row_num = 1
    