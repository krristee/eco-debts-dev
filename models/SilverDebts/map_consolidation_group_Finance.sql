/*map_consolidation_group:
    MAPPING LOAD
    	APPLYMAP('map_QLIK_company',"Company code",'$(vL.unknown_value)') &'|'& "Customer / Vendor NAV code" as id,
        "Consolidation Group"
 	FROM [$(vG.ExtractFlatFilesQVDPath)1.Common\consolidation_group.qvd] (qvd);*/

SELECT 
    "id",
    "Consolidation Group"
FROM (
    SELECT 
        concat(IFNULL(c."company.id", ''), '|', cg."Customer / Vendor NAV code") as "id",
        cg."Consolidation Group",
        ROW_NUMBER() OVER (PARTITION BY concat(IFNULL(c."company.id", ''), '|', cg."Customer / Vendor NAV code") ORDER BY cg."Consolidation Group" ASC) AS row_num
    FROM {{ source("FF", "CONSOLIDATION_GROUP") }} cg
    LEFT JOIN {{ref ('NAV_company') }} c on EQUAL_NULL(cg."Company code", c."company.code")
    where cg."Posting group" is not null
) 
WHERE row_num = 1