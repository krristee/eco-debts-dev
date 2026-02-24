SELECT 
    "id",
    "Consolidation Group"
FROM (
    SELECT 
        concat(IFNULL(c."company.id", ''), '|', cg."Posting group") as "id",
        cg."Consolidation Group",
        ROW_NUMBER() OVER (PARTITION BY concat(IFNULL(c."company.id", ''), '|', cg."Posting group") ORDER BY cg."Consolidation Group" ASC) AS row_num
    FROM {{ source("FF", "CONSOLIDATION_GROUP") }} cg


    LEFT JOIN {{ref ('FO_company') }} c on EQUAL_NULL(cg."Company code", c."company.code")
    where cg."Posting group" is not null
) 

WHERE row_num = 1