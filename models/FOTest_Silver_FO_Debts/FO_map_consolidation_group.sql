SELECT 
    "id",
    "Consolidation Group"
FROM(
    select 
        concat("Company code", '|', "Customer / Vendor NAV code") as "id",
        "Consolidation Group",
        ROW_NUMBER() OVER (PARTITION BY concat("Company code", '|', "Customer / Vendor NAV code") ORDER BY "Consolidation Group" ASC) AS row_num
    FROM {{ source("FF", "CONSOLIDATION_GROUP") }}
    where "Consolidation Group" is not null
    ) 
WHERE row_num = 1
    