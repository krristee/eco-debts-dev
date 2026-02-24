/*map_biller_name_actual:
    MAPPING LOAD
    	APPLYMAP('map_QLIK_company',"Company code",'$(vL.unknown_value)') &'|'& "Biller NAV",
        "Biller actual"
    FROM [$(vG.ExtractFlatFilesQVDPath)2.Finance\finance_biller_actual.qvd] (qvd);  */

SELECT 
    "Biller NAV",
    "Biller actual"
FROM (
    SELECT
        concat(c."company.id", '|', "Biller NAV") as "Biller NAV",
        "Biller actual",
        ROW_NUMBER() OVER (PARTITION BY concat(c."company.id", '|', "Biller NAV") ORDER BY "Biller actual" ASC) AS row_num
    FROM {{ source("FF", "FINANCE_BILLER_ACTUAL") }} fb
    LEFT JOIN {{ref ('NAV_company') }} c on fb."Company code" = c."company.code"
) 
WHERE row_num = 1