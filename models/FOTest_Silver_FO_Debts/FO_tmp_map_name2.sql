/*LEFT JOIN(tmp_map_name)
    LOAD
    	Ledger,
    	"Company code",
        "Priority"
    FROM [$(vG.ExtractFlatFilesQVDPath)1.Common\customer_vendor_name_priority.qvd] (qvd); */

SELECT 
     map."Ledger"
    ,map."Registration_No"
    ,map."Name"
    ,map."Company code"
    ,map."timestamp"
    ,np."Priority"
FROM {{ref ('FO_tmp_map_name') }} map
LEFT JOIN {{ source("FF", "CUSTOMER_VENDOR_NAME_PRIORITY") }} np on EQUAL_NULL(map."Ledger", np."Ledger") and EQUAL_NULL(map."Company code", np."Company code")