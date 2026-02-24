/*    tmp_map_name:
    LOAD
    	'Customer' AS Ledger,
        "Registration No_",
         Name & ' '&  "Name 2" as Name,
         APPLYMAP('map_company_code',APPLYMAP('map_company_name',Company_QLIK),'SA') AS "Company code"	
    FROM [$(vG.ExtractNAVQVDPath)\2.MD/1.ALL/Customer.qvd] (qvd)
    WHERE len(trim("Registration No_"))>4 AND LEN(TRIM(Name)&TRIM("Name 2"))>0 AND Blocked <> 3;
    CONCATENATE(tmp_map_name)
    LOAD
        'Vendor' AS Ledger,
        "Registration No_",
         Name & ' '&  "Name 2" as Name,
		 APPLYMAP('map_company_code',APPLYMAP('map_company_name',Company_QLIK),'SA') AS "Company code"	         
    FROM [$(vG.ExtractNAVQVDPath)\2.MD/1.ALL/Vendor.qvd] (qvd)
	WHERE len(trim("Registration No_"))>4 AND LEN(TRIM(Name)&TRIM("Name 2"))>0 AND Blocked <> 3;*/

    SELECT
        'Customer'                                              AS "Ledger",
        trim(c."Registration No_")                              AS "Registration_No",
        CONCAT(IFNULL(c."Name",''), ' ', IFNULL(c."Name 2",'')) AS "Name",
        coalesce(cc."Company code", 'SA')                       AS "Company code",
        c."timestamp"
    FROM {{ source("NAV", "Customer") }} c
    LEFT JOIN {{ref ('map_company_name') }} cn on c."Com" = cn."Com"
    LEFT JOIN {{ref ('map_company_code') }} cc on cn."Name" = cc."Company name"
    WHERE LENGTH(trim(c."Registration No_")) > 4 AND LENGTH(TRIM(c."Name") || TRIM(c."Name 2")) > 0 AND "Blocked" <> 3

union all

    SELECT
        'Vendor'                                                AS "Ledger",
        trim(v."Registration No_")                              AS "Registration No_",
        concat(IFNULL(v."Name",''), ' ', IFNULL(v."Name 2",'')) AS "Name",
        coalesce(cc."Company code", 'SA')                       AS "Company code",
        v."timestamp"
    FROM {{ source("NAV", "Vendor") }} v
    LEFT JOIN {{ref ('map_company_name') }} cn on EQUAL_NULL(v."Com", cn."Com")
    LEFT JOIN {{ref ('map_company_code') }} cc on EQUAL_NULL(cn."Name",cc."Company name")	         
	WHERE LENGTH(trim(v."Registration No_")) > 4 AND LENGTH(TRIM(v."Name") || TRIM(v."Name 2")) > 0 AND "Blocked" <> 3