/*	tmp_cust_vend:
    LOAD
    	Company_QLIK,
    	1 AS source_type,
    	No_,
        "Registration No_",
        APPLYMAP('map_name',"Registration No_",Name) as Name,
        "Customer Posting Group" AS cust_vend_registration_group
    FROM [$(vG.ExtractNAVQVDPath)\2.MD/1.ALL/Customer.qvd] (qvd);
    CONCATENATE(tmp_cust_vend)
    LOAD
    	Company_QLIK,
    	2 AS source_type,
    	No_,
        "Registration No_",
        APPLYMAP('map_name',"Registration No_",Name) as Name,
        "Vendor Posting Group" AS cust_vend_registration_group
    FROM [$(vG.ExtractNAVQVDPath)\2.MD/1.ALL/Vendor.qvd] (qvd);
    CONCATENATE(tmp_cust_vend)
    LOAD
        Company_QLIK,
    	3 AS source_type,
        No_,
        Name
    FROM [$(vG.ExtractNAVQVDPath)\2.MD/1.ALL/Bank Account.qvd] (qvd);
    CONCATENATE(tmp_cust_vend)
    LOAD
        Company_QLIK,
    	4 AS source_type,
        No_,
        Description AS Name
    FROM [$(vG.ExtractNAVQVDPath)\2.MD/1.ALL/Fixed Asset.qvd] (qvd);*/
    

    SELECT
    	c."Com",
    	1 AS "source_type",
    	c."No_",
        trim(c."Registration No_")      AS "Registration No_",
        coalesce(map."Name", c."Name")  AS "Name",
        "Customer Posting Group" AS "cust_vend_registration_group"
    FROM {{ source("NAV", "Customer") }} c
    LEFT JOIN {{ref ('map_name') }} map on trim(c."Registration No_") = map."Registration_No"
    where "Blocked" <> 3

    UNION ALL

    SELECT
    	v."Com",
    	2 AS source_type,
    	v."No_",
        trim(v."Registration No_")      AS "Registration No_",
        coalesce(map."Name", v."Name") AS "Name",
        v."Vendor Posting Group" AS "cust_vend_registration_group"  
    FROM {{ source("NAV", "Vendor") }} v
    LEFT JOIN {{ref ('map_name') }} map on trim(v."Registration No_") = map."Registration_No"
    where "Blocked" <> 3

    UNION ALL

    SELECT
        "Com",
    	3 AS source_type,
        "No_",
        null,
        "Name",
        null
    FROM {{ source("NAV", "Bank Account") }}

    UNION ALL

    SELECT
        "Com",
    	4 AS source_type,
        "No_",
        null,
        "Description" AS "Name",
        null
    FROM {{ source("NAV", "Fixed Asset") }}