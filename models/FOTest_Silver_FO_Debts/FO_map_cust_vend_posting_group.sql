SELECT "id",
        "posting_group"   
FROM (        
    SELECT 
        "id",
        "posting_group",
        ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "posting_group" ASC) AS row_num
    FROM(    
        select 
            CONCAT('Vendor','|', "vendor.Com" ,'|', "vendor.code")          AS "id",
            "vendor.posting_group"                                          AS "posting_group"
        FROM {{ ref('FO_vendor') }}

        UNION ALL

        select 
            CONCAT('Customer','|', "customer.Com" ,'|', "customer.code")    AS "id",
            "customer.posting_group"                                        AS "posting_group"
        FROM {{ ref('FO_customer') }}
        )
)
WHERE row_num = 1


