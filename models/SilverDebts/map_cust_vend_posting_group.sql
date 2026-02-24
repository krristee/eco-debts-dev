/*tmp_posting_group:
    LOAD
        'Vendor' &'|'& vendor.qlik_company_id &'|'& vendor.code AS id,
        vendor.posting_group AS posting_group
    FROM [$(vG.TransformMasterDataQVDPath)\2.MD/vendor.qvd]
    (qvd);

	CONCATENATE(tmp_posting_group)
    LOAD
        'Customer' &'|'& customer.qlik_company_id &'|'& customer.code AS id,
        customer.posting_group AS posting_group
    FROM [$(vG.TransformMasterDataQVDPath)\2.MD/customer.qvd]
    (qvd);
    
    map_cust_vend_posting_group:
    MAPPING LOAD
    	id, 
        posting_group
    RESIDENT tmp_posting_group;
    DROP TABLE tmp_posting_group;*/

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
        FROM {{ ref('NAV_vendor') }}

        UNION ALL

        select 
            CONCAT('Customer','|', "customer.Com" ,'|', "customer.code")    AS "id",
            "customer.posting_group"                                        AS "posting_group"
        FROM {{ ref('NAV_customer') }}
        )
)
WHERE row_num = 1


