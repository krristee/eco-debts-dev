/*tmp_registration_no:
    LOAD   
    	'Vendor' &'|'& vendor.id AS id,
    	vendor.registration_no AS registration_no,
        vendor.name AS name
    FROM [$(vG.TransformMasterDataQVDPath)\2.MD/vendor.qvd]
    (qvd); 
    
    CONCATENATE(tmp_registration_no)
    LOAD   
    	'Customer' &'|'& customer.id AS id,
    	customer.registration_no AS registration_no,
        customer.name AS name
    FROM [$(vG.TransformMasterDataQVDPath)\2.MD/customer.qvd]; */

SELECT
    	concat('Vendor', '|', IFNULL("vendor.id",''))   AS "id",
    	"vendor.registration_no"                        AS "registration_no",
        "vendor.name"                                   AS "name"
FROM {{ref ('FO_vendor') }} 

UNION ALL

SELECT
    concat('Customer', '|', IFNULL("customer.id",''))   AS "id",
    "customer.registration_no"                          AS "registration_no",
    "customer.name"                                     AS "name"
FROM {{ref ('FO_customer') }} 