/*map_last_payment_date:
    MAPPING LOAD
    	company_id &'|'& customer_code 											AS id,
    	DATE(MAX(posting_date)) 												AS last_payment_date
    RESIDENT debts_customer
    WHERE document_type_code=1
    GROUP BY company_id &'|'& customer_code;*/

SELECT 
    "id",
    "last_payment_date"
FROM (
    SELECT
        concat("company_id", '|', "customer_code")  as  "id",
        DATE(MAX("posting_date"))                   AS "last_payment_date",
        ROW_NUMBER() OVER (PARTITION BY concat("company_id", '|', "customer_code") ORDER BY DATE(MAX("posting_date")) ASC) AS row_num
    FROM {{ref ('debts_customer') }}
WHERE "document_type_code"='1'
GROUP BY concat("company_id", '|', "customer_code")
) 
WHERE row_num = 1