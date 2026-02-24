SELECT 
    "id",
    "last_reminder_date"
FROM (
    SELECT
        concat("company_id", '|', "customer_code")  as "id",
        DATE(MAX("reminder_date"))                  AS "last_reminder_date",
        ROW_NUMBER() OVER (PARTITION BY concat("company_id", '|', "customer_code") ORDER BY DATE(MAX("reminder_date")) ASC) AS row_num
    FROM {{ref ('FO_debts_customer') }}
    WHERE LEN(TRIM("reminder_date"))> '0'
    GROUP BY concat("company_id", '|', "customer_code")
    ) 
WHERE row_num = 1