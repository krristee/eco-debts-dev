    WITH   tmp_business AS (
    SELECT 
        "business.id"                AS "id",
        "business.tmp_id"            AS "tmp_id",
        DATE("business.valid_from")  AS "valid_from", 
        DATE("business.valid_to")    AS "valid_to"
    FROM {{ ref('FO_business') }}
    WHERE NOT ("business.valid_from" = '1900-01-01' AND "business.valid_to" = '9999-12-31')
),
business_dates AS (
    SELECT 
        b."id"             as "id",
        b."tmp_id"         as "tmp_id",
        c."calendar.date"           as "date",
         CONCAT(b."tmp_id", '|', c."calendar.date") AS "tmp_id_date"
    FROM tmp_business b
    LEFT JOIN {{ ref('FO_calendar') }} c
        ON DATE(c."calendar.date") >= b."valid_from" AND DATE(c."calendar.date")<= b."valid_to"
)
SELECT 
    "tmp_id_date",
    "id"
FROM (    
    SELECT 
        "tmp_id_date",
        "id" ,
        ROW_NUMBER() OVER (PARTITION BY "id","date" ORDER BY "id" ASC) AS row_num
    FROM business_dates
    ) 
WHERE row_num = 1