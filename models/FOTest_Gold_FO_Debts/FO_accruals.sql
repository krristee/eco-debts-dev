WITH accruals as (
SELECT
    ID                                  AS "id",
    "% of Accrual"                      AS "percent",
    "Aging step FROM (days)"            AS "rate_start",
        CASE 
        WHEN LENGTH(TRIM("Aging step TO (days)")) is null THEN 10000 
        ELSE "Aging step TO (days)" END AS "rate_end",
    "VALID FROM"                        AS "valid_from",
    "VALID TO"                          AS "valid_to"
FROM {{ source("FF", "ACCRUALS") }}
)      
SELECT 
    c."date"                                    as "accruals.date",
    ifnull(a."id", 0)                           as "accruals.id",
    ifnull(a."percent", 0)                      as "accruals.percent",
    ifnull(a."rate_start", 0)                   as "accruals.rate_start",
    ifnull(a."rate_end", 0)                     as "accruals.rate_end",
    a."valid_from"                              as "accruals.valid_from",
    a."valid_to"                                as "accruals.valid_to",
    'FO'                                        as "accruals.system"
FROM {{ref ('date') }} c
LEFT JOIN accruals a
    ON c."date" >= a."valid_from" AND c."date"<= a."valid_to"