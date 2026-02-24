SELECT
    ci."id"	                                           AS "company_id",
    to_varchar(trim(sl."Customer registration code"))  AS "cust_vend_registration_no",
    to_varchar(sl."Customer NAV code") 			       AS "customer_code",
--         "Customer name",
	rn."name"                                          AS "cust_vend_name", 
    sl."Status of Debt / Customer" 				       AS "customer_status_log",
    sl."Valid from" 							       AS "valid_from",
	sl."Valid to"								       AS "valid_to",
    c."date"                                           AS "posting_date"
FROM  {{ source("FF", "FINANCE_CUSTOMER_DEBT_STATUS_LOG") }}    sl
LEFT JOIN {{ref ('FO_map_company_id') }}                           ci on EQUAL_NULL(sl."Company code" , ci."code")
LEFT JOIN {{ref ('FO_map_name_by_registration_no') }}              rn on EQUAL_NULL(to_varchar(trim(sl."Customer registration code")) , rn."registration_no")
LEFT JOIN {{ref ('FO_date') }}                                     c  on sl."Valid from" <= c."date" and sl."Valid to" >= c."date"
WHERE ci."id" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta')
and sl."System" = 'FO'