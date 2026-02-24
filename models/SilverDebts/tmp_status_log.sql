/* tmp_status_log:
	LOAD
        APPLYMAp('map_company_id',"Company code") 							AS debts.company_id,
        TEXT("Customer registration code") 									AS debts.cust_vend_registration_no,
        TEXT("Customer NAV code") 											AS debts.customer_code,
		APPLYMAP('map_name_by_registration_no',TEXT("Customer registration code"), '$(vL.unknown_value)') 
        																	AS debts.cust_vend_name, 
        "Status of Debt / Customer" 										AS debts.customer_status_log,
        "Valid from" 														AS valid_from,
		"Valid to"															AS valid_to
    FROM [$(vG.ExtractFlatFilesQVDPath)2.Finance\finance_customer_debt_status_log.qvd] (qvd);
    
    
      LEFT JOIN(tmp_status_log)
      INTERVALMATCH(date)
      LOAD DISTINCT
      	valid_from,
        valid_to
      RESIDENT tmp_status_log;
      
       LEFT JOIN (debts)
    LOAD
    	debts.company_id,
    	debts.cust_vend_registration_no,
        debts.customer_code,
        debts.customer_status_log,
        DATE(date) 																AS debts.posting_date
    RESIDENT tmp_status_log; */

SELECT
    ci."id"	                                    AS "company_id",
    to_varchar(sl."Customer registration code") AS "cust_vend_registration_no",
    TO_VARCHAR(coalesce(NULLIF(cus."Original Customer No_", ''),sl."Customer NAV code"))
                                                AS "customer_code", -- Kristina : permušiau NAV kodą į FO 2025 08 05
    --to_varchar(sl."Customer NAV code") 		AS "customer_code",
--         "Customer name",
	rn."name"                                   AS "cust_vend_name", 
    sl."Status of Debt / Customer" 				AS "customer_status_log",
    sl."Valid from" 							AS "valid_from",
	sl."Valid to"								AS "valid_to",
    c."date"                                    AS "posting_date"
FROM  {{ source("FF", "FINANCE_CUSTOMER_DEBT_STATUS_LOG") }}    sl
LEFT JOIN {{ref ('map_company_id') }}                           ci on EQUAL_NULL(sl."Company code" , ci."code")
LEFT JOIN {{ref ('map_name_by_registration_no') }}              rn on EQUAL_NULL(to_varchar(sl."Customer registration code") , rn."registration_no")
LEFT JOIN {{ref ('date') }}                                     c  on sl."Valid from" <= c."date" and sl."Valid to" >= c."date"
left join {{ source("NAV", "Customer" ) }}                      cus on EQUAL_NULL(cus."No_",to_varchar(sl."Customer NAV code")) and cus."Com" = ci."id"
WHERE ci."id" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta')
and sl."System" = 'NAV'