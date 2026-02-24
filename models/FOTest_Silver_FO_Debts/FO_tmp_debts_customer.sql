with tmp_debts_customer as (
SELECT
 	nb."Com" 							                AS "company_id",
    to_varchar(cust."accountnum")				        AS "customer_code",
    cast(cle."recid" as nvarchar)				        AS "entry_no",
    to_varchar(cle."voucher")				            AS "document_no",-- Kristina : buvo to_varchar(cle."invoice")
    to_varchar(cle."invoice") 	                        AS "external_document_no",  -- Kristina : buvo cle."documentnum" 
    to_varchar(cle."voucher")			                AS "transaction_no",
    IFNULL(cg."Consolidation Group",'Not Group')        AS "consolidation_group", 
    cust."custgroup"	                                AS "posting_group",
    CASE WHEN cle."duedate" < DATE('2005-01-01') THEN null
    ELSE DATE(cle."duedate") END	                    AS "due_date",			            
    /*case when DATE(cle."transdate") = '2025-05-31' and upper(cle."dataareaid") in ('EPR', 'ECO')	
         then CASE WHEN cle."duedate" < DATE('2005-01-01') THEN null
              ELSE DATE(cle."duedate") END
    ELSE DATE(cle."transdate") end*/
    CASE WHEN DATE(cle."transdate") = '2025-05-31' and upper(cle."dataareaid") in ('EPR', 'ECO')
        THEN NULLIF(DATE(cle."documentdate"), '1900-01-01') 
    ELSE DATE(cle."transdate")    END                  AS "document_date",   --DATE(cle."transdate")
    CASE WHEN cle."duedate" < DATE('2005-01-01') THEN null
    ELSE DATE(cle."duedate") - DATE(cle."transdate") END
                                                        AS "payment_term_calc",       
	trim(cust."organizationcode_ecs")                   AS "cust_vend_registration_no", 
    coalesce(mrn."name",cvn."Name")                     AS "cust_vend_name",
    null                                                AS "reminder_date",
    to_varchar(cle."txt")				                AS "description",
    case when cle."closed" ='1900-01-01T00:00:00.0000000' then 1 else 0 
    end	                                                AS "open_flag",
    concat(IFNULL(nb."Com",''), '|', IFNULL(to_varchar(cust."accountnum"),''), '|', DATE(cle."transdate")) 
                                                        AS "customer_search",

  --- left join 
  DATE(cle."transdate")					                AS "posting_date",
    cle."transtype"			                            AS "document_type_code", 
    opt.C5                                              AS "document_type_name",
    cle."currencycode"						            AS "currency_code",
    cle."reportingcurrencyamount"			            AS "amount_lcy",
    COALESCE(dept."department_code",dim."department_code",'') as "department_code",
    COALESCE(proj."project_code",dim."project_code",'') as "project_code",
    case when cle."closed" < '2000-01-01' then p_company."Company_load_dateto2" 
    else cle."closed" end                               as "ClosedDate",
    cle."postingprofile"                                as "posting_profile" 
    
FROM {{source("FOD", "CUSTTRANS")}}				                cle
left join {{ref('FO_map_NAV_BC')}}                              nb      on nb.DATAREAD = upper(cle."dataareaid")
left join {{source("FOD", "CUSTTABLE")}}			            cust    on cust."accountnum" = cle."accountnum" and upper(cust."dataareaid") = upper(cle."dataareaid") 
left join {{source("FOD", "DIRPARTYTABLE")}}                    dp      on cust."party" = dp."recid" 
left join {{ref('FO_tmp_dim_from_gl')}}                         dim     on cle."Id" = dim."Id" 
LEFT JOIN {{ref('FO_map_name_by_registration_no')}}             mrn     on EQUAL_NULL(trim(cust."organizationcode_ecs"), mrn."registration_no")
LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}                 p_company    on EQUAL_NULL(p_company."Company code",upper(cle."dataareaid")) and p_company."Data Source" = 'FO' 
left join {{source("FOD", "GLOBAL_OPTIONSET_METADA")}}          opt     on cast(cle."transtype" as varchar) = opt.C2 and opt.C7 = 'custtrans' and opt.C1 = 'transtype'
LEFT JOIN {{ref ('FO_map_consolidation_group_Finance') }}       cg      on EQUAL_NULL(concat(nb."Com", '|', to_varchar(cust."custgroup")), cg."id")
LEFT JOIN {{ref ('FO_map_cust_vend_name') }}                    cvn     on EQUAL_NULL(concat(nb."Com", '|1|', TO_VARCHAR(cust."accountnum")) , cvn."id")
LEFT JOIN {{ ref('map_department_by_external_doc') }}           dept    on EQUAL_NULL(concat(nb."Com" ,'|', to_varchar(cle."invoice")), dept."id_by_external_doc_no") and to_varchar(cle."invoice") <> ''
LEFT JOIN {{ ref('map_project_by_external_doc') }}              proj    on EQUAL_NULL(concat(nb."Com" ,'|', to_varchar(cle."invoice")), proj."id_by_external_doc_no") and to_varchar(cle."invoice") <> ''

WHERE p_company."Company_load" = 'TRUE'
and IFNULL(p_company."Company_load_datefrom",'2025-05-31') <= DATE(cle."transdate")	 and IFNULL(p_company."Company_load_dateto2",'2050-01-01') > DATE(cle."transdate")

union all

SELECT
 	nb."Com" 							                AS "company_id",
    to_varchar(cust."accountnum")				        AS "customer_code",
    cast(cle."recid" as nvarchar)				        AS "entry_no",
    to_varchar(cle."voucher")				            AS "document_no",-- Kristina : buvo to_varchar(cle."invoice")
    to_varchar(cle."invoice") 	                        AS "external_document_no",  -- Kristina : buvo cle."documentnum" 
    to_varchar(cle."voucher")			                AS "transaction_no",
    IFNULL(cg."Consolidation Group",'Not Group')        AS "consolidation_group", 
    cust."custgroup"	                                AS "posting_group",
    CASE WHEN cle."duedate" < DATE('2005-01-01') THEN null
    ELSE DATE(cle."duedate") END	                    AS "due_date",			            
    /*case when DATE(setl."transdate") = '2025-05-31' and upper(cle."dataareaid") in ('EPR', 'ECO')	
         then CASE WHEN cle."duedate" < DATE('2005-01-01') THEN null
              ELSE DATE(cle."duedate") END
    ELSE DATE(setl."transdate") end*/
    CASE WHEN DATE(setl."transdate") = '2025-05-31' and upper(cle."dataareaid") in ('EPR', 'ECO')
        THEN NULLIF(DATE(cle."documentdate"), '1900-01-01') 
    ELSE DATE(setl."transdate")    END                  AS "document_date",     --DATE(setl."transdate")
    CASE WHEN cle."duedate" < DATE('2005-01-01') THEN null
    ELSE DATE(cle."duedate") - DATE(cle."transdate") END
                                                        AS "payment_term_calc",       
	trim(cust."organizationcode_ecs")                   AS "cust_vend_registration_no", 
    coalesce(mrn."name",cvn."Name")                     AS "cust_vend_name",
    null                                                AS "reminder_date",
    to_varchar(cle."txt")				                AS "description",
    case when cle."closed" ='1900-01-01T00:00:00.0000000' then 1 else 0 
    end	                                                AS "open_flag",
    concat(IFNULL(nb."Com",''), '|', IFNULL(to_varchar(cust."accountnum"),''), '|', DATE(cle."transdate")) 
                                                        AS "customer_search",

  --- left join 
    DATE(setl."transdate")					            AS "posting_date",
    cle."transtype"			                            AS "document_type_code", 
    opt.C5                                              AS "document_type_name",
    cle."currencycode"						            AS "currency_code",
    setl."settleamountreporting"			            AS "amount_lcy",
    COALESCE(dept."department_code",dim."department_code",'')   as "department_code",
    COALESCE(proj."project_code",dim."project_code",'') as "project_code",
    case when cle."closed" < '2000-01-01' then p_company."Company_load_dateto2" 
    else cle."closed" end                               as "ClosedDate",
    cle."postingprofile"                                as "posting_profile" 
    
FROM {{source("FOD", "CUSTSETTLEMENT")}}                        setl
left join {{source("FOD", "CUSTTRANS")}}                        cle     on setl."offsetrecid" = cle."recid"
left join {{source("FOD", "CUSTTABLE")}}			            cust    on cust."accountnum" = cle."accountnum" and cust."dataareaid" = cle."dataareaid"  
left join {{source("FOD", "DIRPARTYTABLE")}}                    dp      on cust."party" = dp."recid" 
left join {{ref('FO_tmp_dim_from_gl')}}                         dim     on cle."Id" = dim."Id" 
left join {{ref('FO_map_NAV_BC')}}                              nb      on nb.DATAREAD = upper(cle."dataareaid")
LEFT JOIN {{ref('FO_map_name_by_registration_no')}}             mrn     on EQUAL_NULL(trim(cust."organizationcode_ecs"), mrn."registration_no")
LEFT JOIN {{ source("FF", "DEBTS_COMPANIES") }}                 p_company    on EQUAL_NULL(p_company."Company code",upper(cle."dataareaid")) and p_company."Data Source" = 'FO'
left join {{source("FOD", "GLOBAL_OPTIONSET_METADA")}}          opt     on cast(cle."transtype" as varchar)= opt.C2 and opt.C7 = 'custtrans' and opt.C1 = 'transtype'
LEFT JOIN {{ref ('FO_map_consolidation_group_Finance') }}       cg      on EQUAL_NULL(concat(nb."Com", '|', to_varchar(cust."custgroup")), cg."id")
LEFT JOIN {{ref ('FO_map_cust_vend_name') }}                    cvn     on EQUAL_NULL(concat(nb."Com", '|1|', TO_VARCHAR(cust."accountnum")) , cvn."id")
LEFT JOIN {{ ref('map_department_by_external_doc') }}           dept    on EQUAL_NULL(concat(nb."Com" ,'|', to_varchar(cle."invoice")), dept."id_by_external_doc_no") and to_varchar(cle."invoice") <> ''
LEFT JOIN {{ ref('map_project_by_external_doc') }}              proj    on EQUAL_NULL(concat(nb."Com" ,'|', to_varchar(cle."invoice")), proj."id_by_external_doc_no") and to_varchar(cle."invoice") <> ''
WHERE p_company."Company_load" = 'TRUE'
and IFNULL(p_company."Company_load_datefrom",'2025-05-31') <= DATE(setl."transdate") and IFNULL(p_company."Company_load_dateto2",'2050-01-01') > DATE(setl."transdate")

)

SELECT 
IFNULL("company_id", '')                            AS "company_id",
IFNULL("customer_code", '')                         AS "customer_code",
IFNULL("entry_no", '')                              AS "entry_no",
IFNULL("document_no", '')                           AS "document_no",
IFNULL("external_document_no", '')                  AS "external_document_no",  
IFNULL("transaction_no", '')                        AS "transaction_no",
IFNULL("posting_group", '')                         AS "posting_group",               
IFNULL("consolidation_group", '')                   AS "consolidation_group",          
"due_date"                                          AS "due_date",
"document_date"                                     AS "document_date",  
"payment_term_calc"                                 AS "payment_term_calc",       
IFNULL("cust_vend_registration_no", '')             AS "cust_vend_registration_no", 
IFNULL("cust_vend_name", '')                        AS "cust_vend_name", 
"reminder_date"                                     AS "reminder_date",
IFNULL("description", '')                           AS "description",
IFNULL("open_flag", '')                             AS "open_flag",
IFNULL("customer_search", '')                       AS "customer_search", 
"posting_date"                                      AS "posting_date",
IFNULL("document_type_code", 0)                     AS "document_type_code", 
IFNULL("document_type_name", '')                    AS "document_type_name",
IFNULL("currency_code", '')                         AS "currency_code",
IFNULL("amount_lcy", 0)                             AS "amount_lcy",
IFNULL("department_code", '')                       as "department_code",
IFNULL("project_code", '')                          as "project_code",
"ClosedDate"                                        as "ClosedDate",
IFNULL("posting_profile", '')                       as "posting_profile" 
FROM tmp_debts_customer

