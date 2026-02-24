/*tmp_debts_customer:
	NOCONCATENATE LOAD
 	  [Company_QLIK] 															AS company_id,
      TEXT("Customer No_")														AS customer_code,  
      "Entry No_"																AS entry_no,
      TEXT("Document No_")														AS document_no,
      TEXT("External Document No_") 											AS external_document_no,  
      TEXT("Transaction No_")													AS transaction_no,                                            
      APPLYMAP('map_consolidation_group', [Company_QLIK] &'|'& TEXT("Customer No_"),'Not Group') 
      																			AS consolidation_group, 
      APPLYMAP('map_cust_vend_posting_group', 'Customer' &'|'& [Company_QLIK] &'|'& TEXT("Customer No_"), '$(vG.unknown_value_txt)')
      																			AS posting_group,                                                                                               
      IF(FLOOR("Due Date")<FLOOR(MAKEDATE(2005,1,1)),'$(vG.unknown_value_txt)',DATE("Due Date"))	//Remove mistakes: 1/1/1753, 1/21/1931	
    																			AS due_date, 
      DATE("Document Date")														AS document_date,  
      IF(FLOOR("Due Date")<FLOOR(MAKEDATE(2005,1,1)),'$(vG.unknown_value_txt)',DATE("Due Date")) - DATE("Document Date")	
      																			AS payment_term_calc,       
	  APPLYMAP('map_registration_no','Customer' &'|'& [Company_QLIK] &'|'& [Customer No_],'$(vL.unknown_value)')
      																			AS cust_vend_registration_no, 
      APPLYMAP('map_name_by_registration_no',APPLYMAP('map_registration_no','Customer' &'|'& [Company_QLIK] &'|'& [Customer No_],'$(vL.unknown_value)'), '$(vL.unknown_value)')
      																			AS cust_vend_name,  
      IF(FLOOR("Reminder Sending Date")<FLOOR(MAKEDATE(2005,1,1)),'$(vG.unknown_value_txt)',DATE("Reminder Sending Date"))				
      																			AS reminder_date,                                          
      TEXT(Description)															AS description,
      Open																		AS open_flag,
      [Company_QLIK] &'|'& TEXT("Customer No_") &'|'& DATE("Posting Date") 		AS customer_search 
	FROM [$(vG.ExtractNAVQVDPath)\1.TD\1.ALL\Cust_ Ledger Entry\Cust_ Ledger Entry.qvd] (qvd)
    
    LEFT JOIN(tmp_debts_customer)
    LOAD   
      [Company_QLIK] 															AS company_id,
      DATE("Posting Date")														AS posting_date,      
      "Cust_ Ledger Entry No_"													AS entry_no,
      TEXT("Document Type")														AS document_type_code, 
      APPLYMAP('map_document_type', "Document Type",'')							AS document_type_name,            
      "Currency Code"															AS currency_code,
      ROUND("Amount (LCY)", 0.01)												AS amount_lcy                                     
    FROM [$(vG.ExtractNAVQVDPath)\1.TD\1.ALL\Detailed Cust_ Ledg_ Entry\Detailed Cust_ Ledg_ Entry.qvd] (qvd)
    WHERE [Company_QLIK]='$(vCompany)';*/

with tmp_debts_customer as (
SELECT
 	cle."Com" 							            AS "company_id",
    --TO_VARCHAR(coalesce(NULLIF(c."Original Customer No_", ''),cle."Customer No_"))  
    --                                                AS "customer_code" , -- Kristina : permušiau NAV kodą į FO 2025 08 05
    --to_varchar(cle."Customer No_")				AS "customer_code",
    case when fo_cust."customer.code" is null then TO_VARCHAR(cle."Customer No_")  
    else NULLIF(c."Original Customer No_", '') end  AS "customer_code", -- Kristina 2025-09-15: sąlyga dėl FO code
    cle."Entry No_"								    AS "entry_no",
    to_varchar(cle."Document No_")				    AS "document_no",
    to_varchar(cle."External Document No_") 	    AS "external_document_no",  
    to_varchar(cle."Transaction No_")			    AS "transaction_no",
    --IFNULL(cg."Consolidation Group",'Not Group')    
    IFNULL(coalesce(cgg."Consolidation Group",cg."Consolidation Group"),'Not Group')
                                                    AS "consolidation_group", 
    --to_varchar(mcvp."posting_group")	            AS "posting_group",
    coalesce(to_varchar(mcvp."posting_group"),to_varchar(mcvp2."posting_group"))	            
                                                    AS "posting_group",
    CASE WHEN cle."Due Date" < DATE('2005-01-01') THEN null
    ELSE DATE(cle."Due Date") END	                AS "due_date",
    DATE(cle."Document Date")					    AS "document_date",  
    CASE WHEN cle."Due Date" < DATE('2005-01-01') THEN null
    ELSE DATE(cle."Due Date") - DATE(cle."Document Date") END
                                                    AS "payment_term_calc",       
	coalesce(rn."registration_no", rn2."registration_no" )
                                                    AS "cust_vend_registration_no", 
    --mrn."name"              AS "cust_vend_name",
    coalesce(NULLIF(FO_mrn."name", ''), NULLIF(mrn."name", ''), NULLIF(fo_cust."customer.name", ''), NULLIF(nav_cust."customer.name", ''))   
                                                    AS "cust_vend_name",
    CASE WHEN DATE("Reminder Sending Date") < DATE('2005-01-01') THEN NULL
         ELSE DATE("Reminder Sending Date") END     AS "reminder_date",
    to_varchar(cle."Description")				    AS "description",
    "Open"										    AS "open_flag",
    concat(IFNULL(cle."Com",''), '|', IFNULL(TO_VARCHAR(coalesce(NULLIF(c."Original Customer No_", ''),cle."Customer No_")) ,''), '|', DATE(cle."Posting Date")) 
                                                    AS "customer_search",

  --- left join 
  DATE(le."Posting Date")					        AS "posting_date",
    to_varchar(cle."Document Type")			        AS "document_type_code", 
    dt."b"                                          AS "document_type_name",
    le."Currency Code"						        AS "currency_code",
    le."Amount (LCY)"			                    AS "amount_lcy",
    cast(CASE 
            WHEN p_company."Company_load_dateto2" < md."Posting Date" then p_company."Company_load_dateto2"
            WHEN p_company."Company_load_dateto2" is not null and (cle."Open" <> 0 or md."Suma"  <> 0 ) THEN p_company."Company_load_dateto2"
			WHEN cle."Open" = 0 or md."Suma"  = 0 THEN md."Posting Date"
			ELSE TO_DATE('2050-01-01', 'YYYY-MM-DD') 
		END as date)								AS "CloseDate",
    c."No_"                                         AS "customer_code_NAV"    
    
FROM {{ source("NAV", "Cust_ Ledger Entry") }}              cle
left join {{ source("NAV", "Customer" ) }}                  c       on EQUAL_NULL(c."No_",to_varchar(cle."Customer No_")) and c."Com" = cle."Com"
LEFT JOIN {{ref ('map_consolidation_group_Finance') }}      cg      on EQUAL_NULL(concat(cle."Com", '|', to_varchar(cle."Customer No_")), cg."id")
LEFT JOIN {{ref ('map_registration_no') }}                  rn      on EQUAL_NULL(concat('Customer|', cle."Com", '|', cle."Customer No_"), rn."id")
LEFT JOIN {{ref ('FO_map_registration_no') }}               rn2     on EQUAL_NULL(concat('Customer|', cle."Com", '|', c."Original Customer No_"), rn2."id")
LEFT JOIN {{ref ('map_name_by_registration_no') }}          mrn     on EQUAL_NULL(coalesce(rn2."registration_no", rn."registration_no" ), mrn."registration_no")
LEFT JOIN {{ref('FO_map_name_by_registration_no')}}         FO_mrn  on EQUAL_NULL(coalesce(rn2."registration_no", rn."registration_no" ), FO_mrn."registration_no")
LEFT JOIN {{ref ('map_cust_vend_posting_group') }}          mcvp    on EQUAL_NULL(mcvp."id", concat('Customer|', cle."Com", '|', TO_VARCHAR(cle."Customer No_")))
LEFT JOIN {{ ref('FO_map_cust_vend_posting_group') }}       mcvp2   on EQUAL_NULL(mcvp2."id", concat('Customer|', cle."Com", '|', TO_VARCHAR(c."Original Customer No_")))
LEFT JOIN {{ source("NAV", "Detailed Cust_ Ledg_ Entry") }} le      on EQUAL_NULL(le."Com",cle."Com") and EQUAL_NULL(le."Cust_ Ledger Entry No_", cle."Entry No_")
LEFT JOIN {{ref ('map_document_type') }}                    dt      on EQUAL_NULL(cle."Document Type" , dt."a")
LEFT JOIN (SELECT MAX("Posting Date") AS "Posting Date", SUM("Amount (LCY)") AS "Suma", "Cust_ Ledger Entry No_", "Com"
                        FROM {{ source("NAV", "Detailed Cust_ Ledg_ Entry") }}
                        group by "Cust_ Ledger Entry No_","Com") as md  on md."Cust_ Ledger Entry No_" = cle."Entry No_" and md."Com" = cle."Com"
left join {{ref('FO_map_NAV_BC')}}                          nb          on EQUAL_NULL(nb."Com", cle."Com")
left join {{ source("FF", "DEBTS_COMPANIES") }}             p_company   on EQUAL_NULL(p_company."Company code",nb.dataread) and p_company."Data Source" = 'NAV'
LEFT JOIN {{ref ('map_consolidation_group') }}              cgg         on EQUAL_NULL(concat(nb.dataread, '|', to_varchar(cle."Customer No_")), cgg."id")
left join (select "customer.code","customer.id", "customer.name" ,ROW_NUMBER() OVER (PARTITION BY "customer.id" ORDER BY "customer.code" ASC) AS row_num 
            FROM {{ ref('FO_customer') }}  )                fo_cust     on EQUAL_NULL(fo_cust."customer.id",concat(cle."Com", '|',  c."Original Customer No_"))
                                                                            and fo_cust.row_num = 1
left join (select "customer.code","customer.id" ,"customer.name",ROW_NUMBER() OVER (PARTITION BY "customer.id" ORDER BY "customer.code" ASC) AS row_num 
            FROM {{ ref('NAV_customer') }}  )               nav_cust     on EQUAL_NULL(nav_cust."customer.id",concat(cle."Com", '|', cle."Customer No_"))
                                                                            and nav_cust.row_num = 1  

where IFNULL(p_company."Company_load_dateto2",'2050-01-01') > DATE(le."Posting Date")
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
IFNULL("document_type_code", '')                    AS "document_type_code", 
IFNULL("document_type_name", '')                    AS "document_type_name",
IFNULL("currency_code", '')                         AS "currency_code",
IFNULL("amount_lcy", 0)                             AS "amount_lcy",
"CloseDate"                                         as "CloseDate",
"customer_code_NAV"                                 AS "customer_code_NAV"
FROM tmp_debts_customer