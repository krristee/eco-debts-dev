/* tmp_debts_vendor:
    LOAD
 	  [Company_QLIK] 															AS company_id,
      TEXT("Vendor No_")														AS vendor_code,      
      "Entry No_"																AS entry_no,
      TEXT("Document No_")														AS document_no,
      TEXT("External Document No_") 											AS external_document_no,  
      TEXT("Transaction No_")													AS transaction_no,
      APPLYMAP('map_consolidation_group', [Company_QLIK] &'|'& TEXT("Vendor No_"),'Not Group') 
      																			AS consolidation_group,
      APPLYMAP('map_cust_vend_posting_group', 'Vendor' &'|'& [Company_QLIK] &'|'& TEXT("Vendor No_"), '$(vG.unknown_value_txt)')
      																			AS posting_group,                                        
      IF(FLOOR("Due Date")<FLOOR(MAKEDATE(2005,1,1)),'$(vG.unknown_value_txt)',DATE("Due Date"))	//Remove mistakes: 1/1/1753, 1/21/1931	
    																			AS due_date,
      DATE("Document Date")														AS document_date,                                                 
      IF(FLOOR("Due Date")<FLOOR(MAKEDATE(2005,1,1)),'$(vG.unknown_value_txt)',DATE("Due Date")) - DATE("Document Date")	
      																			AS payment_term_calc,                                          
	  APPLYMAP('map_registration_no','Vendor' &'|'& [Company_QLIK] &'|'& [Vendor No_],'$(vL.unknown_value)')
      																			AS cust_vend_registration_no,
      APPLYMAP('map_name_by_registration_no',APPLYMAP('map_registration_no','Vendor' &'|'& [Company_QLIK] &'|'& [Vendor No_],'$(vL.unknown_value)'), '$(vL.unknown_value)')
      																			AS cust_vend_name,
      TEXT(Description)															AS description,
      Open																		AS open_flag
    FROM [$(vG.ExtractNAVQVDPath)\1.TD\1.ALL\Vendor Ledger Entry\Vendor Ledger Entry.qvd] (qvd)
    WHERE [Company_QLIK]='$(vCompany)';
    
	LEFT JOIN(tmp_debts_vendor)
	LOAD 
      [Company_QLIK] 															AS company_id,
      DATE("Posting Date")														AS posting_date,
      "Vendor Ledger Entry No_"													AS entry_no,
      TEXT("Document Type")														AS document_type_code,  
      APPLYMAP('map_document_type', "Document Type",'')							AS document_type_name,          
      "Currency Code"															AS currency_code,
      ROUND("Amount (LCY)", 0.01)												AS amount_lcy    
	FROM [$(vG.ExtractNAVQVDPath)\1.TD\1.ALL\Detailed Vendor Ledg_ Entry\Detailed Vendor Ledg_ Entry.qvd] (qvd)
    WHERE [Company_QLIK]='$(vCompany)';
    
    */

with tmp_debts_vendor as (
SELECT
 	vle."Com" 							                AS "company_id",
    --TO_VARCHAR(coalesce(NULLIF(v."Original Vendor No_", ''),vle."Vendor No_"))     
    --                                                    AS "vendor_code", -- Kristina : permušiau NAV kodą į FO 2025 08 05
    --to_varchar(vle."Vendor No_")	                    AS "vendor_code",
    case when fo_vend."vendor.code" is null then TO_VARCHAR(vle."Vendor No_")  
    else NULLIF(v."Original Vendor No_", '') end        AS "vendor_code",  -- Kristina 2025-09-15: sąlyga dėl FO code
    vle."Entry No_"								        AS "entry_no",
    to_varchar(vle."Document No_")				        AS "document_no",
    to_varchar(vle."External Document No_") 	        AS "external_document_no",  
    to_varchar(vle."Transaction No_")			        AS "transaction_no",
    --IFNULL(cg."Consolidation Group",'Not Group')    
    IFNULL(coalesce(cgg."Consolidation Group",cg."Consolidation Group"),'Not Group')
                                                    AS "consolidation_group",  
    --to_varchar(mcvp."posting_group")	                AS "posting_group",
    coalesce(to_varchar(mcvp."posting_group"),to_varchar(mcvp2."posting_group"))	            
                                                    AS "posting_group",
    CASE WHEN vle."Due Date" < DATE('2005-01-01') THEN null
    ELSE DATE(vle."Due Date") END	                    AS "due_date",
    DATE(vle."Document Date")					        AS "document_date",  
    CASE WHEN vle."Due Date" < DATE('2005-01-01') THEN null
    ELSE DATE(vle."Due Date") - DATE(vle."Document Date") END
                                                        AS "payment_term_calc",       
	coalesce(rn."registration_no", rn2."registration_no" )
                                                        AS "cust_vend_registration_no",
    --mrn."name"                                          AS "cust_vend_name",
    coalesce(NULLIF(FO_mrn."name", ''), NULLIF(mrn."name", ''), NULLIF(fo_vend."vendor.name", ''), NULLIF(nav_vend."vendor.name", '') )
                                                        AS "cust_vend_name",
    to_varchar(vle."Description")				        AS "description",
    "Open"										        AS "open_flag",
    concat(IFNULL(vle."Com",''), '|', IFNULL(TO_VARCHAR(coalesce(NULLIF(v."Original Vendor No_", ''),vle."Vendor No_")),''), '|', DATE(vle."Posting Date")) 
                                                        AS "customer_search",

  --- left join 
  DATE(le."Posting Date")					            AS "posting_date",      
    to_varchar(vle."Document Type")			            AS "document_type_code", 
    dt."b"                                              AS "document_type_name",       
    le."Currency Code"						            AS "currency_code",
    le."Amount (LCY)"			                        AS "amount_lcy",
    cast(CASE 
            WHEN p_company."Company_load_dateto2" < md."Posting Date" then p_company."Company_load_dateto2"
            WHEN p_company."Company_load_dateto2" is not null and (vle."Open" <> 0 or md."Suma"  <> 0 ) THEN p_company."Company_load_dateto2"
			WHEN vle."Open" = 0 or md."Suma"  = 0 THEN md."Posting Date"
			ELSE TO_DATE('2050-01-01', 'YYYY-MM-DD') 
		END as date)								    AS "CloseDate",
    v."No_"                                             as "vendor_code_NAV"      
      
FROM {{ source("NAV", "Vendor Ledger Entry") }}                 vle
left join {{ source("NAV", "Vendor") }}                         v       on EQUAL_NULL(v."No_",to_varchar(vle."Vendor No_")) and v."Com" = vle."Com"
LEFT JOIN {{ref ('map_consolidation_group_Finance') }}          cg      on EQUAL_NULL(concat(vle."Com", '|', to_varchar(vle."Vendor No_")), cg."id")
LEFT JOIN {{ref ('map_registration_no') }}                      rn      on EQUAL_NULL(concat('Vendor|', vle."Com", '|', vle."Vendor No_"), rn."id")
LEFT JOIN {{ref ('FO_map_registration_no') }}                   rn2     on EQUAL_NULL(concat('Vendor|', vle."Com", '|', TO_VARCHAR(v."Original Vendor No_")), rn2."id")
LEFT JOIN {{ref ('map_name_by_registration_no') }}              mrn     on EQUAL_NULL(coalesce(rn2."registration_no", rn."registration_no" ), mrn."registration_no")
LEFT JOIN {{ref('FO_map_name_by_registration_no')}}             FO_mrn  on EQUAL_NULL(coalesce(rn2."registration_no", rn."registration_no" ), FO_mrn."registration_no")
LEFT JOIN {{ref ('map_cust_vend_posting_group') }}              mcvp    on EQUAL_NULL(mcvp."id", concat('Vendor|', vle."Com", '|', TO_VARCHAR(vle."Vendor No_")))
LEFT JOIN {{ ref('FO_map_cust_vend_posting_group') }}           mcvp2   on EQUAL_NULL(mcvp."id", concat('Vendor|', vle."Com", '|', TO_VARCHAR(v."Original Vendor No_")))
LEFT JOIN {{ source("NAV", "Detailed Vendor Ledg_ Entry") }}    le      on EQUAL_NULL(le."Com",vle."Com") and EQUAL_NULL(le."Vendor Ledger Entry No_", vle."Entry No_")
LEFT JOIN {{ref ('map_document_type') }}                        dt      on EQUAL_NULL(vle."Document Type" , dt."a")
LEFT JOIN (SELECT MAX("Posting Date") AS "Posting Date", SUM("Amount (LCY)") AS "Suma", "Vendor Ledger Entry No_", "Com"
                        FROM {{ source("NAV", "Detailed Vendor Ledg_ Entry") }}
                        group by "Vendor Ledger Entry No_","Com") as md  on md."Vendor Ledger Entry No_" = vle."Entry No_" and md."Com" = vle."Com"
left join {{ref('FO_map_NAV_BC')}}                              nb       on EQUAL_NULL(nb."Com", vle."Com")
left join {{ source("FF", "DEBTS_COMPANIES") }}                 p_company   on EQUAL_NULL(p_company."Company code",nb.dataread) and p_company."Data Source" = 'NAV'
LEFT JOIN {{ref ('map_consolidation_group') }}                  cgg      on EQUAL_NULL(concat(nb.dataread, '|', to_varchar(vle."Vendor No_")), cgg."id")
left join (select "vendor.code","vendor.id","vendor.name" ,ROW_NUMBER() OVER (PARTITION BY "vendor.id" ORDER BY "vendor.code" ASC) AS row_num 
            FROM {{ ref('FO_vendor') }}   )                     fo_vend  on EQUAL_NULL(fo_vend."vendor.id",concat(vle."Com", '|', v."Original Vendor No_"))
                                                                            and fo_vend.row_num = 1
left join (select "vendor.code","vendor.id","vendor.name" ,ROW_NUMBER() OVER (PARTITION BY "vendor.id" ORDER BY "vendor.code" ASC) AS row_num 
            FROM {{ ref('NAV_vendor') }}   )                    nav_vend on EQUAL_NULL(nav_vend."vendor.id",concat(vle."Com", '|', vle."Vendor No_"))
                                                                            and nav_vend.row_num = 1
where IFNULL(p_company."Company_load_dateto2",'2050-01-01') > DATE(le."Posting Date")
)

SELECT 
IFNULL("company_id", '')                            AS "company_id",
IFNULL("vendor_code", '')                           AS "vendor_code",
IFNULL("entry_no", 0)                               AS "entry_no",
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
IFNULL("description", '')                           AS "description",
IFNULL("open_flag", '')                             AS "open_flag",
IFNULL("customer_search", '')                       AS "customer_search",
"posting_date"                                      as "posting_date",
IFNULL("document_type_code", '')                    AS "document_type_code", 
IFNULL("document_type_name", '')                    AS "document_type_name",       
IFNULL("currency_code", '')                         AS "currency_code",
IFNULL("amount_lcy", 0)                             AS "amount_lcy",
"CloseDate"                                         as "CloseDate",
"vendor_code_NAV"                                   as "vendor_code_NAV"

FROM tmp_debts_vendor