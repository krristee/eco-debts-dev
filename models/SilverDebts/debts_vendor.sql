/*	CONCATENATE(debts_vendor) 
    LOAD
    	*,
        APPLYMAP('map_business_id',project_code &'|'& department_code &'|'& FLOOR(posting_date),
        	project_code &'|'& department_code &'|1|2958465')					AS business_id;
    LOAD
    	*,                                               
		APPLYMAP('map_department_code_by_cust_vend_code','VendorLedger' &'|'& company_id &'|'& document_no &'|'& vendor_code,
        	APPLYMAP('map_department_code_by_cust_vend_code_and_posting_date',company_id &'|'& document_no &'|'& vendor_code &'|'& posting_date,
        		APPLYMAP('map_department_code_by_cust_vend_code2',company_id &'|'& document_no &'|'& vendor_code,
      				APPLYMAP('map_department_code_by_posting_date',company_id &'|'& document_no &'|'& posting_date,'$(vL.unknown_value)'))))
       																			AS department_code,
		APPLYMAP('map_project_code_by_cust_vend_code','VendorLedger' &'|'& company_id &'|'& document_no &'|'& vendor_code,
        	APPLYMAP('map_project_code_by_cust_vend_code_and_posting_date',company_id &'|'& document_no &'|'& vendor_code &'|'& posting_date,
        		APPLYMAP('map_project_code_by_cust_vend_code2',company_id &'|'& document_no &'|'& vendor_code,
      				APPLYMAP('map_project_code_by_posting_date',company_id &'|'& document_no &'|'& posting_date,'$(vL.unknown_value)'))))
       																			AS project_code,
      APPLYMAP('map_cust_vend_type',posting_group,'$(vL.unknown_value)')		AS cust_vend_type                                                
    RESIDENT tmp_debts_vendor; DROP TABLE tmp_debts_vendor;;*/

SELECT 
    dv.*,  
    
    IFNULL(bi."id",CONCAT(COALESCE(pcc."project_code",pccpd."project_code",pcc2."project_code",pcpd."project_code",' '),'|', COALESCE(dcc."department_code", dccpd."department_code", dcc2."department_code", dcpd."department_code", ' ') ,'|1900-01-01|9999-12-31'))
                                                AS "business_id",
    COALESCE(dcc."department_code", dccpd."department_code", dcc2."department_code", dcpd."department_code", ' ')
                                                AS "department_code",
    COALESCE(pcc."project_code",pccpd."project_code",pcc2."project_code",pcpd."project_code",' ')
                                                AS "project_code",
    IFNULL(mc."Customer / Vendor Type", ' ')    AS "cust_vend_type"                                        
FROM {{ref ('tmp_debts_vendor') }} dv

LEFT JOIN {{ ref('map_project_code_by_cust_vend_code') }}                    pcc on EQUAL_NULL(concat('VendorLedger','|', "company_id" ,'|', "document_no",'|', "vendor_code_NAV"), pcc."id")
LEFT JOIN {{ ref('map_project_code_by_cust_vend_code_and_posting_date') }}   pccpd on EQUAL_NULL(concat("company_id" ,'|', "document_no",'|', "vendor_code_NAV",'|',"posting_date"), pccpd."id_by_cust_vend_code_and_posting_date")
LEFT JOIN {{ ref('map_project_code_by_cust_vend_code2') }}                   pcc2 on EQUAL_NULL(concat("company_id" ,'|', "document_no",'|', "vendor_code_NAV"), pcc2."id_by_cust_vend_code")
LEFT JOIN {{ ref('map_project_code_by_posting_date') }}                      pcpd on EQUAL_NULL(concat("company_id" ,'|', "document_no",'|',"posting_date"), pcpd."id_by_posting_date")

LEFT JOIN {{ ref('map_department_code_by_cust_vend_code') }}                    dcc on EQUAL_NULL(concat('VendorLedger','|', "company_id" ,'|', "document_no",'|', "vendor_code_NAV"), dcc."id")
LEFT JOIN {{ ref('map_department_code_by_cust_vend_code_and_posting_date') }}   dccpd on EQUAL_NULL(concat("company_id" ,'|', "document_no",'|', "vendor_code_NAV",'|',"posting_date"), dccpd."id_by_cust_vend_code_and_posting_date")
LEFT JOIN {{ ref('map_department_code_by_cust_vend_code2') }}                   dcc2 on EQUAL_NULL(concat("company_id" ,'|', "document_no",'|', "vendor_code_NAV"), dcc2."id_by_cust_vend_code")
LEFT JOIN {{ ref('map_department_code_by_posting_date') }}                      dcpd on EQUAL_NULL(concat("company_id" ,'|', "document_no",'|',"posting_date"), dcpd."id_by_posting_date")

LEFT JOIN {{ ref('map_business_id') }}            bi on EQUAL_NULL(CONCAT(COALESCE(pcc."project_code",pccpd."project_code",pcc2."project_code",pcpd."project_code",' '), '|', COALESCE(dcc."department_code", dccpd."department_code", dcc2."department_code", dcpd."department_code", ' '), '|',"posting_date") , bi."tmp_id_date")

LEFT JOIN {{ ref('map_cust_vend_type') }}         mc on EQUAL_NULL("posting_group",mc."Posting group")