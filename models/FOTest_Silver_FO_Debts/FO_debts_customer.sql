SELECT 
    dc.*,
    --coalesce(bna."Biller actual",coalesce(bn."biller_name", bnpd."biller_name" , bne."biller_name", exd."biller_name" ,' '))
    coalesce(exd."biller_name",inv."biller_name" ,' ')  AS "biller_name_actual",
    --coalesce(bn."biller_name", bnpd."biller_name" , bne."biller_name" , exd."biller_name",' ')
    coalesce(exd."biller_name",inv."biller_name" ,' ')  AS "biller_name",
    IFNULL(bi."id",CONCAT(COALESCE(dc."project_code",' '),'|', COALESCE(dc."department_code", ' ') ,'|1900-01-01|9999-12-31'))
                                                        AS "business_id",                                                
    IFNULL(mc."Customer / Vendor Type", ' ')            AS "cust_vend_type"       

FROM {{ref ('FO_tmp_debts_customer') }}                 dc

LEFT JOIN {{ ref('map_business_id') }}                  bi      on EQUAL_NULL(CONCAT(COALESCE(dc."project_code",' '), '|', COALESCE(dc."department_code", ' '), '|',"posting_date") , bi."tmp_id_date")
--LEFT JOIN {{ ref('map_biller_name') }}                  bn      on EQUAL_NULL(concat('CustomerLedger','|', "company_id" ,'|', "document_no"), bn."id")
--LEFT JOIN {{ ref('map_biller_name_by_posting_date') }}  bnpd    on EQUAL_NULL(concat("company_id" ,'|', "document_no",'|',"posting_date"), bnpd."id_by_posting_date")
--LEFT JOIN {{ ref('map_biller_name_by_entry_no') }}      bne     on EQUAL_NULL(concat("company_id" ,'|', "entry_no"), bne."id_by_entry_no")
--LEFT JOIN {{ ref('map_biller_name_actual') }}           bna     on EQUAL_NULL(concat(dc."company_id", '|', coalesce(bn."biller_name", bnpd."biller_name" , bne."biller_name" ,' ')), bna."Biller NAV")
LEFT JOIN {{ ref('map_cust_vend_type') }}               mc      on EQUAL_NULL("posting_group",mc."Posting group")

LEFT JOIN {{ ref('map_biller_by_external_document') }}  exd     on EQUAL_NULL(concat("company_id" ,'|', "external_document_no"), exd."id_by_external_doc_no") and "external_document_no" <> ''
LEFT JOIN {{ ref('FO_map_biller_by_invoiceid') }}       inv     on EQUAL_NULL(concat("company_id" ,'|', "customer_code",'|', "external_document_no"), inv."id_by_invoiceid") and "external_document_no" <> ''


