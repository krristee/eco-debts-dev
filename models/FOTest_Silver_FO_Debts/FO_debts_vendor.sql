SELECT 
    dv.*,  
    IFNULL(bi."id",CONCAT(COALESCE(dv."project_code",' '),'|', COALESCE(dv."department_code", ' ') ,'|1900-01-01|9999-12-31'))
                                                    AS "business_id", 
    IFNULL(mv."Customer / Vendor Type", ' ')        AS "cust_vend_type"                                       
FROM {{ref ('FO_tmp_debts_vendor') }}       dv
LEFT JOIN {{ ref('map_business_id') }}      bi on EQUAL_NULL(CONCAT(COALESCE(dv."project_code",' '), '|', COALESCE(dv."department_code", ' '), '|',"posting_date") , bi."tmp_id_date")
LEFT JOIN {{ ref('map_cust_vend_type') }}   mv on EQUAL_NULL("posting_group",mv."Posting group")


