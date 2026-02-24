/* tmp_dim_from_gl:
	LOAD DISTINCT
    	[Company_QLIK] &'|'& [Document No_] 												AS id,
        [Company_QLIK] &'|'& [Document No_] &'|'& [Source No_] 								AS id_by_cust_vend_code,
        [Company_QLIK] &'|'& [Document No_] &'|'& [Source No_] &'|'& DATE([Posting Date]) 	AS id_by_cust_vend_code_and_posting_date, 
        [Company_QLIK] &'|'& [Document No_] &'|'& DATE([Posting Date]) 						AS id_by_posting_date, 
        [Company_QLIK] &'|'& [Entry No_] 													AS id_by_entry_no,
        
        PICK(MATCH(LEFT([G_L Account No_],1),5,6),'CustomerLedger','VendorLedger') 			AS type,
        
        "Global Dimension 1 Code" 															AS department_code,
        "Global Dimension 2 Code" 															AS project_code,
         LEFT([G_L Account No_],3) 															AS account_group,
         [User ID] 																			AS biller_name,
         timestamp
    FROM [$(vG.ExtractNAVQVDPath)\1.TD\1.ALL\G_L Entry\G_L Entry.qvd] (qvd);
*/

SELECT DISTINCT
    concat(IFNULL("Com",''), '|', IFNULL("Document No_",''))                                        AS "id",
    concat(IFNULL("Com",''), '|', IFNULL("Document No_",''), '|', IFNULL("Source No_",''))          AS "id_by_cust_vend_code",
    concat(IFNULL("Com",''), '|', IFNULL("Document No_",''), '|', IFNULL("Source No_",''), '|', IFNULL(DATE("Posting Date"),'')) AS "id_by_cust_vend_code_and_posting_date",
    concat(IFNULL("Com",''), '|', IFNULL("Document No_",''), '|', IFNULL(DATE("Posting Date"),''))  AS "id_by_posting_date",
    concat(IFNULL("Com",''), '|', IFNULL("Entry No_",''))                                           AS "id_by_entry_no",
    CASE WHEN LEFT("G_L Account No_", 1) = '5' THEN 'CustomerLedger'
         WHEN LEFT("G_L Account No_", 1) = '6' THEN 'VendorLedger'
    END                                                                                             AS "type",
    "Global Dimension 1 Code"                                                                       AS "department_code",
    "Global Dimension 2 Code"                                                                       AS "project_code",
    LEFT("G_L Account No_",3)                                                                       AS "account_group",
    "User ID"                                                                                       AS "biller_name",
    "timestamp"
    ,concat(IFNULL("Com",''), '|', IFNULL("External Document No_",''))                              as "id_by_external_doc_no"
FROM {{ source("NAV", "G_L Entry") }}