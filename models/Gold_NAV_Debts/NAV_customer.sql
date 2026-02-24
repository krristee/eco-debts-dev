/*customer:
      LOAD
          [Company_QLIK] &'|'& TEXT(No_) 										AS id,
          TEXT(No_) 															AS code,
          Name 																	AS name_original,
          APPLYMAP('map_cust_vend_name',[Company_QLIK] &'|1|'& TEXT(No_),'$(vL.unknown_value)') 
          																		AS name,
          Address &' '&"Address 2" 												AS address,
          "Post Code" 															AS post_code,
          City																	AS city,
          "Country Code" 														AS country_code,  
          TEXT("Registration No_") 												AS registration_no,
          TEXT("VAT Registration No_") 											AS vat_no,
          TEXT("Payment Terms Code")											AS payment_terms_code,
          "Salesperson Code"													AS salesperson_code,
          ApplyMap('map_status', "Salesperson Code", 'Other') 					AS status,
          ApplyMap('map_status_hist', "Salesperson Code", 'Other') 				AS status_hist,                                        
          APPLYMAP('map_debt_owner', "Salesperson Code", '$(vL.unknown_value)')	AS debt_owner,
          "Customer Posting Group"												AS posting_group,
          "Gen_ Bus_ Posting Group"												AS gen_bus_posting_group,
          "VAT Bus_ Posting Group"												AS vat_bus_posting_group,
          Contact																AS contact_original,
          TEXT("Phone No_") & IF(LEN(TEXT("Phone No_"))>0 and LEN("E-Mail")>0,';') & "E-Mail"
          																		AS contact,
          TEXT("Phone No_")														AS phone_no,
          "E-Mail"																AS email,
          Date("Last Date Modified")											AS modified_at,
          APPLYMAP('map_consolidation_group',[Company_QLIK] &'|'& [Registration No_],'Not Group')
          																		AS consolidation_group,
          APPLYMAP('map_cust_vend_type',[Customer Posting Group],'$(vL.unknown_value)') 
          																		AS type,
           [Company_QLIK] 														AS qlik_company_id
      FROM [$(vG.ExtractNAVQVDPath)\2.MD\1.ALL\Customer.qvd] (qvd);*/

WITH customer AS (
SELECT
    IFNULL(c."Com",'') || '|' || IFNULL(TO_VARCHAR(c."No_"),'') 
                                                    AS "id", -- visad NAV, nepermušinėt,buvo:IFNULL(TO_VARCHAR(coalesce(NULLIF(c."Original Customer No_", ''),c."No_")),'')
    TO_VARCHAR(c."No_")                             AS "code",  -- visad NAV, nepermušinėt,buvo: TO_VARCHAR(coalesce(NULLIF(c."Original Customer No_", ''),c."No_"))
    TO_VARCHAR(c."No_") 				            AS "nav_code",
    TO_VARCHAR(c."Original Customer No_") 			AS "fo_code",
    c."Name" 							            AS "name_original",
    IFNULL(FO_mrn."name",nav_cvn."Name")            AS "name",-- Kristina 2025-11-17: sukeista vietom IFNULL(nav_cvn."Name",FO_mrn."name") 2025-09-11: permušt tiems, kurie nėra nav: 2025 09 05 :nepermušt,  2025-08-12 permuštas NAV map į FO IFNULL(FO_mrn."name",nav_cvn."Name")  
    IFNULL(c."Address",'') || ' ' || IFNULL(c."Address 2",'') 	        
                                                    AS "address",
    c."Post Code" 					                AS "post_code",
    c."City"							            AS "city",
    c."Country Code" 				                AS "country_code",  
    TO_VARCHAR(trim(c."Registration No_"))	        AS "registration_no",
    TO_VARCHAR(c."VAT Registration No_")            AS "vat_no",
    TO_VARCHAR(c."Payment Terms Code")	            AS "payment_terms_code",
    c."Salesperson Code"			                AS "salesperson_code",
    coalesce(st."Status actual", 'Other')           AS "status",
    coalesce(sh."Status Historical", 'Other')       AS "status_hist",                                        
    dto."Debt owner"                                AS "debt_owner",
    c."Customer Posting Group"		                AS "posting_group",
    c."Gen_ Bus_ Posting Group"		                AS "gen_bus_posting_group",
    c."VAT Bus_ Posting Group"		                AS "vat_bus_posting_group",
    c."Contact"						                AS "contact_original",
    IFNULL(TO_VARCHAR(c."Phone No_"),' ') || 
    CASE 
        WHEN LENGTH(TO_VARCHAR(c."Phone No_")) > 0 AND LENGTH(c."E-Mail") > 0 
        THEN ';' 
        ELSE '' 
    END || 
    IFNULL(c."E-Mail",' ')                          AS "contact",
    TO_VARCHAR(c."Phone No_")				        AS "phone_no",
    c."E-Mail"						                AS "email",
    Date(c."Last Date Modified")	                AS "modified_at",
    --coalesce(cg."Consolidation Group", 'Not Group') 
    IFNULL(coalesce(cgg."Consolidation Group",cg."Consolidation Group"),'Not Group')
                                                    AS "consolidation_group",
    cvt."Customer / Vendor Type"                    AS "type",
    c."Com" 				                        AS "Com"
    FROM {{ source("NAV", "Customer") }}                c
    LEFT JOIN {{ref ('map_cust_vend_name') }}           nav_cvn on concat(c."Com", '|1|', TO_VARCHAR(c."No_")) = nav_cvn."id"
    LEFT JOIN {{ref('FO_map_name_by_registration_no')}} FO_mrn  on EQUAL_NULL(TO_VARCHAR(trim(c."Registration No_")), FO_mrn."registration_no")
    LEFT JOIN {{ref ('map_status') }}                   st      on c."Salesperson Code" = st."Salesperson code"
    LEFT JOIN {{ref ('map_status_hist') }}              sh      on c."Salesperson Code" = sh."Salesperson code"
    LEFT JOIN {{ref ('map_debt_owner') }}               dto     on c."Salesperson Code" = dto."Salesperson code"
    LEFT JOIN {{ref ('map_consolidation_group') }}      cg      on concat(c."Com", '|', trim(c."Registration No_")) = cg."id"
    LEFT JOIN {{ref ('map_cust_vend_type') }}           cvt     on c."Customer Posting Group" = cvt."Posting group"
    LEFT JOIN {{ ref('NAV_company') }}                  company ON EQUAL_NULL(company."company.id",c."Com")
    left join {{ source("FF", "DEBTS_COMPANIES") }}   p_company on EQUAL_NULL(p_company."Company code",company."company.code") and p_company."Data Source" = 'NAV'
    LEFT JOIN {{ref ('map_consolidation_group') }}      cgg     on concat(company."company.code", '|',  c."No_") = cgg."id"
    
where (IFNULL(p_company."Company_load_dateto2",'2050-01-01') > DATE(getdate()) and "Company_load" = 'TRUE') 
or c."Original Customer No_" --not in (Select "customer_code" FROM {{ ref('FO_debts_customer') }})
 not in (Select "customer.code" FROM {{ ref('FO_customer') }})
    
    --where company."company.id" = c."Com"        
    --where c."Com" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta')
)

SELECT 
 ifnull("id", ' ')                       AS "customer.id"
,ifnull("code", ' ')                     AS "customer.code"
,ifnull("nav_code", ' ')                 as "customer.nav_code"
,ifnull("fo_code", ' ')                  as "customer.fo_code"
,ifnull("name_original", ' ')            AS "customer.name_original"
,ifnull("name", ' ')                     AS "customer.name"
,ifnull("address", ' ')                  AS "customer.address"
,ifnull("post_code", ' ')                AS "customer.post_code"
,ifnull("city", ' ')                     AS "customer.city"
,ifnull("country_code", ' ')             AS "customer.country_code"
,ifnull("registration_no", '')          AS "customer.registration_no"
,ifnull("vat_no", ' ')                   AS "customer.vat_no"
,ifnull("payment_terms_code", ' ')       AS "customer.payment_terms_code"
,ifnull("salesperson_code", ' ')         AS "customer.salesperson_code"
,ifnull("status", ' ')                   AS "customer.status"
,ifnull("status_hist", ' ')              AS "customer.status_hist"
,ifnull("debt_owner", ' ')               AS "customer.debt_owner"
,ifnull("posting_group", ' ')            AS "customer.posting_group"
,ifnull("gen_bus_posting_group", ' ')    AS "customer.gen_bus_posting_group"
,ifnull("vat_bus_posting_group", ' ')    AS "customer.vat_bus_posting_group"
,ifnull("contact_original", ' ')         AS "customer.contact_original"
,ifnull("contact", ' ')                  AS "customer.contact"
,ifnull("phone_no", ' ')                 AS "customer.phone_no"
,ifnull("email", ' ')                    AS "customer.email"
,ifnull("modified_at", ' ')              AS "customer.modified_at"
,ifnull("consolidation_group", ' ')      AS "customer.consolidation_group"
,ifnull("type", ' ')                     AS "customer.type"
,ifnull("Com", ' ')                      AS "customer.Com"
,'NAV'                                   AS "customer.system"
FROM customer