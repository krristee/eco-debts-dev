/*vendor:
      LOAD
          [Company_QLIK] &'|'& TEXT(No_) 										AS id,
          TEXT(No_) 															AS code,
          Name																	AS name_original,
          APPLYMAP('map_cust_vend_name',[Company_QLIK] &'|2|'& TEXT(No_),'$(vL.unknown_value)') 
          																		AS name,          
           Address &' '&"Address 2" 											AS address,
          "Post Code" 															AS post_code,
          City 																	AS city,
          "Country Code" 														AS country_code,
          TEXT("Registration No_") 												AS registration_no,
          TEXT("VAT Registration No_") 											AS vat_no,
          TEXT("Payment Terms Code") 											AS payment_term_code,
          "Purchaser Code" 														AS purchaser_code,
          "Vendor Posting Group" 												AS posting_group,
          "Gen_ Bus_ Posting Group" 											AS gen_bus_posting_group,
          "VAT Bus_ Posting Group" 												AS vat_bus_posting_group,
          Contact 																AS contact_original,
          TEXT("Phone No_") & IF(LEN(TEXT("Phone No_"))>0 and LEN("E-Mail")>0,';') & "E-Mail"
          																		AS contact,
          TEXT("Phone No_") 													AS phone_no,
          "E-Mail" 																AS email,
          DATE("Last Date Modified") 											AS modified_at,
          APPLYMAP('map_consolidation_group',[Company_QLIK] &'|'& [Registration No_],'Not Group')
          																		AS consolidation_group,
          APPLYMAP('map_cust_vend_type',[Vendor Posting Group],'$(vL.unknown_value)') AS type,
           [Company_QLIK] 														AS qlik_company_id          
      FROM [$(vG.ExtractNAVQVDPath)\2.MD\1.ALL\Vendor.qvd] (qvd);*/


with vendor as(
SELECT
    IFNULL(v."Com",'') || '|' || IFNULL(TO_VARCHAR(v."No_"),'')       -- visad NAV, nepermušinėt,buvo:     
                                                    AS "id",
    TO_VARCHAR(v."No_")                             AS "code",  -- visad NAV, nepermušinėt,buvo: TO_VARCHAR(coalesce(NULLIF(v."Original Vendor No_", ''),v."No_"))
    TO_VARCHAR(v."No_") 					        AS "nav_code",
    TO_VARCHAR(v."Original Vendor No_") 			AS "fo_code",
    v."Name"							            AS "name_original",
    IFNULL(FO_mrn."name",nav_cvn."Name")            AS "name",--  Kristina:2025-11-17 sukeista vietom, 2025-09-11: permušt tiems, kurie nėra nav, 2025 09 05 :nepermušt, 2025-08-12 permuštas NAV map į FO IFNULL(FO_mrn."name",nav_cvn."Name")  
    IFNULL(v."Address",'') || ' ' || IFNULL(v."Address 2",'')             
                                                    AS "address",
    v."Post Code" 					                AS "post_code",
    v."City" 							            AS "city",
    v."Country Code" 				                AS "country_code",
    TO_VARCHAR(trim(v."Registration No_"))	        AS "registration_no",
    TO_VARCHAR(v."VAT Registration No_") 	        AS "vat_no",
    TO_VARCHAR(v."Payment Terms Code") 	            AS "payment_term_code",
    v."Purchaser Code" 				                AS "purchaser_code",
    v."Vendor Posting Group" 		                AS "posting_group",
    v."Gen_ Bus_ Posting Group" 	                AS "gen_bus_posting_group",
    v."VAT Bus_ Posting Group" 		                AS "vat_bus_posting_group",
    v."Contact" 						            AS "contact_original",
        IFNULL(TO_VARCHAR(v."Phone No_"),'') || 
    CASE 
        WHEN LENGTH(TO_VARCHAR(v."Phone No_")) > 0 AND LENGTH(v."E-Mail") > 0 
        THEN ';' 
        ELSE '' 
    END || 
    IFNULL(v."E-Mail",'')                           AS "contact",
    TO_VARCHAR(v."Phone No_") 			            AS "phone_no",
    v."E-Mail" 						                AS "email",
    DATE(v."Last Date Modified") 	                AS "modified_at",
    --coalesce(cg."Consolidation Group", 'Not Group') 
    IFNULL(coalesce(cgg."Consolidation Group",cg."Consolidation Group"),'Not Group')
                                                    AS "consolidation_group",
    cvt."Customer / Vendor Type"                    AS "type",
    v."Com" 				                        AS "Com"          
    FROM {{ source("NAV", "Vendor") }}                  v
    LEFT JOIN {{ref ('map_consolidation_group') }}      cg      on concat(v."Com", '|', trim(v."Registration No_")) = cg."id"
    LEFT JOIN {{ref ('map_cust_vend_type') }}           cvt     on v."Vendor Posting Group" = cvt."Posting group"
    LEFT JOIN {{ref ('map_cust_vend_name') }}           nav_cvn on concat(v."Com", '|2|', TO_VARCHAR(v."No_")) = nav_cvn."id"
    LEFT JOIN {{ref('FO_map_name_by_registration_no')}} FO_mrn  on EQUAL_NULL(TO_VARCHAR(trim(v."Registration No_")), FO_mrn."registration_no")
    LEFT JOIN {{ ref('NAV_company') }}                  company ON EQUAL_NULL(company."company.id",v."Com")
    left join {{ source("FF", "DEBTS_COMPANIES") }}   p_company on EQUAL_NULL(p_company."Company code",company."company.code") and p_company."Data Source" = 'NAV'
    LEFT JOIN {{ref ('map_consolidation_group') }}      cgg     on concat(company."company.code", '|',  v."No_") = cgg."id"

where (IFNULL(p_company."Company_load_dateto2",'2050-01-01') > DATE(getdate()) and "Company_load" = 'TRUE') 
or v."Original Vendor No_" --not in (Select "vendor_code" FROM {{ ref('FO_debts_vendor') }})
 not in (Select "vendor.code" FROM {{ ref('FO_vendor') }})
    
     --where v."Com" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta')
)

select 
 ifnull("id", ' ')                    as "vendor.id"
,ifnull("code", ' ')                  as "vendor.code"
,ifnull("nav_code", ' ')              as "vendor.nav_code"
,ifnull("fo_code", ' ')               as "vendor.fo_code"
,ifnull("name_original", ' ')         as "vendor.name_original"
,ifnull("name", ' ')                  as "vendor.name"
,ifnull("address", ' ')               as "vendor.address"
,ifnull("post_code", ' ')             as "vendor.post_code"
,ifnull("city", ' ')                  as "vendor.city"
,ifnull("country_code", ' ')          as "vendor.country_code"
,ifnull("registration_no", '')       as "vendor.registration_no"
,ifnull("vat_no", ' ')                as "vendor.vat_no"
,ifnull("payment_term_code", ' ')     as "vendor.payment_term_code"
,ifnull("purchaser_code", ' ')        as "vendor.purchaser_code"
,ifnull("posting_group", ' ')         as "vendor.posting_group"
,ifnull("gen_bus_posting_group", ' ') as "vendor.gen_bus_posting_group"
,ifnull("vat_bus_posting_group", ' ') as "vendor.vat_bus_posting_group"
,ifnull("contact_original", ' ')      as "vendor.contact_original"
,ifnull("contact", ' ')               as "vendor.contact"
,ifnull("phone_no", ' ')              as "vendor.phone_no"
,ifnull("email", ' ')                 as "vendor.email"
,ifnull("modified_at", ' ')           as "vendor.modified_at"
,ifnull("consolidation_group", ' ')   as "vendor.consolidation_group"
,ifnull("type", ' ')                  as "vendor.type"
,ifnull("Com", ' ')                   as "vendor.Com"   
,'NAV'                                AS "vendor.system" 
from vendor