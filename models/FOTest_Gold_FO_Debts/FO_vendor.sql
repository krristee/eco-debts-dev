
with
vendor as(
SELECT
    IFNULL(nb."Com",'') || '|' || IFNULL(TO_VARCHAR(v."accountnum"),'')     -- KRISTINA : buvo  dp."partynumber" --> v."accountnum"        
                                                    AS "id",
    TO_VARCHAR(v."accountnum") 					    AS "code",              -- KRISTINA : buvo  dp."partynumber" --> v."accountnum"
    TO_VARCHAR(nv."Original Vendor No_") 			AS "nav_code",
    TO_VARCHAR(v."accountnum") 			            AS "fo_code",           -- KRISTINA : buvo  dp."partynumber" --> v."accountnum"
    dp."name"							            AS "name_original",
    cvn."Name"                                      AS "name",          
    logis."street"                                  AS "address",
    logis."zipcode"					                AS "post_code",
    logis."city"							        AS "city",
    logis."countryregionid"				            AS "country_code",
    TO_VARCHAR(trim(v."organizationcode_ecs"))      AS "registration_no",
    TO_VARCHAR(v."vatnum") 	                        AS "vat_no",
    TO_VARCHAR(v."paymtermid") 	                    AS "payment_term_code",
    '' 				                                AS "purchaser_code",
    v."vendgroup" 		                            AS "posting_group",
    '' 	                                            AS "gen_bus_posting_group",
    '' 		                                        AS "vat_bus_posting_group",
    ''           						            AS "contact_original",
    IFNULL(TO_VARCHAR(loget."locator"),' ') || 
    CASE 
        WHEN LENGTH(TO_VARCHAR(loget."locator")) > 0 AND LENGTH(logem."locator") > 0 
        THEN ';' 
        ELSE '' 
    END || 
    IFNULL(logem."locator",' ')                     AS "contact",
    loget."locator"				                    AS "phone_no",
    logem."locator"					                AS "email",
    Date(ifnull(v."modifiedon",'2000-01-01')) 	    AS "modified_at",
    coalesce(cg."Consolidation Group", 'Not Group') AS "consolidation_group",
    cvt."Customer / Vendor Type"                    AS "type",
    nb."Com" 				                        AS "Com"          
    FROM {{ source("FOD", "VENDTABLE") }}          v
    left join {{ref('FO_map_NAV_BC') }}            nb       on nb.DATAREAD = upper(v."dataareaid")
    left join {{source("FOD", "DIRPARTYTABLE") }}  dp       on v."party" = dp."recid"
    --LEFT JOIN {{ref('map_consolidation_group') }}  cg       on EQUAL_NULL(concat(nb."Com", '|', dp."partynumber") , cg."id") 
    LEFT JOIN {{ref('map_cust_vend_type') }}       cvt      on EQUAL_NULL(v."vendgroup" , cvt."Posting group")
    LEFT JOIN {{ref('FO_map_cust_vend_name') }}    cvn      on EQUAL_NULL(concat(nb."Com", '|2|', TO_VARCHAR(v."accountnum")) , cvn."id")
    left join {{ source("NAV", "Vendor") }}        nv       on v."accountnum" = nv."Original Vendor No_" and nb."Com" = nv."Com"
    --LEFT JOIN 'FO_map_consolidation_group'    cg  on EQUAL_NULL(concat(nb.DATAREAD, '|', TO_VARCHAR(nv."No_")) , cg."id")
    LEFT JOIN {{ref('FO_map_consolidation_group_Finance') }}    cg  on EQUAL_NULL(concat(nb."Com", '|', to_varchar(v."vendgroup")), cg."id")
    LEFT JOIN {{ref('FO_company') }}               company  ON EQUAL_NULL(company."company.id",nb."Com")

    LEFT JOIN (SELECT DISTINCT "street" ,"zipcode", "city", "countryregionid", "location", "recid",
                ROW_NUMBER() OVER (PARTITION BY "location" ORDER BY "validfrom" DESC) AS rn
                FROM {{ source("FOD", "LOGISTICSPOSTALADDRESS") }} 
                )                                  logis    ON EQUAL_NULL(logis."location",dp."primaryaddresslocation") and rn= 1

    LEFT JOIN {{ source("FOD", "LOGISTICSELECTRONICADDRESS") }}    loget ON EQUAL_NULL(loget."recid",dp."primarycontactphone")  and loget."isprimary"=1
    LEFT JOIN {{ source("FOD", "LOGISTICSELECTRONICADDRESS") }}    logem ON EQUAL_NULL(logem."recid",dp."primarycontactemail")  and logem."isprimary"=1 
    
    where company."company.id" = nb."Com" --v."Com" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta','UAB Marijampolės švara')
)

select 
 ifnull("id", ' ')                    as "vendor.id"
,ifnull("code", ' ')                  as "vendor.code"
,ifnull("nav_code", ' ')              as "vendor.nav_code"
,ifnull("fo_code", ' ')               as "vendor.fo_code"
,ifnull("name_original", ' ')         as "vendor.name_original"
,ifnull("name", '')                  as "vendor.name"
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
,'FO'                                 as "vendor.system" 
from vendor
