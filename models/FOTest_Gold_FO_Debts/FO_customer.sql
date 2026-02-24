WITH 

customer AS (
SELECT
    IFNULL(nb."Com",'') || '|' || IFNULL(c."accountnum",'') AS "id",    -- KRISTINA : buvo  dp."partynumber" --> c."accountnum"   
    TO_VARCHAR(coalesce(c."accountnum",nc."No_"))   AS "code",          -- KRISTINA : buvo  dp."partynumber" --> c."accountnum"   
    TO_VARCHAR(nc."No_")                            as "nav_code",
    TO_VARCHAR(c."accountnum")                      as "fo_code",       -- KRISTINA : buvo  dp."partynumber" --> c."accountnum"   
    dp."name" 							            AS "name_original",
    cvn."Name"                                      AS "name",
    logis."street"                                  AS "address",
    logis."zipcode"					                AS "post_code",
    logis."city"							        AS "city",
    logis."countryregionid"				            AS "country_code",  
    TO_VARCHAR(trim(c."organizationcode_ecs"))	    AS "registration_no",
    TO_VARCHAR(c."vatnum")                          AS "vat_no",
    TO_VARCHAR(c."paymtermid")	                    AS "payment_terms_code",
    ''			                                    AS "salesperson_code",
    coalesce(st."Status actual", 'Other')           AS "status",
    coalesce(sh."Status Historical", 'Other')       AS "status_hist",
    dto."Debt owner"                                AS "debt_owner",
    c."custgroup"		                            AS "posting_group",
    ''		                                        AS "gen_bus_posting_group",
    ''		                                        AS "vat_bus_posting_group",
    ''						                        AS "contact_original",
    IFNULL(TO_VARCHAR(loget."locator"),' ') || 
    CASE 
        WHEN LENGTH(TO_VARCHAR(loget."locator")) > 0 AND LENGTH(logem."locator") > 0 
        THEN ';' 
        ELSE '' 
    END || 
    IFNULL(logem."locator",' ')                     AS "contact",
    loget."locator"				                    AS "phone_no",
    logem."locator"					                AS "email",
    Date(ifnull(c."modifiedon",'2000-01-01'))	    AS "modified_at",
    coalesce(cg."Consolidation Group", 'Not Group') AS "consolidation_group",
    cvt."Customer / Vendor Type"                    AS "type",
    nb."Com" 				                        AS "Com"
    FROM {{ source("FOD", "CUSTTABLE") }}               c
    left join {{ref('FO_map_NAV_BC') }}                 nb  on nb.DATAREAD = upper(c."dataareaid")
    left join {{ source("FOD", "DIRPARTYTABLE") }}      dp  on c."party" = dp."recid"
    LEFT JOIN {{ref('FO_map_cust_vend_name') }}         cvn on EQUAL_NULL(concat(nb."Com", '|1|', TO_VARCHAR(c."accountnum")) , cvn."id")
    LEFT JOIN {{ref('FO_map_status') }}                 st  on EQUAL_NULL(c."credmanaccountstatusid" , st."Salesperson code")
    LEFT JOIN {{ref('FO_map_status_hist') }}            sh  on EQUAL_NULL(c."credmanaccountstatusid" , sh."Salesperson code")
    LEFT JOIN {{ref('FO_map_debt_owner') }}             dto on EQUAL_NULL(c."credmanaccountstatusid", dto."Salesperson code")
    
    LEFT JOIN {{ref('FO_map_cust_vend_type') }}         cvt on EQUAL_NULL(c."custgroup" , cvt."Posting group")
    left join {{ source("NAV", "Customer") }}           nc  on c."accountnum" = nc."Original Customer No_" and nb."Com" = nc."Com"
    --LEFT JOIN 'FO_map_consolidation_group'    cg  on EQUAL_NULL(concat(nb.DATAREAD, '|', TO_VARCHAR(nc."No_")) , cg."id")
    LEFT JOIN {{ref('FO_map_consolidation_group_Finance') }}    cg  on EQUAL_NULL(concat(nb."Com", '|', to_varchar(c."custgroup")), cg."id")
    LEFT JOIN {{ref('FO_company') }}                company ON EQUAL_NULL(company."company.id",nb."Com")

    LEFT JOIN (SELECT DISTINCT "street" ,"zipcode", "city", "countryregionid", "location", "recid",
                ROW_NUMBER() OVER (PARTITION BY "location" ORDER BY "validfrom" DESC) AS rn
                FROM {{ source("FOD", "LOGISTICSPOSTALADDRESS") }} 
                )                                       logis ON EQUAL_NULL(logis."location",dp."primaryaddresslocation") and rn= 1

    LEFT JOIN {{ source("FOD", "LOGISTICSELECTRONICADDRESS") }}    loget ON EQUAL_NULL(loget."recid",dp."primarycontactphone")  and loget."isprimary"=1
    LEFT JOIN {{ source("FOD", "LOGISTICSELECTRONICADDRESS") }}    logem ON EQUAL_NULL(logem."recid",dp."primarycontactemail")  and logem."isprimary"=1 
    
    where company."company.id" = nb."Com" --c."Com" in ('UAB Ecoservice','AB Specializuotas transportas','UAB Biržų komunalinis ūkis','UAB Ecoplasta','UAB Marijampolės švara')
)

SELECT 
 ifnull("id", ' ')                       AS "customer.id"
,ifnull("code", ' ')                     AS "customer.code"
,ifnull("nav_code", ' ')                 as "customer.nav_code"
,ifnull("fo_code", ' ')                  as "customer.fo_code"
,ifnull("name_original", ' ')            AS "customer.name_original"
,ifnull("name", '')                     AS "customer.name"
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
,'FO'                                    as "customer.system" 
FROM customer