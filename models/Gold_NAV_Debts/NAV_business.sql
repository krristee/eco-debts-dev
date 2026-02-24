/* business:
      LOAD
          [Project code] & '|' & [Department code] &'|'& FLOOR([VALID_FROM (YYYY-MM-DD)]) &'|'& FLOOR([VALID_TO (YYYY-MM-DD)]) 
          																		AS id,
          [Project code] & '|' & [Department code] 								AS tmp_id,                                                                                
          [Business code] 														AS code,
          APPLYMAP('map_business_name',[Business code],'$(vL.unknown_value)') 	AS name,
          [Business code] & ' ' & APPLYMAP('map_business_name',[Business code],'$(vL.unknown_value)') 
          																		AS code_name,
     	  [VALID_FROM (YYYY-MM-DD)]												AS valid_from,
          [VALID_TO (YYYY-MM-DD)]												AS valid_to,
          APPLYMAP('map_sub_business_code',[Project code],'$(vL.unknown_value)') AS sub_code,
          APPLYMAP('map_sub_business_name',APPLYMAP('map_sub_business_code',[Project code],'$(vL.unknown_value)'),'$(vL.unknown_value)') 
          																		AS sub_name,
          APPLYMAP('map_sub_business_code',[Project code],'$(vL.unknown_value)') & ' ' &
         	 APPLYMAP('map_sub_business_name',APPLYMAP('map_sub_business_code',[Project code],'$(vL.unknown_value)'),'$(vL.unknown_value)') 
             																	AS sub_code_name
      FROM [$(vG.ExtractFlatFilesQVDPath)1.Common\business_code.qvd] (qvd); */

WITH business as (
SELECT
    IFNULL(bc."Project code",'') || '|' || IFNULL(bc."Department code",'') ||'|'|| DATE("VALID_FROM (YYYY-MM-DD)") ||'|'|| DATE("VALID_TO (YYYY-MM-DD)") 
                                                                                            AS "id",  
    bc."Project code"|| '|' || "Department code" 								            AS "tmp_id",
    bc."Business code"                                                                      AS "code",
    IFNULL(map_business_name."Business name", ' ')                                          AS "name",
    bc."Business code" || ' ' || IFNULL(map_business_name."Business name", ' ')             AS "code_name",
    "VALID_FROM (YYYY-MM-DD)"												                AS "valid_from",
    "VALID_TO (YYYY-MM-DD)"											                        AS "valid_to",
    IFNULL(map_sub_business_code."Sub business code", ' ')                                  AS "sub_code",
    IFNULL(map_sub_business_name."Sub business name", ' ')                                  AS "sub_name",
    IFNULL(map_sub_business_code."Sub business code", ' ') || ' ' || IFNULL(map_sub_business_name."Sub business name",' ') 
                                                                                            AS "sub_code_name"
FROM {{ source("FF", "BUSINESS_CODE") }}        as bc
LEFT JOIN {{ref ('map_business_name') }}        as map_business_name        on EQUAL_NULL(bc."Business code", map_business_name."Business code")
LEFT JOIN {{ref ('map_sub_business_code') }}    as map_sub_business_code    on EQUAL_NULL(bc."Project code", map_sub_business_code."Project code")
LEFT JOIN {{ref ('map_sub_business_name') }}    as map_sub_business_name    on EQUAL_NULL(map_sub_business_code."Sub business code", map_sub_business_name."Sub business code")
)

SELECT 
 ifnull("id", ' ')                           AS "business.id"
,ifnull("tmp_id", ' ')                       AS "business.tmp_id"
,ifnull("code", ' ')                         AS "business.code"
,ifnull("name", ' ')                         AS "business.name"
,ifnull("code_name", ' ')                    AS "business.code_name"
,"valid_from"                               AS "business.valid_from"
,"valid_to"                                 AS "business.valid_to"
,ifnull("sub_code", ' ')                     AS "business.sub_code"
,ifnull("sub_name", ' ')                     AS "business.sub_name"
,ifnull("sub_code_name", ' ')                AS "business.sub_code_name"
,'NAV'                                       AS "business.system"
from business