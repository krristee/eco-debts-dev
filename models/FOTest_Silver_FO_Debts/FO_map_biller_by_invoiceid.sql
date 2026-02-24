
SELECT 
    "id_by_invoiceid",
    "biller_name"
FROM (
select 
 concat(IFNULL(FO_map_NAV_BC."Com",''), '|', IFNULL(to_varchar(salestable."custaccount"),''), '|',  IFNULL(to_varchar(salestable."invoiceid_ecs"),''))          AS "id_by_invoiceid",
 dpt_resp."name"               as "biller_name",
 ROW_NUMBER() OVER (PARTITION BY  concat(IFNULL(FO_map_NAV_BC."Com",''), '|', IFNULL(to_varchar(salestable."custaccount"),''), '|',  IFNULL(to_varchar(salestable."invoiceid_ecs"),'')) ORDER BY dpt_resp."name" ASC) AS row_num
  
FROM {{source("FOD", "SALESTABLE")}}                    salestable
left join {{source("FOD", "CUSTTABLE")}}                custtable       on salestable."custaccount" = custtable."accountnum"
left join {{source("FOD", "DIRPARTYTABLE")}}            dirpartytable   on custtable."party" = dirpartytable."recid"
left join {{source("FOD", "HCMWORKER")}}                hcmworker       on salestable."workersalesresponsible" = hcmworker."recid"
left join {{source("FOD", "DIRPERSON")}}                dirperson       on hcmworker."person" = dirperson."recid"
left join {{source("FOD", "DIRPARTYTABLE")}}            dpt_resp        on dirperson."recid" = dpt_resp."recid"
left join {{ref('FO_map_NAV_BC')}}                      FO_map_NAV_BC   on FO_map_NAV_BC.DATAREAD = upper(salestable."dataareaid")
) 
WHERE row_num = 1