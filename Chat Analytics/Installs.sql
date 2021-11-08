Select distinct JSON_EXTRACT_SCALAR(data, '$.user_data.aaid') as did, DATE(createdAt) as date_
from trellatale.trellDbDump.all_installs
where length(JSON_EXTRACT_SCALAR(data, '$.user_data.app_version'))<=7  
and JSON_EXTRACT_SCALAR(data, '$.user_data.app_version') >= '6.1.08'
and TRIM(JSON_EXTRACT(data, "$.last_attributed_touch_data['~advertising_partner_name']"),'"') in ('Google AdWords', 'Facebook') 
and DATE(createdAt) between '2021-10-01' and '2021-10-15'


--FB Filter
select  TRIM(JSON_EXTRACT(data, "$.user_data['aaid']"),'"') as did ,
        TRIM(JSON_EXTRACT(data, "$.last_attributed_touch_data['~campaign']"),'"') as campaign,
        EXTRACT(date FROM createdAt ) as install_date, count(*)
        from `trellatale.trellDbDump.all_installs`
        where FORMAT_TIMESTAMP("%Y%m%d", createdAt)  = '{}'  
        and TRIM(JSON_EXTRACT(data, "$.last_attributed_touch_data['~advertising_partner_name']"),'"') = 'Facebook' 
        and length(JSON_EXTRACT_SCALAR(data, '$.user_data.app_version') ) <=7 
        group by 1,2,3
