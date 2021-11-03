select groups_joined, count(distinct did), count (distinct devices) from
(select distinct device.advertising_id as did, count(distinct c.value.string_value) as groups_joined
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b, UNNEST(event_params) c
        where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_join'
    and b.key = 'current_page_name' and b.value.string_value in ('trell_chat','group_chat')
    and c.key = 'group_id' 
        and _table_suffix = '20211101'
         and device.advertising_id in 
(select distinct JSON_EXTRACT_SCALAR(data, '$.user_data.aaid') as devices
from trellatale.trellDbDump.all_installs
where length(JSON_EXTRACT_SCALAR(data, '$.user_data.app_version'))<=7  
and JSON_EXTRACT_SCALAR(data, '$.user_data.app_version') >= '6.1.08'
and TRIM(JSON_EXTRACT(data, "$.last_attributed_touch_data['~advertising_partner_name']"),'"') in ('Google AdWords', 'Facebook') 
and DATE(createdAt) = '2021-11-01')group by 1)  a
left join 
(
                    select distinct device.advertising_id as devices
                    FROM `trellatale.analytics_153549617.events_*`, unnest(event_params) e ,UNNEST(event_params) b, UNNEST(event_params) c
                    where event_name = 'PAGE_LANDING' and e.key = 'current_page_name' and e.value.string_value = 'group_chat'
                    and b.key = 'previous_page_name' and b.value.string_value in ('trell_chat','group_chat')
                    and c.key = 'group_id' 
                    and _table_suffix in ('20211102')               
                    ) b 
        on a.did = b.devices
        group by 1
        order by 1
