
            select dau, 
            chat_page_imp, 
            round(((case when chat_page_imp > 0 then chat_page_imp else 0 end)/dau),2), 
            group_page_imp, 
            group_page_imp_unjoined,
       group_page_imp_joined, 
       join_group_click,
       ifnull(round((case when join_group_click > 0 then join_group_click else 0 end)/nullif((case when chat_page_imp > 0 then chat_page_imp else 0 end), 0), 2),0),
       ifnull(avg_group_joined, 0), 
       join_group_click_via_chat, 
       join_group_click_via_group,
        message_click,
       ifnull(round((case when message_click > 0 then message_click else 0 end)/nullif((case when group_page_imp > 0 then group_page_imp else 0 end), 0), 2),0),
       like_click,
       ifnull(round((case when like_click > 0 then like_click else 0 end)/nullif((case when group_page_imp > 0 then group_page_imp else 0 end), 0), 2),0),
       share_click,
       ifnull(round((case when share_click > 0 then share_click else 0 end)/nullif((case when group_page_imp > 0 then group_page_imp else 0 end), 0), 2),0),
       ifnull(avg_group_opened, 0),
       mute_click,leave_click,timespent
from 
(select count(distinct dau) as dau,
count(distinct chat_page_imp) as chat_page_imp,
count(distinct group_page_imp) as group_page_imp,
count(distinct group_page_imp_unjoined) as group_page_imp_unjoined,
count(distinct group_page_imp_joined) as group_page_imp_joined,
count(distinct join_group_click) as join_group_click,
count(distinct join_group_click_chat) as join_group_click_via_chat,
count(distinct join_group_click_group) as join_group_click_via_group,
count(distinct message_click) as message_click,
count(distinct like_click) as like_click,
count(distinct share_click) as share_click,
round(avg(chat_group),2) as avg_group_joined,
round(avg(group_opened),2) as avg_group_opened,
count(distinct mute_click) as mute_click,
count(distinct leave_click) as leave_click,
round(avg(timespent),2) as timespent
  from
(select P.did as dau,
B.did as chat_page_imp ,
C.did as group_page_imp,
D.did as group_page_imp_unjoined,
E.did as group_page_imp_joined,
F.did as join_group_click_chat,
G.did as join_group_click_group,
H.did as message_click,
I.did as like_click,
J.did as share_click,
L.did as join_group_click,
chat_group,
K.group_opened,
M.did as mute_click,
N.did as leave_click,
datetime_diff(event_datetime2, event_datetime1 ,second) as timespent

  from
        (select  TRIM(JSON_EXTRACT(data, "$.user_data['aaid']"),'"') as did ,
        TRIM(JSON_EXTRACT(data, "$.last_attributed_touch_data['~campaign']"),'"') as campaign,
        EXTRACT(date FROM createdAt ) as install_date, count(*)
        from `trellatale.trellDbDump.all_installs`
        where TRIM(JSON_EXTRACT(data, "$.last_attributed_touch_data['~advertising_partner_name']"),'"') = 'Facebook' 
        and length(JSON_EXTRACT_SCALAR(data, '$.user_data.app_version') ) <=7 
        and JSON_EXTRACT_SCALAR(data, '$.user_data.app_version') >= '6.1.08'
        group by 1,2,3) A -- D0 installs
left join 
        (select  distinct device.advertising_id  as did
            from `trellatale.analytics_153549617.events_*`
            where event_name= 'user_engagement'
            and _table_suffix = '{}'
            )  P on P.did=A.did 

left join --chat page Impressions
(select distinct device.advertising_id as did,min(TIMESTAMP_MICROS(event_timestamp)) AS event_datetime1
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'trell_chat'
        and _table_suffix = '{}' group by 1) B on P.did = B.did       

left join --chat group Impressions
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
        and _table_suffix = '{}') C on P.did = C.did  

left join --chat group Impressions(Unjoined)
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,  UNNEST(event_params) b
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
    and b.key = 'group_status' and b.value.int_value = 0
        and _table_suffix = '{}') D on P.did = D.did

left join --chat group Impressions(joined)
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,  UNNEST(event_params) b
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
    and b.key = 'group_status' and b.value.int_value = 1
        and _table_suffix = '{}') E on P.did = E.did

left join--Unique Join Group clicks
(select distinct device.advertising_id as did,count(distinct c.value.string_value) as chat_group
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b,
        UNNEST(event_params) c
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_join'
    and b.key = 'current_page_name' and b.value.string_value in ('trell_chat','group_chat')
    and c.key = 'group_id' 
        and _table_suffix = '{}' group by 1) L on P.did = L.did

left join--Unique Join Group clicks
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_join'
    and b.key = 'current_page_name' and b.value.string_value in ('trell_chat')
        and _table_suffix = '{}') F on P.did = F.did
left join--Unique Group page clicks
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_join'
    and b.key = 'current_page_name' and b.value.string_value in ('group_chat')
        and _table_suffix = '{}') G on P.did = G.did

left join --Unique message click
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_message'
    and b.key = 'current_page_name' and b.value.string_value = 'group_chat'
        and _table_suffix = '{}'
        group by 1) H on H.did = P.did 

left join --Unique like click
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_like'
    and b.key = 'current_page_name' and b.value.string_value = 'group_chat'
        and _table_suffix = '{}'
        group by 1) I on I.did = P.did     

left join --Unique share click
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_share_initiate'
    and b.key = 'current_page_name' and b.value.string_value = 'group_chat'
        and _table_suffix = '{}'
        group by 1) J on J.did = P.did
 left join --chat group opened
(select distinct device.advertising_id as did, count(distinct TIMESTAMP_MICROS(event_timestamp)) AS group_opened
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
        and _table_suffix = '{}' group by 1) K on P.did = K.did  
 left join --Unique mute click
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_mute_notifications'
    and b.key = 'current_page_name' and b.value.string_value = 'group_chat'
        and _table_suffix = '{}'
        group by 1) M on M.did = P.did         
 left join --Unique leave click
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_leave_group_confirm'
    and b.key = 'current_page_name' and b.value.string_value = 'group_chat'
        and _table_suffix = '{}'
        group by 1) N on N.did = P.did 
 left join --Away from chat screen
(select distinct device.advertising_id as did,min(TIMESTAMP_MICROS(event_timestamp)) AS event_datetime2
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LEAVING' and a.key = 'leaving_page_name' 
        and _table_suffix = '{}' group by 1 order by 1
) O on O.did = P.did      
        ))

