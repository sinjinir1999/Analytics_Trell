select installs,chat_page_imp,
      round(((case when chat_page_imp > 0 then chat_page_imp else 0 end)/installs),2),
      group_page_imp,
      group_page_imp_unjoined,
      group_page_imp_joined,
      join_group_click,
      ifnull(round((case when join_group_click>0 then join_group_click else 0 end)/nullif((case when chat_page_imp>0 then chat_page_imp else 0 end),0),2),0),
      ifnull(avg_group_joined, 0),
      join_group_click_via_chat,
      join_group_click_via_group,
       d1_app_open,
       ifnull(round((case when d1_app_open>0 then d1_app_open else 0 end)/nullif((case when join_group_click>0 then join_group_click else 0 end),0),2),0),
       d7_app_open,
       d1_group_chat_open,
    ifnull(round((case when d1_group_chat_open>0 then d1_group_chat_open else 0 end)/nullif((case when join_group_click>0 then join_group_click else 0 end),0),2),0),
    d7_group_chat_open,
    message_click,
    ifnull(round((case when message_click>0 then message_click else 0 end)/nullif((case when group_page_imp>0 then group_page_imp else 0 end),0),2),0),
    like_click,
    ifnull(round((case when like_click>0 then like_click else 0 end)/nullif((case when group_page_imp>0 then group_page_imp else 0 end),0),2),0),
    share_click,
    ifnull(round((case when share_click>0 then share_click else 0 end)/nullif((case when group_page_imp>0 then group_page_imp else 0 end),0),2),0),
    ifnull(avg_group_opened,0),
    d0_CV_per_dav,
    d1_app_open_chatpage,
    round((d1_app_open_chatpage/nullif(chat_page_imp,0))*100,2) as d1_app_open_chatpage,
    d1_chat_open_chatpage,
    round((d1_chat_open_chatpage/nullif(chat_page_imp,0))*100,2) as d1_chat_open_chatpage,
    d1_chat_open,
    round((d1_chat_open/nullif(join_group_click,0))*100,2) as d1_chat_open




       from
            (select count(distinct installs) as installs,
count(distinct chat_page_imp) as chat_page_imp,
count(distinct group_page_imp) as group_page_imp,
count(distinct group_page_imp_unjoined) as group_page_imp_unjoined,
count(distinct group_page_imp_joined) as group_page_imp_joined,
count(distinct join_group_click) as join_group_click,
count(distinct join_group_click_chat) as join_group_click_via_chat,
count(distinct join_group_click_group) as join_group_click_via_group,
count(distinct d1_app_open) as d1_app_open,
count(distinct d7_app_open) as d7_app_open,
count(distinct d1_group_chat_open) as d1_group_chat_open,
count(distinct d7_group_chat_open) as d7_group_chat_open,
count(distinct message_click) as message_click,
count(distinct like_click) as like_click,
count(distinct share_click) as share_click,
round(avg(chat_group),2) as avg_group_joined,
round(avg(group_opened),2) as avg_group_opened,
round((sum(d0_trails)/NULLIF(count(distinct CASE WHEN d0_trails > 0 THEN join_group_click ELSE NULL END),0)),2) as d0_CV_per_dav,
count(distinct d1_app_open_chatpage) as d1_app_open_chatpage,
count(distinct d1_chat_open_chatpage) as d1_chat_open_chatpage,
count(distinct d1_chat_open) as d1_chat_open


  from
(select A.did as installs,
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
K.did as d1_app_open,
M.did as d7_app_open,
N.did as d1_group_chat_open,
O.did as d7_group_chat_open,
chat_group,
P.group_opened,
Q.did as d1_app_open_chatpage,
R.did as d1_chat_open_chatpage,
S.did as d1_chat_open,
count(distinct tm.event_datetime1) as d0_trails
  from
        (select  TRIM(JSON_EXTRACT(data, "$.user_data['aaid']"),'"') as did ,
        TRIM(JSON_EXTRACT(data, "$.last_attributed_touch_data['~campaign']"),'"') as campaign,
        EXTRACT(date FROM createdAt ) as install_date, count(*)
        from `trellatale.trellDbDump.all_installs`
        where FORMAT_TIMESTAMP("%Y%m%d", createdAt)  = '{}'  
        and TRIM(JSON_EXTRACT(data, "$.last_attributed_touch_data['~advertising_partner_name']"),'"') = 'Facebook' 
        and length(JSON_EXTRACT_SCALAR(data, '$.user_data.app_version') ) <=7 
        group by 1,2,3) A -- D0 installs

left join --chat page Impressions
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'trell_chat'
        and _table_suffix = '{}') B on A.did = B.did       

left join --chat group Impressions
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
        and _table_suffix = '{}') C on A.did = C.did  

left join --chat group Impressions(Unjoined)
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,  UNNEST(event_params) b
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
    and b.key = 'group_status' and b.value.int_value = 0
        and _table_suffix = '{}') D on A.did = D.did

left join --chat group Impressions(joined)
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,  UNNEST(event_params) b
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
    and b.key = 'group_status' and b.value.int_value = 1
        and _table_suffix = '{}') E on A.did = E.did

left join--Unique Join Group clicks
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_join'
    and b.key = 'current_page_name' and b.value.string_value in ('trell_chat','group_chat')
        and _table_suffix = '{}') L on A.did = L.did

left join--Unique Join Group clicks
(select distinct device.advertising_id as did,count(distinct c.value.string_value) as chat_group
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b,
        UNNEST(event_params) c
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_join'
    and b.key = 'current_page_name' and b.value.string_value in ('trell_chat')
    and c.key = 'group_id' 
        and _table_suffix = '{}' group by 1) F on A.did = F.did
left join--Unique Group page clicks
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_join'
    and b.key = 'current_page_name' and b.value.string_value in ('group_chat')
        and _table_suffix = '{}') G on A.did = G.did

left join --Unique message click
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_message'
    and b.key = 'current_page_name' and b.value.string_value = 'group_chat'
        and _table_suffix = '{}'
        group by 1) H on H.did = A.did 

left join --Unique like click
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_like'
    and b.key = 'current_page_name' and b.value.string_value = 'group_chat'
        and _table_suffix = '{}'
        group by 1) I on I.did = A.did     

left join --Unique share click
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_share_initiate'
    and b.key = 'current_page_name' and b.value.string_value = 'group_chat'
        and _table_suffix = '{}'
        group by 1) J on J.did = A.did
left join -- D1 app open activity-join group click %
        (select  distinct device.advertising_id  as did
            from `trellatale.analytics_153549617.events_*`
            where event_name= 'user_engagement'
            and _table_suffix = '{}'
            )  K on K.did=L.did     
left join -- D1 app open activity-chat page imp %
        (select  distinct device.advertising_id  as did
            from `trellatale.analytics_153549617.events_*`
            where event_name= 'user_engagement'
            and _table_suffix = '{}'
            )  Q on Q.did=B.did                 
left join -- D7 app open activity %
        (select  distinct device.advertising_id  as did
            from `trellatale.analytics_153549617.events_*`
            where event_name= 'user_engagement'
            and _table_suffix = '{}'
            )  M on M.did=L.did  
left join --d1 chat group
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
        and _table_suffix = '{}') N on N.did = L.did 
left join --d1 chat page
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'trell_chat'
        and _table_suffix = '{}') R on R.did = B.did        
left join --d7 chat group
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
        and _table_suffix = '{}') O on O.did = L.did     
left join --chat group opened
(select distinct device.advertising_id as did, count(distinct TIMESTAMP_MICROS(event_timestamp)) AS group_opened
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
        and _table_suffix = '{}' group by 1) P on A.did = P.did  

LEFT JOIN --content view/CAU
  (SELECT
  device.advertising_id as did, 
  TIMESTAMP_MICROS(event_timestamp) AS event_datetime1,
   e.value.string_value as contentview_DAV,
  FROM  `trellatale.analytics_153549617.events_*`, UNNEST(event_params) e, UNNEST(event_params) f 
  WHERE _table_suffix = '{}' and event_name = 'CONTENT_VIEW' and f.value.int_value > 0
  AND e.key = 'trail_id' AND f.key = 'watch_duration_ms' group by 1,2,3) 
  as tm on F.did = tm.did 

left join -- d0 joiners on d1 chat page
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'trell_chat'
        and _table_suffix = '{}') S on S.did = L.did
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20                   
        ))
