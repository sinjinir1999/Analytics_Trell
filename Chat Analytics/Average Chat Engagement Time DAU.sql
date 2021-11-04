select round(avg(diff),2) as chat_engagement_time from(select did, sum(diff) as diff 
from
(select distinct A.did,landing,leaving,datetime_diff(leaving,landing,second) as diff  from
 (select distinct *,TIMESTAMP_MICROS(event_timestamp) landing,ROW_NUMBER ( )   
    OVER ( PARTITION BY did order by event_timestamp ) rnk from 
    (select distinct
    device.advertising_id as did, event_timestamp,c.value.int_value as session_id
    from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a, UNNEST(event_params) b,UNNEST(event_params) c
    where event_name = 'PAGE_LANDING' and  _TABLE_SUFFIX  = '20211103' AND
    b.key = 'current_page_name' AND b.value.string_value = 'trell_chat' and c.key = 'ga_session_id'
 ) )A
 left join 
(select distinct *,TIMESTAMP_MICROS(event_timestamp) leaving,ROW_NUMBER ( )   
    OVER ( PARTITION BY did order by event_timestamp ) rnk from (select distinct
    device.advertising_id as did, event_timestamp,c.value.int_value as session_id
    from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a, UNNEST(event_params) b,UNNEST(event_params) c
    where event_name = 'PAGE_LEAVING' and  _TABLE_SUFFIX  =  '20211103' AND
    b.key = 'leaving_page_name' AND b.value.string_value = 'trell_chat'and c.key = 'ga_session_id'
    ) ) B ON A.did = B.did and A.session_id = B.session_id 
    where A.rnk = B.rnk and leaving > landing 
    and A.did in 
(select distinct did from
(select A.did, count(distinct A.event_timestamp ) as page_landing,count(distinct B.event_timestamp ) as page_leaving
from
(select
    device.advertising_id as did, event_timestamp
    from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a, UNNEST(event_params) b
    where event_name = 'PAGE_LANDING' and  _TABLE_SUFFIX  = '20211103' AND
    b.key = 'current_page_name' AND b.value.string_value = 'trell_chat')A 
left join
(select
    device.advertising_id as did, event_timestamp
    from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a, UNNEST(event_params) b
    where event_name = 'PAGE_LEAVING' and  _TABLE_SUFFIX  ='20211103' AND
    b.key = 'leaving_page_name' AND b.value.string_value = 'trell_chat')B on A.did = B.did     
group by 1    
having page_landing = page_leaving)))
    group by 1) 
