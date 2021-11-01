--- Date and User wise Number of Groups Joined
select distinct device.advertising_id as did, DATE(TIMESTAMP_MICROS(cast(`event_timestamp` as int64)), "Asia/Kolkata") as date_ , count(distinct c.value.string_value) as groups_joined
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b, UNNEST(event_params) c
        where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_join'
        and b.key = 'current_page_name' and b.value.string_value in ('trell_chat','group_chat')
    and c.key = 'group_id' 
        and _table_suffix between '20211001' and '20211015' group by 1,2
