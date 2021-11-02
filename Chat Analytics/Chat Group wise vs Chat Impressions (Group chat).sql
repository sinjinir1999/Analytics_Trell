-- Chat Group wise vs Chat Impressions (Group chat)
select distinct c.value.string_value as chat_group,count(distinct device.advertising_id) as chat_group_Impressions
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a, UNNEST(event_params) c 
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat' 
   and c.key = 'group_name' 
and  _table_suffix >='20210825'
        group by 1 order by 2 desc
