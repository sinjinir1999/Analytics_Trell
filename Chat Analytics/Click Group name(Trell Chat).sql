
---Click Group name(Trell Chat)
select distinct c.value.string_value as chat_group,count(distinct device.advertising_id) as click_group_name_CF
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b,
        UNNEST(event_params) c
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_group_name'
    and b.key = 'current_page_name' and b.value.string_value in ('trell_chat')
    and c.key = 'group_name'
        and  _table_suffix >='20210825'
        group by 1 order by 2 desc
