-- Count of Users vs No of groups joined
select distinct groups_joined, count(distinct did) as count_of_users
from
(select distinct device.advertising_id as did , count(distinct c.value.string_value) as groups_joined
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b, UNNEST(event_params) c
        where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_join'
        and b.key = 'current_page_name' and b.value.string_value in ('trell_chat','group_chat')
    and c.key = 'group_id' 
        and _table_suffix between '20211025' and '20211101' group by 1) as a
        group by 1 order by 1

