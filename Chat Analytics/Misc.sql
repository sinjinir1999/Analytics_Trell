select count( a.devices) as App_Open,
count(distinct case when b.devices is not null then a.devices end) as Create_channel,
count(distinct case when c.devices is not null then a.devices end) as Select_group_type,
count(distinct case when d.devices is not null then a.devices end) as Group_details,
count(distinct case when e.devices is not null then a.devices end) as Add_friends,
count(distinct case when f.devices is not null then a.devices end) as Atleast_one_message,
from
(select distinct device.advertising_id as devices from `trellatale.analytics_153549617.events_*`, 
UNNEST (event_params) e, UNNEST (event_params) f
where event_name = 'APP_OPEN'
and _TABLE_SUFFIX = '20210928') a
left join
(select * from
(select distinct device.advertising_id as devices, f.value.string_value as ad_type from `trellatale.analytics_153549617.events_*`, 
UNNEST (event_params) e, UNNEST (event_params) f
where event_name = 'ITEM_CLICK'
and _TABLE_SUFFIX = '20210928'
and e.key = 'current_page_name' and e.value.string_value = 'trell_chat'
and f.key = 'item_name' and f.value.string_value = 'btn_create')) b
on a.devices = b.devices
left join
(select distinct device.advertising_id as devices, f.value.string_value as selected_lang from `trellatale.analytics_153549617.events_*`,
UNNEST (event_params) e, UNNEST (event_params) f, UNNEST (event_params) h
where _TABLE_SUFFIX = '20210928'
and event_name = 'ITEM_CLICK'
and f.key = 'group_type' and f.value.string_value = 'public'
and h.key = 'current_page_name' and h.value.string_value = 'trell_chat') c
on a.devices = c.devices
left join
(select distinct device.advertising_id as devices from `trellatale.analytics_153549617.events_*`, 
UNNEST (event_params) e, UNNEST (event_params) f, UNNEST (event_params) g,  UNNEST (event_params) h, UNNEST (event_params) i, UNNEST (event_params) k
where event_name = 'ITEM_CLICK'
and _TABLE_SUFFIX = '20210928'
and e.key = 'item_name' and e.value.string_value = 'btn_next_group_details'
and f.key = 'current_page_name' and f.value.string_value = 'input_group_details') d
on a.devices = d.devices
left join
(select distinct device.advertising_id as devices from `trellatale.analytics_153549617.events_*`, 
UNNEST (event_params) e, UNNEST (event_params) f
where event_name = 'ITEM_CLICK'
and _TABLE_SUFFIX = '20210928'
and e.key = 'item_name' and e.value.string_value = 'btn_add_friends'
and f.key = 'current_page_name' and f.value.string_value = 'group_chat') e
on a.devices = e.devices
left join
(select distinct device.advertising_id as devices from `trellatale.analytics_153549617.events_*`, 
UNNEST (event_params) e, UNNEST (event_params) f
where event_name = 'ITEM_CLICK'
and _TABLE_SUFFIX = '20210928'
and e.key = 'item_name' and e.value.string_value = 'btn_message_post'
and f.key = 'current_page_name' and f.value.string_value = 'group_chat') f
on a.devices = f.devices
