Select count(distinct did) group_description
from
(select
    device.advertising_id as did,
    from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a, UNNEST(event_params) b,UNNEST(event_params)c
    where event_name = 'PAGE_LANDING' and  _TABLE_SUFFIX  ='20211107' AND
    b.key = 'current_page_name' AND b.value.string_value = 'group_description' and app_info.version >= '6.1.08') as A
