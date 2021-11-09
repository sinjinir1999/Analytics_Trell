Select count(distinct did) media_screen_video
from
(select
    device.advertising_id as did,
    from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a, UNNEST(event_params) b, UNNEST(event_params) c
    where event_name = 'PAGE_LANDING' and  _TABLE_SUFFIX  ='20211103' AND
    b.key = 'current_page_name' AND b.value.string_value = 'media_screen'  and  c.key = 'content_type' AND c.value.string_value = 'video') as A
