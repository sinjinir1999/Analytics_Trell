-- d0 joiners on d1 chat page
select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'trell_chat'
        and _table_suffix = '20211105'
