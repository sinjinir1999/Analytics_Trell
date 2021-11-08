---chat group opened
select distinct device.advertising_id as did, count(distinct TIMESTAMP_MICROS(event_timestamp)) AS group_opened
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a
    where event_name = 'PAGE_LANDING' and a.key = 'current_page_name' and a.value.string_value = 'group_chat'
        and _table_suffix = '20211101' group by 1
