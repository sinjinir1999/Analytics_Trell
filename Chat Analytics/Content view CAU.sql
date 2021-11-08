----content view/CAU
SELECT
  device.advertising_id as did, 
  TIMESTAMP_MICROS(event_timestamp) AS event_datetime1,
   e.value.string_value as contentview_DAV,
  FROM  `trellatale.analytics_153549617.events_*`, UNNEST(event_params) e, UNNEST(event_params) f 
  WHERE _table_suffix = '20211105' and event_name = 'CONTENT_VIEW' and f.value.int_value > 0
  AND e.key = 'trail_id' AND f.key = 'watch_duration_ms' group by 1,2,3
