	SELECT device.advertising_id AS did
							,TIMESTAMP_MICROS(event_timestamp),b.value.string_value
						FROM `trellatale.analytics_153549617.events_*`
							,UNNEST(event_params) a
							,UNNEST(event_params) b
						WHERE event_name = 'PAGE_LANDING'
							AND _TABLE_SUFFIX = '20211101' 	AND b.KEY = 'current_page_name' and b.value.string_value in('trell_chat','group_chat','group_description','media_screen','private_group_preview_screen')
							and device.advertising_id is not null
