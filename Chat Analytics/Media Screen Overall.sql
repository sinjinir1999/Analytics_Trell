ELECT count(DISTINCT B.did) media_screen_imp
FROM (
	SELECT TRIM(JSON_EXTRACT(data, "$.user_data['aaid']"), '"') AS did
		,TRIM(JSON_EXTRACT(data, "$.last_attributed_touch_data['~campaign']"), '"') AS campaign
		,EXTRACT(DATE FROM createdAt) AS install_date
		,count(*)
	FROM `trellatale.trellDbDump.all_installs`
	WHERE TRIM(JSON_EXTRACT(data, "$.last_attributed_touch_data['~advertising_partner_name']"), '"') = 'Facebook'
		AND length(JSON_EXTRACT_SCALAR(data, '$.user_data.app_version')) <= 7
		AND JSON_EXTRACT_SCALAR(data, '$.user_data.app_version') >= '6.1.08'
	GROUP BY 1
		,2
		,3
	) A -- D0 installs
LEFT JOIN (
	
	SELECT DISTINCT device.advertising_id AS did
	FROM `trellatale.analytics_153549617.events_*`
	WHERE event_name = 'user_engagement'
		AND _table_suffix = '{}'
	) P ON P.did = A.did
LEFT JOIN (
	SELECT device.advertising_id AS did
		,
	FROM `trellatale.analytics_153549617.events_*`
		,UNNEST(event_params) a
		,UNNEST(event_params) b
	WHERE event_name = 'PAGE_LANDING'
		AND _TABLE_SUFFIX = '{}'
		AND b.KEY = 'current_page_name'
		AND b.value.string_value = 'media_screen'
	) AS B ON P.did = B.did
