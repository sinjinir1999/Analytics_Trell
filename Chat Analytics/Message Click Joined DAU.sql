SELECT count(DISTINCT B.did) Unique_click_joined
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
	) A 
LEFT JOIN (
	
	SELECT DISTINCT device.advertising_id AS did
	FROM `trellatale.analytics_153549617.events_*`
	WHERE event_name = 'user_engagement'
		AND _table_suffix = '20211026'
	) P ON P.did = A.did
left join --Unique message click
(SELECT DISTINCT device.advertising_id AS did
FROM `trellatale.analytics_153549617.events_*`
	,UNNEST(event_params) a
	,UNNEST(event_params) b
	,UNNEST(event_params) c
WHERE event_name = 'ITEM_CLICK'
	AND a.KEY = 'item_name'
	AND a.value.string_value = 'btn_message'
	AND b.KEY = 'current_page_name'
	AND b.value.string_value = 'group_chat'
	AND c.KEY = 'group_status'
	AND c.value.int_value = 1
	AND _table_suffix = '20211026'
GROUP BY 1) B on P.did = B.did 
