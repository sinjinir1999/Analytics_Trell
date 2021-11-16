
	Select d1_app ,round((d1_app*100.0/join_group),2)
    from 
    (Select count(distinct join_group_click) as join_group,count(distinct d1_app_open) as d1_app from
    (Select L.did as join_group_click,K.did as d1_app_open
    from((SELECT TRIM(JSON_EXTRACT(data, "$.user_data['aaid']"), '"') AS did
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
		AND _table_suffix = '20211106'
	) P ON P.did = A.did
left join--Unique Join Group clicks
(select distinct device.advertising_id as did
        from `trellatale.analytics_153549617.events_*`, UNNEST(event_params) a,UNNEST(event_params) b
    where event_name = 'ITEM_CLICK' and a.key = 'item_name' and a.value.string_value = 'btn_join'
    and b.key = 'current_page_name' and b.value.string_value in ('trell_chat','group_chat')
        and _table_suffix = '20211106') L on P.did = L.did
left join -- D1 app open activity-join group click %
        (select  distinct device.advertising_id  as did
            from `trellatale.analytics_153549617.events_*`
            where event_name= 'user_engagement'
            and _table_suffix = '20211107'
            )  K on K.did=L.did )))

    
