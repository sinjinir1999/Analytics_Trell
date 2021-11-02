--Group Details(public)
SELECT GroupName AS chat_group
	,lang
	,gender
	,categories
	,PUBLIC
	,totalMembers
	,messageCount
FROM `trell - bq - platform - backend.chat_service_mongo.channeldetails`
WHERE GroupName IS NOT NULL
	AND groupType = 'public'
