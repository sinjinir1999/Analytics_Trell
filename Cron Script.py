#!/usr/bin/env python
# coding: utf-8

# In[36]:


import pandas as pd
# import pandas_gbq as gbq
import pygsheets as pg
# from clickhouse_driver import Client
# import mysql.connector
# import json
# import gspread
# from gspread_pandas import Spread
# import os
# from google.cloud import bigquery
# from google.oauth2.service_account import Credentials
from trellDB2df import trellDBconnect
from datetime import datetime, timedelta
from dateutil.relativedelta import *


# In[37]:



spreadsheetId = '1ACDp928tPmtbgC04mxIi8mwBbmkMJOaxa6nSAQgFWs8'
spreadsheetName = 'Trell Chats MIS'
client_secret = 'service_account.json'
wks = 'DAU'
gc = pg.authorize(service_file=client_secret)
credz = pg.Spreadsheet(gc, id=spreadsheetId)
wks1 = credz.worksheet_by_title(wks)
print(wks1, 'Authorized')


# In[38]:


##DAU
# from_date = '2021-08-20'
# to_date = '2021-12-17'
from_date = datetime.strftime(datetime.now() - timedelta(40), '%Y-%m-%d')
to_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
toDate = to_date.replace("-", "")
fromDate = from_date.replace("-", "")


# In[39]:


DF = pd.DataFrame()
r = pd.date_range(from_date, to_date)
dateList = r.format(formatter=lambda x: x.strftime('%Y-%m-%d'))


# In[40]:


for i in dateList:
    print(i)
    to_date = i

    toDate = to_date.replace("-", "")
    datetime_object = datetime.strptime(toDate, '%Y%m%d')
    
    date_format = "%Y-%m-%d"
    a = datetime.strptime('2021-08-20', date_format)
    b = datetime.strptime(to_date, date_format)
    n1 = str(((b - a).days) + 3)
    print(n1)
    
    query = """SELECT round(avg(diff), 2) AS chat_engagement_time
FROM (
	SELECT did
		,sum(diff) AS diff
	FROM (
		SELECT DISTINCT A.did
			,landing
			,leaving
			,datetime_diff(leaving, landing, second) AS diff
		FROM (
			SELECT DISTINCT *
				,TIMESTAMP_MICROS(event_timestamp) landing
				,ROW_NUMBER() OVER (
					PARTITION BY did ORDER BY event_timestamp
					) rnk
			FROM (
				SELECT DISTINCT device.advertising_id AS did
					,event_timestamp
					,c.value.int_value AS session_id
				FROM `trellatale.analytics_153549617.events_*`
					,UNNEST(event_params) a
					,UNNEST(event_params) b
					,UNNEST(event_params) c
				WHERE event_name = 'PAGE_LANDING'
					AND _TABLE_SUFFIX ='{}'
					AND b.KEY = 'current_page_name'
					AND b.value.string_value = 'trell_chat'
					AND c.KEY = 'ga_session_id'
				)
			) A
		LEFT JOIN (
			SELECT DISTINCT *
				,TIMESTAMP_MICROS(event_timestamp) leaving
				,ROW_NUMBER() OVER (
					PARTITION BY did ORDER BY event_timestamp
					) rnk
			FROM (
				SELECT DISTINCT device.advertising_id AS did
					,event_timestamp
					,c.value.int_value AS session_id
				FROM `trellatale.analytics_153549617.events_*`
					,UNNEST(event_params) a
					,UNNEST(event_params) b
					,UNNEST(event_params) c
				WHERE event_name = 'PAGE_LEAVING'
					AND _TABLE_SUFFIX = '{}'
					AND b.KEY = 'leaving_page_name'
					AND b.value.string_value = 'trell_chat'
					AND c.KEY = 'ga_session_id'
				)
			) B ON A.did = B.did
			AND A.session_id = B.session_id
		WHERE A.rnk = B.rnk
			AND leaving > landing
			AND A.did IN (
				SELECT DISTINCT did
				FROM (
					SELECT A.did
						,count(DISTINCT A.event_timestamp) AS page_landing
						,count(DISTINCT B.event_timestamp) AS page_leaving
					FROM (
						SELECT device.advertising_id AS did
							,event_timestamp
						FROM `trellatale.analytics_153549617.events_*`
							,UNNEST(event_params) a
							,UNNEST(event_params) b
						WHERE event_name = 'PAGE_LANDING'
							AND _TABLE_SUFFIX = '{}'
							AND b.KEY = 'current_page_name'
							AND b.value.string_value = 'trell_chat'
						) A
					LEFT JOIN (
						SELECT device.advertising_id AS did
							,event_timestamp
						FROM `trellatale.analytics_153549617.events_*`
							,UNNEST(event_params) a
							,UNNEST(event_params) b
						WHERE event_name = 'PAGE_LEAVING'
							AND _TABLE_SUFFIX = '{}'
							AND b.KEY = 'leaving_page_name'
							AND b.value.string_value = 'trell_chat'
						) B ON A.did = B.did
					GROUP BY 1
					HAVING page_landing = page_leaving
					)
				)
		)
	GROUP BY 1
	)""".format(
        toDate,
        toDate,
        toDate,
        toDate,

    )
    df = trellDBconnect(query).callbigQuery()
    df = df.fillna(0)
    index = 'AE' + str(n1)
    wks1.set_dataframe(df, index, copy_head=False)

    


