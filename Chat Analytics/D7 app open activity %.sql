---- D7 app open activity %

select  distinct device.advertising_id  as did
            from `trellatale.analytics_153549617.events_*`
            where event_name= 'user_engagement'
            and _table_suffix = '20211105'
