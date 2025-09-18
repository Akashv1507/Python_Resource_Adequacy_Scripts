outageFetchSql = '''SELECT
            
            outages.elementname,
            ssm.FULL_NAME AS state_name,
            gs.GENERATING_STATION_NAME AS station_name,
            gst.NAME station_type,
            gu.UNIT_NUMBER,
            gu.INSTALLED_CAPACITY, 
            CM.CLASSIFICATION, 
            to_char(outages.outage_date, 'YYYY-MM-DD') || ' '|| outages.outage_time AS out_date_time,
            to_char(outages.revived_date, 'YYYY-MM-DD') || ' '|| outages.revived_time AS revived_date_time,
            to_char(outages.expected_date, 'YYYY-MM-DD') || ' '|| outages.expected_time AS expected_date_time,
            sd_type.name    AS shut_down_type_name,
            sd_tag.name     AS shutdown_tag
            FROM
            reporting_web_ui_uat.real_time_outage     outages
            LEFT JOIN reporting_web_ui_uat.entity_master        ent_master ON ent_master.id = outages.entity_id
            LEFT JOIN reporting_web_ui_uat.shutdown_outage_tag  sd_tag ON sd_tag.id = outages.shutdown_tag_id
            LEFT JOIN reporting_web_ui_uat.shutdown_outage_type sd_type ON sd_type.id = outages.shut_down_type
            LEFT JOIN reporting_web_ui_uat.GENERATING_UNIT gu ON gu.ID = outages.ELEMENT_ID 
            LEFT JOIN reporting_web_ui_uat.GENERATING_STATION gs ON gs.ID = gu.FK_GENERATING_STATION 
            LEFT JOIN reporting_web_ui_uat.CLASSIFICATION_MASTER cm ON cm.ID = gs.CLASSIFICATION_ID 
            LEFT JOIN REPORTING_WEB_UI_UAT.SRLDC_STATE_MASTER SSM on SSM.id = GS.LOCATION_ID
            LEFT JOIN reporting_web_ui_uat.GENERATING_STATION_TYPE gst ON gst.ID = gs.station_type
            WHERE ent_master.entity_name='GENERATING_UNIT' 
                AND
                    (TO_CHAR(outages.outage_date, 'YYYY-MM-DD') || ' ' || outages.OUTAGE_TIME) <= :targetDatetime
                AND (
                    (outages.REVIVED_DATE IS NULL)
                    OR 
                    (TO_CHAR(outages.REVIVED_DATE, 'YYYY-MM-DD') || ' ' || outages.REVIVED_TIME) >= :targetDatetime
                )
                ORDER BY out_date_time DESC'''