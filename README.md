
--Goodrx email Segment
DROP TABLE IF EXISTS goodrx_email_send_staging;
CREATE TEMP TABLE goodrx_email_send_staging
AS
SELECT 'goodrx' AS app,
       (CASE WHEN COALESCE(es.canvas_name,es.campaign_name) ILIKE '%core%' THEN 'CORE'
             WHEN COALESCE(es.canvas_name,es.campaign_name) ILIKE '%gold%' THEN 'GOLD'
             -- Care emails are not sent from GoodRx so no Care BU
             WHEN COALESCE(es.canvas_name,es.campaign_name) ILIKE '[NEWSLETTER]%' THEN 'NEWSLETTER'
             ELSE 'CORE' END) AS bu,
       'email' as channel,
       'delivery' AS action,
       id,
       external_user_id,
       dispatch_id,
       canvas_step_id,
       campaign_id,
       canvas_id,
       COALESCE(es.canvas_id,es.campaign_id) as canvas_or_campaign_id,
       COALESCE(es.canvas_name,es.campaign_name) as canvas_or_campaign_name,
       COALESCE(es.canvas_step_id,es.campaign_id) as step_or_campaign_id,
       COALESCE(es.canvas_step_name,es.campaign_name) as step_or_campaign_name,
       convert_timezone('US/Pacific', (TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as timestamp
FROM goodrx.braze_currents__users_messages_email_delivery AS es
WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -12, GETDATE()))::date
and timestamp::date>= '2022-07-01'
;

-- GRX email Segment
DROP TABLE IF EXISTS gdrx_email_send_staging;
CREATE TEMP TABLE gdrx_email_send_staging
AS
SELECT 'gdrx' AS app,
       (CASE WHEN COALESCE(canvas_name,campaign_name) ILIKE '%core%' THEN 'CORE'
             WHEN COALESCE(canvas_name,campaign_name) ILIKE '%gold%' THEN 'GOLD'
             WHEN (COALESCE(canvas_name,campaign_name) ILIKE '%care%' and COALESCE(canvas_name,campaign_name) NOT ILIKE '%medicare%') THEN 'CARE'
             WHEN COALESCE(canvas_name,campaign_name) ILIKE '[NEWSLETTER]%' THEN 'NEWSLETTER'
             ELSE 'CORE' END) AS bu,
       'email' as channel,
       'send' AS action,
       id,
       external_user_id,
       dispatch_id,
       canvas_step_id,
       campaign_id,
       canvas_id,
       COALESCE(canvas_id,campaign_id) as canvas_or_campaign_id,
       COALESCE(canvas_name,campaign_name) as canvas_or_campaign_name,
       COALESCE(canvas_step_id, campaign_id) as step_or_campaign_id,
       COALESCE(canvas_step_name, campaign_name) as step_or_campaign_name,
       convert_timezone('US/Pacific', (TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as timestamp
---(TIMESTAMP 'epoch' + sd.time * INTERVAL '1 second') as click_time
FROM goodrx.braze_currents_segment_gdrx__users_messages_email_delivery
--WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -3, GETDATE()))::date
WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -12, GETDATE()))::date
and timestamp::date>= '2022-07-01'
;

DROP TABLE IF EXISTS goodrx_push_send_staging;
CREATE TEMP TABLE goodrx_push_send_staging
AS
SELECT 'goodrx' AS app,
       (CASE WHEN COALESCE(canvas_name,campaign_name) ILIKE '%core%' THEN 'CORE'
             WHEN COALESCE(canvas_name,campaign_name) ILIKE '%gold%' THEN 'GOLD'
             WHEN COALESCE(canvas_name,campaign_name) ILIKE '[NEWSLETTER]%' THEN 'NEWSLETTER'
             ELSE 'CORE' END) AS bu,
       'push' as channel,
       'send' AS action,
       id,
       external_user_id,
       dispatch_id,
       canvas_step_id,
       campaign_id,
       canvas_id,
       COALESCE(canvas_id,campaign_id) as canvas_or_campaign_id,
       COALESCE(canvas_name,campaign_name) as canvas_or_campaign_name,
       COALESCE(canvas_step_id, campaign_id) as step_or_campaign_id,
       COALESCE(canvas_step_name, campaign_name) as step_or_campaign_name,
       convert_timezone('US/Pacific', (TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as timestamp
FROM goodrx.braze_currents__users_messages_pushnotification_send
--WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -3, GETDATE()))::date
WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -12, GETDATE()))::date
and timestamp::date>= '2022-07-01'
;

-- GRX Segment
DROP TABLE IF EXISTS gdrx_push_send;
CREATE TEMP TABLE gdrx_push_send
AS
SELECT 'gdrx' AS app,
       (CASE WHEN COALESCE(canvas_name,campaign_name) ILIKE '%core%' THEN 'CORE'
             WHEN COALESCE(canvas_name,campaign_name) ILIKE '%gold%' THEN 'GOLD'
             WHEN (COALESCE(canvas_name,campaign_name) ILIKE '%care%' and COALESCE(canvas_name,campaign_name) NOT ILIKE '%medicare%') THEN 'CARE'
             WHEN COALESCE(canvas_name,campaign_name) ILIKE '[NEWSLETTER]%' THEN 'NEWSLETTER'
             ELSE 'CORE' END) AS bu,
       'push' as channel,
       'send' AS action,
       id,
       external_user_id,
       dispatch_id,
       canvas_step_id,
       campaign_id,
       canvas_id,
       COALESCE(canvas_id,campaign_id) as canvas_or_campaign_id,
       COALESCE(canvas_name,campaign_name) as canvas_or_campaign_name,
       COALESCE(canvas_step_id, campaign_id) as step_or_campaign_id,
       COALESCE(canvas_step_name, campaign_name) as step_or_campaign_name,
       convert_timezone('US/Pacific', (TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as timestamp
FROM goodrx.braze_currents_segment_gdrx__users_messages_pushnotification_send
--WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -3, GETDATE()))::date
WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -12, GETDATE()))::date
and timestamp::date>= '2022-07-01'
;

commit;

DROP TABLE IF EXISTS goodrx_inapp_send_staging;
CREATE TEMP TABLE goodrx_inapp_send_staging
AS
SELECT 'goodrx' AS app,
       (CASE WHEN COALESCE(canvas_name,campaign_name) ILIKE '%core%' THEN 'CORE'
             WHEN COALESCE(canvas_name,campaign_name) ILIKE '%gold%' THEN 'GOLD'
             WHEN COALESCE(canvas_name,campaign_name) ILIKE '[NEWSLETTER]%' THEN 'NEWSLETTER'
             ELSE 'CORE' END) AS bu,
       'inapp' as channel,
       'send' AS action,
       id,
       external_user_id,
       canvas_step_id,
       campaign_id,
       canvas_id,
       COALESCE(canvas_id,campaign_id) as canvas_or_campaign_id,
       COALESCE(canvas_name,campaign_name) as canvas_or_campaign_name,
       COALESCE(canvas_step_id, campaign_id) as step_or_campaign_id,
       COALESCE(canvas_step_name, campaign_name) as step_or_campaign_name,
       convert_timezone('US/Pacific', (TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as timestamp
FROM goodrx.braze_currents__users_messages_inappmessage_impression
--WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -3, GETDATE()))::date
WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -12, GETDATE()))::date
and timestamp::date>= '2022-07-01';

DROP TABLE IF EXISTS gdrx_inapp_send_staging;
CREATE TEMP TABLE gdrx_inapp_send_staging
AS
SELECT 'gdrx' AS app,
       (CASE WHEN COALESCE(canvas_name,campaign_name) ILIKE '%core%' THEN 'CORE'
             WHEN COALESCE(canvas_name,campaign_name) ILIKE '%gold%' THEN 'GOLD'
             WHEN COALESCE(canvas_name,campaign_name) ILIKE '[NEWSLETTER]%' THEN 'NEWSLETTER'
             ELSE 'CORE' END) AS bu,
       'inapp' as channel,
       'send' AS action,
       id,
       external_user_id,
       canvas_step_id,
       campaign_id,
       canvas_id,
       COALESCE(canvas_id,campaign_id) as canvas_or_campaign_id,
       COALESCE(canvas_name,campaign_name) as canvas_or_campaign_name,
       COALESCE(canvas_step_id, campaign_id) as step_or_campaign_id,
       COALESCE(canvas_step_name, campaign_name) as step_or_campaign_name,
       convert_timezone('US/Pacific', (TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as timestamp
FROM goodrx.braze_currents_segment_gdrx__users_messages_inappmessage_impression
--WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -3, GETDATE()))::date
WHERE timestamp::date>= DATE_TRUNC('month', DATEADD(month, -12, GETDATE()))::date
and timestamp::date>= '2022-07-01';
-----
--select * from gdrx_inapp_send_staging limit 10;

DROP TABLE IF EXISTS gdrx_canvas_staging;
CREATE TEMP TABLE gdrx_canvas_staging
AS
SELECT 'gdrx' as app,
       canvas_id,
       name,
       (CASE  --(WHEN name ILIKE '%gold%' AND name ILIKE '%global%' AND name ILIKE '%control%') THEN 'Gold Global Control'
           WHEN  name ILIKE '%abandoned%' THEN 'Gold Abandoned Registration'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%SS/PV/CV%') THEN 'Gold SS/PV/CV'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%PV/CV%' AND name ILIKE '%nonmaint%') THEN 'Gold PV/CV Non-Maintenance'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%PV/CV%' AND name ILIKE '%maint%') THEN 'Gold PV/CV Maintenance'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%SS%') THEN 'Gold SS'
           WHEN (name ILIKE '%coupon%' AND name ILIKE '%view%' AND name ILIKE '%savings%') THEN 'Gold CV Savings >$1'
           WHEN (name ILIKE '%coupon%' AND name ILIKE '%view%' and name NOT ILIKE '%core%') THEN 'Gold CV'
           WHEN (name ILIKE '%price%' AND name ILIKE '%view%' AND name NOT ILIKE '%core%') THEN 'Gold PV'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%saved%' AND name ILIKE '%drug%') THEN 'Gold Saved Drugs'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%savings%' AND name ILIKE '%alert%') THEN 'Gold Savings Alert'
           WHEN (name ILIKE '%gold%') THEN 'Gold Batch'
           WHEN (name ILIKE '%core%' AND name ILIKE '%onboarding%') THEN 'Core Onboarding'
           WHEN (name ILIKE '%core%' AND name ILIKE '%Refill%' AND name ILIKE '%reminder%') THEN 'Core Refill Reminder'
           WHEN (name ILIKE '%core%' and name ILIKE '%dormant%') THEN 'Core Dormant'
           WHEN (name ILIKE '%core%' AND name ILIKE '%pharmacy%' AND name ILIKE '%pick%' AND name ILIKE '%reminder%') THEN 'Core Pharmacy Pickup Reminder'
           WHEN (name ILIKE '%core%' AND name ILIKE '%claim%' AND name ILIKE '%reversal%') THEN 'Core Claims Reversals'
           WHEN (name ILIKE '%core%' AND name ILIKE '%eob%') THEN 'Core EOB'
           WHEN (name ILIKE '%core%' AND name ILIKE '%missed%') THEN 'Core Missed Fill'
           WHEN (name ILIKE '%core%' AND name ILIKE '%new%' AND name ILIKE '%subscriber%') THEN 'CORE Newsletter Onboarding'
           ELSE 'Other'
           END
           ) AS canvas_name_rollup,
       (CASE WHEN (channels ILIKE '%push%' AND channels ILIKE '%in_app%' AND channels ILIKE '%email%') THEN 'push+in_app+email'
             WHEN (channels ILIKE '%push%' AND channels ILIKE '%in_app%') THEN 'push+in_app'
             WHEN (channels ILIKE '%push%' AND channels ILIKE '%email%') THEN 'push+email'
             WHEN (channels ILIKE '%in_app%' AND channels ILIKE '%email%') THEN 'in_app+email'
             WHEN (channels ILIKE '%push%') THEN 'push'
             WHEN (channels ILIKE '%in_app%') THEN 'in_app'
             WHEN (channels ILIKE '%email%') THEN 'email'
             WHEN (channels ILIKE '%webhook%') THEN 'webhook'
             ELSE 'other'
             END
           ) AS canvas_channel
FROM goodrx.braze_api_gdrx_segment_canvas
WHERE _execution_date IN (SELECT MAX(_execution_date) FROM goodrx.braze_api_gdrx_segment_canvas)
AND ((tags ILIKE '%gold%' AND tags ILIKE '%upsell%') OR (tags ILIKE '%gold%' AND tags ILIKE '%UPS%'))
AND draft='false'
;
DROP TABLE IF EXISTS goodrx_canvas_staging;
CREATE TEMP TABLE goodrx_canvas_staging
AS
SELECT 'goodrx' AS app,
       canvas_id,
       name,
 (CASE --WHEN (name ILIKE '%gold%' AND name ILIKE '%global%' AND name ILIKE '%control%') THEN 'Gold Global Control'
           WHEN  name ILIKE '%abandoned%' THEN 'Gold Abandoned Registration'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%SS/PV/CV%') THEN 'Gold SS/PV/CV'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%PV/CV%' AND name ILIKE '%nonmaint%') THEN 'Gold PV/CV Non-Maintenance'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%PV/CV%' AND name ILIKE '%maint%') THEN 'Gold PV/CV Maintenance'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%SS%') THEN 'Gold SS'
           WHEN (name ILIKE '%coupon%' AND name ILIKE '%view%' AND name ILIKE '%savings%') THEN 'Gold CV Savings >$1'
           WHEN (name ILIKE '%coupon%' AND name ILIKE '%view%' and name NOT ILIKE '%core%') THEN 'Gold CV'
           WHEN (name ILIKE '%price%' AND name ILIKE '%view%' AND name NOT ILIKE '%core%') THEN 'Gold PV'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%saved%' AND name ILIKE '%drug%') THEN 'Gold Saved Drugs'
           WHEN (name ILIKE '%gold%' AND name ILIKE '%savings%' AND name ILIKE '%alert%') THEN 'Gold Savings Alert'
           WHEN (name ILIKE '%gold%') THEN 'Gold Batch'
           WHEN (name ILIKE '%core%' AND name ILIKE '%onboarding%') THEN 'Core Onboarding'
           WHEN (name ILIKE '%core%' AND name ILIKE '%Refill%' AND name ILIKE '%reminder%') THEN 'Core Refill Reminder'
           WHEN (name ILIKE '%core%' and name ILIKE '%dormant%') THEN 'Core Dormant'
           WHEN (name ILIKE '%core%' AND name ILIKE '%pharmacy%' AND name ILIKE '%pick%' AND name ILIKE '%reminder%') THEN 'Core Pharmacy Pickup Reminder'
           WHEN (name ILIKE '%core%' AND name ILIKE '%claim%' AND name ILIKE '%reversal%') THEN 'Core Claims Reversals'
           WHEN (name ILIKE '%core%' AND name ILIKE '%eob%') THEN 'Core EOB'
           WHEN (name ILIKE '%core%' AND name ILIKE '%missed%') THEN 'Core Missed Fill'
           WHEN (name ILIKE '%core%' AND name ILIKE '%new%' AND name ILIKE '%subscriber%') THEN 'CORE Newsletter Onboarding'
           ELSE 'Other'
           END
           ) AS canvas_name_rollup,
       (CASE WHEN (channels ILIKE '%push%' AND channels ILIKE '%in_app%' AND channels ILIKE '%email%') THEN 'push+in_app+email'
             WHEN (channels ILIKE '%push%' AND channels ILIKE '%in_app%') THEN 'push+in_app'
             WHEN (channels ILIKE '%push%' AND channels ILIKE '%email%') THEN 'push+email'
             WHEN (channels ILIKE '%in_app%' AND channels ILIKE '%email%') THEN 'in_app+email'
             WHEN (channels ILIKE '%push%') THEN 'push'
             WHEN (channels ILIKE '%in_app%') THEN 'in_app'
             WHEN (channels ILIKE '%email%') THEN 'email'
             WHEN (channels ILIKE '%webhook%') THEN 'webhook'
             ELSE 'other'
             END
           ) AS canvas_channel
FROM goodrx.braze_api_canvas
WHERE _execution_date IN (SELECT MAX(_execution_date) FROM goodrx.braze_api_canvas)
AND ((tags ILIKE '%gold%' AND tags ILIKE '%upsell%') OR (tags ILIKE '%gold%' AND tags ILIKE '%UPS%'))
AND draft='false'
;

commit;

-- inapp clicks in GRX are not tracked correctly (know issue to Braze); use inapp impression as a proxy for inapp clicks.
-- to union email_click+inapp_impression+push_open, in order to count unique recipients from all sends
-- set window starting '2022-01-01'
DROP TABLE IF EXISTS gold_upsell_grx_email_click;
CREATE TEMP TABLE gold_upsell_grx_email_click
AS
    (
-- GRX Segment email click
        select 'gdrx'                                                                              as app,
               'click'                                                                             as engagement_type,
               'email'                                                                             as channel,
--                cd.canvas_name_rollup,
               canvas_name,
               canvas_variation_name                                                               as variant_name,
               canvas_step_name                                                                    as step_name,
               user_id,
               canvas_id,
               canvas_variation_id                                                                 as variant_id,
--                canvas_step_id                                                                      as step_id,
               id,
               external_user_id,
               dispatch_id,
               canvas_step_id,
               campaign_id,
               convert_timezone('US/Pacific', (TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as click_time
        from --dl_pii_braze_currents_grx_segment.users_messages_email_click AS sd
             goodrx.braze_currents_segment_gdrx__users_messages_email_click
        --WHERE click_time::date >= '2022-01-01'
        );
commit;
DROP TABLE IF EXISTS gold_upsell_grx_inapp_click;
CREATE TEMP TABLE gold_upsell_grx_inapp_click
AS
(select        'gdrx'                                                                            as app,
               'impression'                                                                      as engagement_type,
               'inapp'                                                                           as channel,
               --cd.canvas_name_rollup,
              canvas_name,
               canvas_variation_name                                                             as variant_name,
               canvas_step_name                                                                  as step_name,
               user_id,
               canvas_id,
               canvas_variation_id                                                               as variant_id,
--                canvas_step_id                                                                      as step_id,
               id,
               external_user_id,
               --dispatch_id,
               canvas_step_id,
               campaign_id,
               convert_timezone('US/Pacific',(TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as click_time
        from --dl_pii_braze_currents_grx_segment.users_messages_inappmessage_impression AS i
              goodrx.braze_currents_segment_gdrx__users_messages_inappmessage_impression
 --WHERE click_time::date >= '2022-01-01'
 );

DROP TABLE IF EXISTS gold_upsell_grx_push_click;
CREATE TEMP TABLE gold_upsell_grx_push_click
as
(select 'gdrx'                                                                            AS app,
               'open'                                                                              as engagement_type,
               'push'                                                                              as channel,
               --cd.canvas_name_rollup,
               canvas_name,
               canvas_variation_name                                                               as variant_name,
               canvas_step_name                                                                    as step_name,
               user_id,
               canvas_id,
               canvas_variation_id                                                                 as variant_id,
--                canvas_step_id                                                                      as step_id,
               id,
               external_user_id,
               dispatch_id,
               canvas_step_id,
               campaign_id,
               convert_timezone('US/Pacific',(TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as click_time
        from --dl_pii_braze_currents_grx_segment.users_messages_pushnotification_open AS ps
              goodrx.braze_currents_segment_gdrx__users_messages_pushnotification_open
 --WHERE click_time::date >= '2022-01-01'
 );


DROP TABLE IF EXISTS gold_upsell_goodrx_email_click;
CREATE TEMP TABLE gold_upsell_goodrx_email_click
AS
    (
        select 'goodrx'                                                                            as app,
               'click'                                                                             as engagement_type,
               'email'                                                                             as channel,
               canvas_name,
               canvas_variation_name                                                               as variant_name,
               canvas_step_name                                                                    as step_name,
               user_id,
               canvas_id,
               canvas_variation_id                                                                 as variant_id,
--                canvas_step_id                                                                      as step_id,
               id,
               external_user_id,
               dispatch_id,
               canvas_step_id,
               campaign_id,
               convert_timezone('US/Pacific',(TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as click_time
        from goodrx.braze_currents_segment_gdrx__users_messages_email_click
         --WHERE click_time::date >= '2022-01-01'
         );
            ------

DROP TABLE IF EXISTS gold_upsell_goodrx_inapp_click;
CREATE TEMP TABLE gold_upsell_goodrx_inapp_click
AS      -- Goodrx inapp impression
        (select 'goodrx'                                                                           as app,
               'impression'                                                                        as engagement_type,
               'inapp'                                                                             as channel,
               canvas_name,
               canvas_variation_name                                                               as variant_name,
               canvas_step_name                                                                    as step_name,
               user_id,
               canvas_id,
               canvas_variation_id                                                                 as variant_id,
--                canvas_step_id                                                                      as step_id,
               id,
               external_user_id,
               --dispatch_id,
               canvas_step_id,
               campaign_id,
               convert_timezone('US/Pacific',(TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as click_time
        from --dl_pii_braze_currents_goodrx.users_messages_inappmessage_impression AS i
              goodrx.braze_currents_segment_gdrx__users_messages_inappmessage_impression
        --WHERE click_time::date >= '2022-01-01'
        );


DROP TABLE IF EXISTS gold_upsell_goodrx_push_click;
CREATE TEMP TABLE gold_upsell_goodrx_push_click
as
-- Goodrx push open
        (select 'goodrx'                                                                           as app,
               'open'                                                                              as engagement_type,
               'push'                                                                              as channel,
               canvas_name,
               canvas_variation_name                                                               as variant_name,
               canvas_step_name                                                                    as step_name,
               user_id,
               canvas_id,
               canvas_variation_id                                                                 as variant_id,
--                canvas_step_id                                                                      as step_id,
               id,
               external_user_id,
               dispatch_id,
               canvas_step_id,
               campaign_id,
               convert_timezone('US/Pacific',(TIMESTAMP 'epoch' + time * INTERVAL '1 second')) as click_time
        from --dl_pii_braze_currents_goodrx.users_messages_pushnotification_open AS ps
              goodrx.braze_currents_segment_gdrx__users_messages_pushnotification_open
                  --WHERE click_time::date >= '2022-01-01'
                  );



--select * from goodrx.braze_currents_segment_gdrx__users_messages_email_click limit 5;

DROP TABLE IF EXISTS gold_upsell_step_level_click;
CREATE TEMP TABLE gold_upsell_step_level_click
AS
    (
-- GRX Segment email click
        select sd.app,
               sd.engagement_type,
               sd.channel,
               cd.canvas_name_rollup,
               sd.canvas_name,
               sd.variant_name,
               sd.step_name,
               sd.user_id,
               sd.canvas_id,
               sd.variant_id,
               sd.canvas_step_id,
               sd.id,
               sd.external_user_id,
               sd.click_time
        from --dl_pii_braze_currents_grx_segment.users_messages_email_click AS sd
              --goodrx.braze_currents_segment_gdrx__users_messages_email_click AS sd
                gold_upsell_grx_email_click as sd
                  join gdrx_email_send_staging as es
                   ON es.external_user_id=sd.external_user_id
     AND COALESCE(es.dispatch_id,es.canvas_step_id, es.campaign_id)=COALESCE(sd.dispatch_id,sd.canvas_step_id, sd.campaign_id)
     AND sd.click_time <= dateadd(day,7,es.timestamp)
--                 -------
                 join gdrx_canvas_staging cd
                      on sd.canvas_id = cd.canvas_id

        WHERE click_time::date >= '2022-01-01'
--         limit 10;
    UNION ALL
        -- GRX Segment inapp impression
        select i.app,
               i.engagement_type,
               i.channel,
               cd.canvas_name_rollup,
               i.canvas_name,
               i.variant_name,
               i.step_name,
               i.user_id,
               i.canvas_id,
               i.variant_id,
               i.canvas_step_id,
               i.id,
               i.external_user_id,
               i.click_time
        from --dl_pii_braze_currents_grx_segment.users_messages_inappmessage_impression AS i
              --goodrx.braze_currents_segment_gdrx__users_messages_inappmessage_impression AS i
                gold_upsell_grx_inapp_click as i
                 join gdrx_inapp_send_staging as s
                  ON s.external_user_id=i.external_user_id
    AND COALESCE(s.canvas_step_id, s.campaign_id)=COALESCE(i.canvas_step_id, i.campaign_id)
    AND click_time<=dateadd(day,7,s.timestamp)
                 join gdrx_canvas_staging cd
                      on i.canvas_id = cd.canvas_id
        WHERE click_time::date >= '2022-01-01'
        UNION ALL
-- GRX Segment push open
        select ps.app,
               ps.engagement_type,
               ps.channel,
               cd.canvas_name_rollup,
               ps.canvas_name,
               ps.variant_name,
               ps.step_name,
               ps.user_id,
               ps.canvas_id,
               ps.variant_id,
               ps.canvas_step_id,
               ps.id,
               ps.external_user_id,
               ps.click_time
        from --dl_pii_braze_currents_grx_segment.users_messages_pushnotification_open AS ps
              --goodrx.braze_currents_segment_gdrx__users_messages_pushnotification_open AS ps
                gold_upsell_grx_push_click as ps
                  join gdrx_push_send as pt
                  ON ps.external_user_id=pt.external_user_id
    AND COALESCE(ps.dispatch_id,ps.canvas_step_id, ps.campaign_id)=COALESCE(pt.dispatch_id,pt.canvas_step_id, pt.campaign_id)
    AND ps.click_time<=dateadd(day,7,pt.timestamp)
                 join gdrx_canvas_staging cd
                      on ps.canvas_id = cd.canvas_id
        WHERE click_time::date >= '2022-01-01'
    -- GoodRx email click
    UNION ALL
        select sd.app,
               sd.engagement_type,
               sd.channel,
               cd.canvas_name_rollup,
               sd.canvas_name,
               sd.variant_name,
               sd.step_name,
               sd.user_id,
               sd.canvas_id,
               sd.variant_id,
               sd.canvas_step_id,
               sd.id,
               sd.external_user_id,
               sd.click_time
        from --goodrx.braze_currents_segment_gdrx__users_messages_email_click AS sd
            ------
            gold_upsell_goodrx_email_click as sd
            join goodrx_email_send_staging as es
            ON es.external_user_id=sd.external_user_id
    AND COALESCE(es.dispatch_id,es.canvas_step_id, es.campaign_id)=COALESCE(sd.dispatch_id,sd.canvas_step_id, sd.campaign_id)
    AND click_time<=dateadd(day,7,es.timestamp)
             ------
            join goodrx_canvas_staging cd
                      on sd.canvas_id = cd.canvas_id
        WHERE click_time::date >= '2022-01-01'
    UNION ALL
        -- Goodrx inapp impression
        select i.app,
               i.engagement_type,
               i.channel,
               cd.canvas_name_rollup,
               i.canvas_name,
               i.variant_name,
               i.step_name,
               i.user_id,
               i.canvas_id,
               i.variant_id,
               i.canvas_step_id,
               i.id,
               i.external_user_id,
               i.click_time
        from --dl_pii_braze_currents_goodrx.users_messages_inappmessage_impression AS i
              --goodrx.braze_currents_segment_gdrx__users_messages_inappmessage_impression AS i
                 gold_upsell_goodrx_inapp_click as i
                  join goodrx_inapp_send_staging as s
                  ON s.external_user_id=i.external_user_id
    AND COALESCE(s.canvas_step_id, s.campaign_id)=COALESCE(i.canvas_step_id, i.campaign_id)
    AND click_time<=dateadd(day,7,s.timestamp)
                 join goodrx_canvas_staging cd
                      on i.canvas_id = cd.canvas_id
        WHERE click_time::date >= '2022-01-01'
        UNION ALL
-- Goodrx push open
        select ps.app,
               ps.engagement_type,
               ps.channel,
               cd.canvas_name_rollup,
               ps.canvas_name,
               ps.variant_name,
               ps.step_name,
               ps.user_id,
               ps.canvas_id,
               ps.variant_id,
               ps.canvas_step_id,
               ps.id,
               ps.external_user_id,
               ps.click_time
        from --dl_pii_braze_currents_goodrx.users_messages_pushnotification_open AS ps
              --goodrx.braze_currents_segment_gdrx__users_messages_pushnotification_open AS ps
                gold_upsell_goodrx_push_click as ps
                  join goodrx_push_send_staging as pt
ON ps.external_user_id=pt.external_user_id
    AND COALESCE(ps.dispatch_id,ps.canvas_step_id, ps.campaign_id)=COALESCE(pt.dispatch_id,pt.canvas_step_id, pt.campaign_id)
    AND click_time<=dateadd(day,7,pt.timestamp)
                 join goodrx_canvas_staging cd
                      on ps.canvas_id = cd.canvas_id
        WHERE click_time::date >= '2022-01-01'
        )
;
DROP TABLE IF EXISTS analyst_scratch.aj_gold_upsell_dashboard_click_based_step_level;
SELECT *
INTO analyst_scratch.aj_gold_upsell_dashboard_click_based_step_level
FROM gold_upsell_step_level_click
;
DROP TABLE IF EXISTS gold_upsell_conversion_base;
CREATE TEMP TABLE gold_upsell_conversion_base
AS
    SELECT app,
            channel,
            canvas_name_rollup,
            click.click_time,
            (TIMESTAMP 'epoch' + con.time * INTERVAL '1 second') AS conversion_time,
            click.external_user_id,
            click.canvas_id,
            click.canvas_name,
            click.canvas_step_id,
            click.step_name,
            -- json_extract_path_text(properties,'is_trial') AS is_trial,
            json_extract_path_text(properties,'plan_type') AS plan_type,
            json_extract_path_text(properties,'invoice_interval') AS billing_cadence
            FROM gold_upsell_step_level_click AS click
            JOIN dl_pii_braze_currents_grx_segment.users_behaviors_customevent AS con
               -- braze_currents_segment_gdrx__users_behaviors_customevent AS con
            ON click.external_user_id = con.external_user_id
            AND (TIMESTAMP 'epoch' + con.time * INTERVAL '1 second') > click.click_time
            AND (TIMESTAMP 'epoch' + con.time * INTERVAL '1 second') < click.click_time + interval '7 days'
            AND name = 'Gold Subscription Activated'
            WHERE app = 'gdrx'
        UNION ALL
        SELECT app,
            channel,
            canvas_name_rollup,
            click.click_time,
            (TIMESTAMP 'epoch' + con.time * INTERVAL '1 second') AS conversion_time,
            click.external_user_id,
            click.canvas_id,
            click.canvas_name,
            click.canvas_step_id,
            click.step_name,
            -- json_extract_path_text(properties,'is_trial') AS is_trial,
            json_extract_path_text(properties,'subscription.plan_type') AS plan_type,
            (CASE WHEN json_extract_path_text(properties,'subscription.period') = 'month'
                THEN 'monthly'
                ELSE 'annually'
                END) AS billing_cadence
            FROM gold_upsell_step_level_click AS click
            JOIN dl_pii_braze_currents_goodrx.users_behaviors_customevent AS con
                --braze_currents_segment_gdrx__users_behaviors_customevent AS con
            ON click.external_user_id = con.external_user_id
            AND (TIMESTAMP 'epoch' + con.time * INTERVAL '1 second') > click.click_time
            AND (TIMESTAMP 'epoch' + con.time * INTERVAL '1 second') < click.click_time + interval '7 days'
            AND name = 'gold.subscription.activated'
            WHERE app = 'goodrx'
;
DROP TABLE IF EXISTS gold_upsell_conversion_dedup;
CREATE TEMP TABLE gold_upsell_conversion_dedup
AS
    SELECT *
        FROM
    (SELECT *,
            ROW_NUMBER() OVER (PARTITION BY external_user_id, conversion_time ORDER BY click_time desc) AS rnk
     FROM gold_upsell_conversion_base
    ) a
   WHERE rnk=1
;
DROP TABLE IF EXISTS analyst_scratch.aj_gold_upsell_dashboard_click_based_conversion_dedup;
SELECT *
INTO analyst_scratch.aj_gold_upsell_dashboard_click_based_conversion_dedup
FROM gold_upsell_conversion_dedup
;

- -- Chart 1: weekly scorecard
SELECT a.week_start,
       unique_recipients,
       trial_starts
FROM   (SELECT date_trunc('week', click_time)::date AS week_start,
               COUNT(distinct external_user_id) AS unique_recipients
        FROM analyst_scratch.aj_gold_upsell_dashboard_click_based_step_level
        GROUP BY 1
    ) AS a
    LEFT JOIN
    (SELECT date_trunc('week', click_time)::date AS week_start,
               COUNT(distinct external_user_id) AS trial_starts
        FROM analyst_scratch.aj_gold_upsell_dashboard_click_based_conversion_dedup
        GROUP BY 1
    ) AS b
    ON a.week_start=b.week_start
ORDER BY 1
;
-- Chart 2: monthly scorecard
SELECT a.month_start,
       unique_recipients,
       trial_starts
FROM   (SELECT date_trunc('month', click_time)::date AS month_start,
               COUNT(distinct external_user_id) AS unique_recipients
        FROM analyst_scratch.aj_gold_upsell_dashboard_click_based_step_level
        GROUP BY 1
    ) AS a
    LEFT JOIN
    (SELECT date_trunc('month', click_time)::date AS month_start,
               COUNT(distinct external_user_id) AS trial_starts
        FROM analyst_scratch.aj_gold_upsell_dashboard_click_based_conversion_dedup
        GROUP BY 1
    ) AS b
    ON a.month_start=b.month_start
ORDER BY 1
;

SELECT date_trunc('week', TIMESTAMP '20220701 04:05:06.789');
