--- description: Get Detailed Daily Sidekick Data From RUM for a given domain
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2023-02-01
--- enddate: 2023-05-28
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret
with sidekick_events AS (
SELECT
  FORMAT_DATE("%Y-%m-%d", DATE_TRUNC(time, DAY)) AS day,
  id,
  checkpoint,
  hostname,
  source,
  user_agent LIKE "%Sidekick%" AS extension
FROM `helix-225321.helix_rum`.CHECKPOINTS_V4(@url, @offset, @interval, @startdate, @enddate, @timezone, 'all', @domainkey)
WHERE 
  checkpoint LIKE "sidekick:%"
)
SELECT
  hostname,
  day,
  COUNT(*) AS actions,
  checkpoint, 
FROM sidekick_events
GROUP BY hostname, sidekick_events.day, checkpoint
ORDER BY day asc

--- description: Get Daily Sidekick Data From RUM for a given domain
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2023-02-01
--- enddate: 2023-05-28
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret
WITH sidekick_events AS
(
       SELECT Format_date("%Y-%m-%d", Date_trunc(time, day)) AS day,
              id,
              checkpoint,
              hostname,
              source,
              user_agent LIKE "%Sidekick%" AS extension
       FROM   helix_rum.CHECKPOINTS_V4(@url, @offset, @interval, @startdate, @enddate, @timezone, 'all', @domainkey )
       WHERE  CHECKPOINT LIKE "sidekick:%")
SELECT   day,
         count(*)                   AS actions,
         checkpoint,
FROM     sidekick_events
WHERE
(
       (
       @exactmatch = true
       AND (
              url = concat('https://', REGEXP_REPLACE(@url, 'https://', '')) 
              or
              url = concat('https://www.', REGEXP_REPLACE(@url, 'https://', '')) 
              or
              url = concat('https://www.', REGEXP_REPLACE(@url, 'www.', ''))
              or
              url = concat('https://', REGEXP_REPLACE(@url, 'https://www.', ''))
              )
       ) OR       @exactmatch = false )
GROUP BY sidekick_events.day, checkpoint,
         hostname
ORDER BY hostname,
         day asc