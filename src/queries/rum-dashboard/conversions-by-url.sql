--- description: Get URL Specific Daily Conversion Data From RUM for a given domain
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2023-02-01
--- enddate: 2023-05-28
--- timezone: UTC
--- exactmatch: true
--- url: -
--- device: all
--- domainkey: secret
--- ckpt: search
with sidekick_events AS (
SELECT
  FORMAT_DATE("%Y-%m-%d", DATE_TRUNC(time, DAY)) AS day,
  id,
  checkpoint,
  hostname,
  url,
  pageviews,
  source,
  target
  FROM   helix_rum.CHECKPOINTS_V4( @url, @offset, @interval, @startdate, @enddate, 'UTC', 'all', @domainkey )
WHERE 
  checkpoint LIKE '%convert%'

)
SELECT   url,
         checkpoint,
         target,
         sum(pageviews) AS invocations,
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
GROUP BY checkpoint,
         url, target
ORDER BY invocations asc;