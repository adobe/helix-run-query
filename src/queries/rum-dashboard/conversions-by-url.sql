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
with sidekick_events AS (
SELECT
  FORMAT_DATE("%Y-%m-%d", DATE_TRUNC(time, DAY)) AS day,
  id,
  checkpoint,
  hostname,
  url,
  pageviews,
  source
  FROM   helix_rum.CHECKPOINTS_V4( @url, @offset, @interval, @startdate, @enddate, @timezone, 'all', @domainkey )
WHERE 
  checkpoint LIKE "%convert%" OR checkpoint = "%search%"

)
SELECT   url,
         day,
         checkpoint,
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
GROUP BY sidekick_events.day, checkpoint,
         url
ORDER BY url,
         day asc