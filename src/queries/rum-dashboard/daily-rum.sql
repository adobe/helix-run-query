--- description: Get Daily Helix RUM data for a given domain
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2023-02-01
--- enddate: 2023-05-28
--- timezone: UTC
--- timeunit: day
--- exactmatch: true
--- url: -
--- device: all
--- domainkey: secret
WITH daily_rum AS
(
       SELECT Regexp_replace(url, '\\?.+', '') AS url,
       CAST(APPROX_QUANTILES(lcp, 100)[OFFSET(75)] AS INT64)/1000 AS avglcp,
       CAST(APPROX_QUANTILES(fid, 100)[OFFSET(75)] AS INT64) AS avgfid,
       CAST(APPROX_QUANTILES(inp, 100)[OFFSET(75)] AS INT64) AS avginp,
       ROUND(APPROX_QUANTILES(cls, 100)[OFFSET(75)], 3) AS avgcls,
       IF(@timeunit = 'day', format_timestamp("%Y-%m-%d", time),
       IF(@timeunit = 'hour', format_timestamp("%Y-%m-%d-%T", time), 
       format_timestamp("%Y-%m-%d", time))) AS date 
       FROM helix_rum.EVENTS_V4(
              @url, # domain or URL
              CAST(@offset AS INT64), # not used, offset in days from today
              CAST(@interval AS INT64), # interval in days to consider
              @startdate, # not used, start date
              @enddate, # not used, end date
              @timezone, # timezone
              @device, # device class
              @domainkey
        ) 
       GROUP BY url,
       date ORDER BY date ASC )
SELECT   *
FROM     daily_rum
WHERE    avglcp IS NOT NULL
AND      avgfid IS NOT NULL
AND      avgcls IS NOT NULL
AND      (
              (@exactmatch = true
         AND (
              url = concat('https://', REGEXP_REPLACE(@url, 'https://', '')) 
              or
              url = concat('https://www.', REGEXP_REPLACE(@url, 'https://', '')) 
              or
              url = concat('https://www.', REGEXP_REPLACE(@url, 'www.', ''))
              or
              url = concat('https://', REGEXP_REPLACE(@url, 'https://www.', ''))
              ))
         OR       @exactmatch = false )
AND      NOT starts_with(url, 'http://localhost')
AND      NOT starts_with(url, 'https://localhost')
ORDER BY url,
         date