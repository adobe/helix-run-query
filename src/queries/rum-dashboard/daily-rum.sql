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
--- exactmatch: false
--- url: -
--- device: all
--- domainkey: secret
WITH daily_rum AS
(
       SELECT Regexp_replace(url, '\\?.+', '') AS url,
              Avg(lcp)                            avglcp,
              Avg(fid)                            avgfid,
              Avg(inp)                         AS avginp,
              Avg(cls)                            avgcls,
       IF(@timeunit = 'day', format_timestamp("%Y-%m-%d", time),
       IF(@timeunit = 'hour', format_timestamp("%Y-%m-%d-%T", time), 
       format_timestamp("%Y-%m-%d", time))) AS date 
       FROM helix_rum.events_v4( 
       regexp_replace(@url, 'https://', ''), # domain OR url 
       cast(@offset AS int64), # NOT used, offset IN days FROM today 
       cast(@interval AS int64), # interval IN days TO consider 
       @startdate, # NOT used, start date 
       @enddate, # NOT used, END date 
       @timezone, # timezone 
       @device, # device class 
       @domainkey ) 
       GROUP BY url,
       date ORDER BY date ASC )
SELECT   *
FROM     daily_rum
WHERE    avglcp IS NOT NULL
AND      avgfid IS NOT NULL
AND      avgcls IS NOT NULL
AND      avginp IS NOT NULL
AND      (
                  @exactmatch = true
         AND      url = concat('https://', regexp_replace(@url, 'https://', ''))
         OR       @exactmatch = false )
AND      NOT starts_with(url, 'http://localhost')
AND      NOT starts_with(url, 'https://localhost')
ORDER BY url,
         date