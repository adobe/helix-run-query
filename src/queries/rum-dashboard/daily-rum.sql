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
WITH
daily_rum AS (
  SELECT 
    REGEXP_REPLACE(url, '\\?.+', '') as url,
    avg(lcp) avglcp, 
    avg(fid) avgfid, 
    avg(inp) as avginp, 
    avg(cls) avgcls, 
    IF(@timeunit = 'day', FORMAT_TIMESTAMP("%Y-%m-%d", time), 
    IF(@timeunit = 'hour', FORMAT_TIMESTAMP("%Y-%m-%d-%T", time), 
    FORMAT_TIMESTAMP("%Y-%m-%d", time))) AS date
   FROM
    helix_rum.EVENTS_V3(
      REGEXP_REPLACE(@url, 'https://', ''), # domain or URL
      CAST(@offset AS INT64), # not used, offset in days from today
      CAST(@interval AS INT64), # interval in days to consider
      @startdate, # not used, start date
      @enddate, # not used, end date
      @timezone, # timezone
      @device, # device class
      @domainkey
    )
    group by url, date
    order by date asc
)
select * from daily_rum 
where avglcp is not null
  and avgfid is not null
  and avgcls is not null
  and avginp is not null
  and (
    @exactmatch = true and url = concat('https://', REGEXP_REPLACE(@url, 'https://', ''))
    or 
    @exactmatch = false
    ) 
  and not starts_with(url, 'http://localhost')
  and not starts_with(url, 'https://localhost')
  order by url, date