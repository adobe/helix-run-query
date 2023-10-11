--- description: Get Daily Lighthouse Scores For a Site
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2023-02-01
--- enddate: 2023-05-28
--- timezone: UTC
--- timeunit: day
--- url: -
--- device: all
--- domainkey: secret
with current_data as (
SELECT 
  * 
FROM 
  helix_external_data.LIGHTHOUSE_SCORES_OPT(
    @url, 
    @offset, 
    @interval, 
    @startdate,
    @enddate, 
    @domainkey
  )
) 
select * from current_data
order by url, time asc
