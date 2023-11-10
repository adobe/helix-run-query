--- description: Get Helix RUM data for a given domain or owner/repo combination
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 10
--- interval: 30
--- offset: 0
--- startdate: 2022-02-01
--- enddate: 2022-05-28
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret
with pageviews_by_id as (
  SELECT hostname, id, max(weight) as pageviews FROM `helix-225321.helix_rum.EVENTS_V4`(net.host(@url), @offset, @interval, '-', '-', 'UTC', 'all', @domainkey) group by id, hostname
)
select hostname, sum(pageviews) as pageviews from pageviews_by_id group by hostname