--- description: Get URL Request for Quotes Data From RUM for a given domain
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- timezone: UTC
--- exactmatch: true
--- url: -
--- device: all
--- domainkey: secret
with conversions AS (
SELECT
*
  FROM   helix_rum.CHECKPOINTS_V4( @url, @offset, @interval, '-', '-', 'UTC', 'all', @domainkey )
WHERE 
  checkpoint LIKE "%convert%" AND 
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
), 
unique_targets as (
  select (case when not @exactmatch then hostname end) as hostname,(case when @exactmatch then url end) as url, lower(target) as target, sum(pageviews) traffic from conversions group by (case when not @exactmatch then hostname end), lower(target), (case when @exactmatch then url end)
)
select hostname, url, target, sum(traffic) as traffic from unique_targets group by hostname, url, target order by traffic desc