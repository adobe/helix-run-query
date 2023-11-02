--- description: Get URL Specific Searches Data From RUM for a given domain
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
with searches AS (
SELECT
*
  FROM   helix_rum.CHECKPOINTS_V4( @url, @offset, @interval, '-', '-', 'UTC', 'all', @domainkey )
WHERE 
  checkpoint LIKE "%search%" AND 
  (
       (
       false = true
       AND (
              url = concat('https://', REGEXP_REPLACE(@url, 'https://', '')) 
              or
              url = concat('https://www.', REGEXP_REPLACE(@url, 'https://', '')) 
              or
              url = concat('https://www.', REGEXP_REPLACE(@url, 'www.', ''))
              or
              url = concat('https://', REGEXP_REPLACE(@url, 'https://www.', ''))
              )
       ) OR       false = false )
), 
unique_targets as (
  select (case when not false then hostname end) as hostname,(case when false then url end) as url, lower(target) as target, sum(pageviews) traffic from searches group by (case when not false then hostname end), lower(target), (case when false then url end)
)
select hostname, url, target, sum(traffic) as traffic from unique_targets group by hostname, url, target order by traffic desc