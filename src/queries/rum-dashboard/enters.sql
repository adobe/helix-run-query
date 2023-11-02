--- description: Get URL Specific Referrals Data From RUM for a given domain
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
--- threshold: 0
with enters AS (
SELECT
*
  FROM   helix_rum.CHECKPOINTS_V4( @url, @offset, @interval, '-', '-', 'UTC', 'all', @domainkey )
WHERE 
  checkpoint LIKE "enter" AND 
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

unique_sources as (
  select (case when not @exactmatch then hostname end) as hostname,(case when @exactmatch then url end) as url, split(net.reg_domain(source), '.')[offset(0)] as source, sum(pageviews) traffic from enters group by (case when not @exactmatch then hostname end), split(net.reg_domain(source), '.')[offset(0)], (case when @exactmatch then url end)
),
total_traffic as (
  select sum(traffic) as total from unique_sources
)
select *, (traffic/total)*100 as percentage from unique_sources join total_traffic on true where traffic >= (@threshold * total_traffic.total) order by traffic desc