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
<<<<<<<< HEAD:src/queries/rum-dashboard/rfqs.sql
with rfqs AS (
SELECT
*
  FROM   helix_rum.CHECKPOINTS_V4( @url, @offset, @interval, '-', '-', 'UTC', 'all', @domainkey )
WHERE 
  checkpoint LIKE "%rfq%" AND 
  (
========
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
  FROM   helix_rum.CHECKPOINTS_V4( @url, @offset, @interval, '-', '-', 'UTC', 'all', @domainkey )
WHERE 
  checkpoint LIKE @ckpt

)
SELECT   url,
         checkpoint,
         target,
         sum(pageviews) AS invocations,
FROM     sidekick_events
WHERE
(
>>>>>>>> 957ce4a (checkpoint search):src/queries/rum-dashboard/checkpoint-by-url.sql
       (
       true = true
       AND (
              url = concat('https://', REGEXP_REPLACE(@url, 'https://', '')) 
              or
              url = concat('https://www.', REGEXP_REPLACE(@url, 'https://', '')) 
              or
              url = concat('https://www.', REGEXP_REPLACE(@url, 'www.', ''))
              or
              url = concat('https://', REGEXP_REPLACE(@url, 'https://www.', ''))
              )
<<<<<<<< HEAD:src/queries/rum-dashboard/rfqs.sql
       ) OR       @exactmatch = false )
), 
unique_targets as (
  select (case when not @exactmatch then hostname end) as hostname,(case when @exactmatch then url end) as url, lower(target) as target, sum(pageviews) traffic from rfqs group by (case when not @exactmatch then hostname end), lower(target), (case when @exactmatch then url end)
)
select hostname, url, target, sum(traffic) as traffic from unique_targets group by hostname, url, target order by traffic desc
========
       ) OR       true = false )
GROUP BY checkpoint,
         url, target
ORDER BY invocations desc;
>>>>>>>> 957ce4a (checkpoint search):src/queries/rum-dashboard/checkpoint-by-url.sql
