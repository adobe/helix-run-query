--- description: Total Daily behavior of Sidekick Across All Projects
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2023-02-01
--- enddate: 2023-05-28
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret
with sidekick_events AS (
  SELECT
    FORMAT_DATE("%Y-%m-%d", DATE_TRUNC(time, DAY)) AS day,
    id,
    hostname,
    checkpoint,
    source,
    pageviews,
    user_agent LIKE "%Sidekick%" AS extension
  FROM   helix_rum.CHECKPOINTS_V4('-', @offset, @interval, @startdate, @enddate, @timezone, 'all', @domainkey ) WHERE  CHECKPOINT LIKE "sidekick:%" and hostname is not null and not hostname = '' and hostname LIKE "%--%--%.hlx.%"
  ), 
chosen_features as (
  select checkpoint, sum(pageviews) as pageviews from sidekick_events group by checkpoint order by pageviews desc limit 10
)
SELECT   day,
         sum(pageviews) AS invocations,
         checkpoint,
FROM     sidekick_events
where checkpoint in (select checkpoint from chosen_features)
GROUP BY sidekick_events.day, checkpoint
ORDER BY
         day asc