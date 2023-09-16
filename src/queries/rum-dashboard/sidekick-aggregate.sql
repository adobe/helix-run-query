--- description: Get Daily Sidekick Data From RUM for a given domain
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
WITH sidekick_events AS
(
       SELECT Format_date("%Y-%m-%d", Date_trunc(time, day)) AS day,
              id,
              checkpoint,
              hostname,
              source,
              pageviews,
              user_agent LIKE "%Sidekick%" AS extension
       FROM   helix_rum.CHECKPOINTS_V4(@url, @offset, @interval, @startdate, @enddate, @timezone, 'all', @domainkey )
       WHERE  CHECKPOINT LIKE "sidekick:%")
SELECT   day,
         sum(pageviews)                   AS actions,
         count(DISTINCT CHECKPOINT) AS checkpoints,
FROM     sidekick_events
GROUP BY sidekick_events.day,
         hostname
ORDER BY hostname,
         day DESC